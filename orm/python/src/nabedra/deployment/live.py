"""One opt-in growth iteration over Control and provider inventory."""
from __future__ import annotations

from dataclasses import dataclass
from ipaddress import ip_address
import json
from pathlib import Path
import subprocess
import time
from typing import Mapping, Protocol
from urllib.parse import urlsplit
from uuid import UUID, uuid5

from ..errors import NabedraError
from ..providers import ProviderError
from .controller import GrowthController
from .docker import identity

_WORKER_FIELDS = ('worker_id', 'incarnation', 'certificate_sha256', 'enrollment_revision')
_NS = 10**9


@dataclass(frozen=True)
class HostObservation:
    worker: dict
    zone: str
    attachment_slots: int
    expandable_resources: frozenset[str] = frozenset()
    cpu_limit_millicores: int | None = None
    memory_limit_bytes: int | None = None


@dataclass(frozen=True)
class ProviderObservation:
    hosts: Mapping[str, HostObservation]
    retained_workers: int
    monthly_spend_floor: int
    worker_zones: frozenset[str] = frozenset()


@dataclass(frozen=True)
class GrowthTarget:
    worker: dict
    pool_id: str
    pool_generation: int
    resource_id: str | None = None


class StatefulGrowthAdapter(Protocol):
    """A deployment-owned adapter bound to one immutable configuration ID.

    Observe the exact runtime mounts and retained provider resources. Reconcile
    every side effect by claim ID and the same target on replay. Honor the
    supplied absolute deadline in all provider and host operations.
    """

    cluster_id: str
    adapter_id: str

    def observe(self, inventories: Mapping[str, dict], *, policy: dict, deadline: float) -> ProviderObservation: ...
    def target(self, claim_id: str, action: dict, inventories: Mapping[str, dict]) -> GrowthTarget | None: ...
    def provision(self, intent: dict, target: GrowthTarget, *, deadline: float) -> dict: ...
    def register(self, intent: dict, target: GrowthTarget, record: dict, *,
                 controller: GrowthController, deadline: float) -> dict | None: ...


class _InventoryUnavailable(ProviderError):
    pass


@dataclass(frozen=True)
class _SplitDemand:
    owner: str
    record: dict
    anchor: dict
    members: tuple[dict, ...]
    needs: tuple[dict, ...]


def _uint(value):
    if type(value) is not int or not 0 <= value < 2**64:
        raise _InventoryUnavailable('Inventory counter is missing or out of range')
    return value


def _worker(enrollment):
    return {key: enrollment[key] for key in _WORKER_FIELDS}


def _remaining(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise _InventoryUnavailable('Automatic growth iteration deadline expired')
    return remaining


def _trusted_url(endpoint):
    url = urlsplit(endpoint._transport._url)
    if url.scheme == 'https':
        return True
    try:
        return ip_address(url.hostname).is_loopback
    except ValueError:
        return False


class AutomaticGrowth:
    """Evaluate live headroom and reconcile at most one durable growth claim."""

    def __init__(self, *, controller: GrowthController, workers: Mapping[str, GrowthController],
                 adapter: StatefulGrowthAdapter | None, planner=None, timeout=60):
        self.controller = controller
        self.workers = {identity(key): endpoint for key, endpoint in workers.items()}
        self.adapter = adapter
        self.planner = tuple(planner or ())
        self.timeout = timeout
        if adapter is None:
            return
        if (isinstance(planner, (str, bytes)) or not self.planner or not Path(self.planner[0]).is_absolute()
                or type(timeout) not in (int, float) or not 1 <= timeout <= 120
                or len(self.workers) > 4096
                or not all(_trusted_url(endpoint) for endpoint in (controller, *self.workers.values()))):
            raise ProviderError('Automatic growth requires bounded local planner and authenticated worker URLs')
        bind_endpoints = getattr(adapter, 'bind_endpoints', None)
        if bind_endpoints is not None:
            self.adapter = bind_endpoints(self.workers)

    def iterate(self):
        if self.adapter is None:
            return {'state': 'unsupported', 'reason': 'adapter_unavailable'}
        deadline = time.monotonic() + self.timeout
        try:
            snapshot = self._snapshot(deadline)
            policy, pin = snapshot['policy'], snapshot['pin']
            auto = policy.get('automatic')
            if auto is None or auto['adapter_id'] != self.adapter.adapter_id:
                return {'state': 'unsupported', 'reason': 'adapter_policy_unavailable'}
            active = snapshot['active']
            if active is not None and not snapshot['active_automatic']:
                return {'state': 'manual_pending', 'claim': active}
            enrollments = self._roster(pin, auto['max_workers'], deadline)
            first = self._samples(pin, enrollments, snapshot['cluster_id'], deadline, active=active)
            if active is not None:
                return self._reconcile(snapshot, active['intent'], first, deadline)
            moving = [(worker, row['split_replica_move']) for worker, row in first.items()
                      if 'split_replica_move' in row]
            if moving:
                if len(moving) != 1:
                    raise _InventoryUnavailable('Several workers report the placement owner')
                worker, record = moving[0]
                request = record['request']
                authority = _authority(pin['ready'])
                if (record['owner'] != enrollments[worker]
                        or record['authority'] != authority
                        or record['id'] != _split_move_id(policy, authority, request)):
                    return {'state': 'placement_pending', 'reason': 'existing_replica_move'}
                if self._snapshot(deadline)['pin'] != pin:
                    raise _InventoryUnavailable('Control inventory changed before placement resume')
                return self._move(worker, policy, authority, request, deadline)
            sample_seconds = auto['sample_seconds']
            if _remaining(deadline) <= sample_seconds:
                raise _InventoryUnavailable('Deadline is shorter than the configured observation window')
            time.sleep(sample_seconds)
            second = self._samples(pin, enrollments, snapshot['cluster_id'], deadline)
            try:
                provider = self.adapter.observe(second, policy=policy, deadline=deadline)
            except ProviderError:
                raise _InventoryUnavailable('Provider inventory unavailable') from None
            if set(provider.hosts) != set(enrollments):
                raise _InventoryUnavailable('Provider inventory does not cover the pinned worker roster')
            rows = [_planner_worker(first[worker_id], current, provider.hosts[worker_id], auto)
                    for worker_id, current in second.items()]
            if any('split_replica_move' in row for row in second.values()):
                return {'state': 'placement_pending', 'reason': 'existing_replica_move'}
            demand = _split_demand(pin, second, provider, policy)
            if demand is None and any('split_deficit' in row for row in first.values()):
                raise _InventoryUnavailable('Exact split observation expired during sampling')
            if demand is not None:
                if not demand.needs:
                    return {'state': 'idle'}
                move = _spare_move(demand, second, rows, policy)
                if move is not None:
                    fresh, fresh_samples = self._refresh_split(
                        demand, snapshot, enrollments, provider, deadline,
                        extra=(move['worker']['worker_id'],))
                    if _spare_move(fresh, fresh_samples, rows, policy) != move:
                        raise _InventoryUnavailable('Spare pool or split source changed')
                    return self._move(demand.owner, policy, _authority(pin['ready']), move, deadline)
            plan = self._plan(snapshot, provider, rows, deadline, demand=demand)
            if self._snapshot(deadline)['pin'] != pin:
                raise _InventoryUnavailable('Control inventory changed during planning')
            if 'refusal' in plan:
                return {'state': 'refused', 'reason': plan['refusal']}
            if not plan['plan']['actions']:
                return {'state': 'idle'}
            action = plan['plan']['actions'][0]
            claim_identity = [pin, action]
            if demand is not None:
                claim_identity.append(demand.record['generation'])
            claim_id = str(uuid5(UUID(policy['policy_id']),
                                 json.dumps(claim_identity, sort_keys=True, separators=(',', ':'))))
            target = self._target(claim_id, action, second)
            if target is None:
                return {'state': 'unsupported', 'reason': 'target_unavailable', 'action': action}
            if action['kind'] == 'add_worker' and target.worker['worker_id'] not in self.workers:
                return {'state': 'unsupported', 'reason': 'worker_inventory_endpoint_unavailable', 'action': action}
            if demand is not None:
                fresh, _ = self._refresh_split(demand, snapshot, enrollments, provider, deadline)
                previous = {need['pool_id']: need['free_bytes'] for need in demand.needs}
                current = {need['pool_id']: need['free_bytes'] for need in fresh.needs}
                if current.keys() != previous.keys() or any(
                        available < previous[pool] for pool, available in current.items()):
                    raise _InventoryUnavailable('Exact split capacity changed during planning')
            intent = dict(policy_id=policy['policy_id'], claim_id=claim_id, action=action,
                          worker=target.worker, pool_id=target.pool_id, pool_generation=target.pool_generation,
                          quoted_monthly_cost=plan['plan']['action_monthly_costs'][0],
                          monthly_spend_ceiling=policy['monthly_spend_ceiling'],
                          max_attempts=policy['max_attempts'], cooldown_seconds=policy['cooldown_seconds'])
            owner = self._owner(intent)
            owner._operation(claim_id, 'claim-automatic', 'storage.growth.claim_automatic',
                             {'intent': intent, 'pin': pin}, deadline=deadline)
            return self._reconcile(snapshot, intent, second, deadline, target=target)
        except _InventoryUnavailable:
            return {'state': 'incomplete', 'reason': 'live_inventory_unavailable'}
        except NabedraError as exc:
            if exc.code == 'unsupported_operation':
                return {'state': 'unsupported', 'reason': 'automatic_policy_unavailable'}
            return {'state': 'incomplete', 'reason': exc.code}
        except ProviderError:
            return {'state': 'pending', 'reason': 'provider_reconciliation_required'}

    def _snapshot(self, deadline):
        snapshot = self.controller._read('storage.growth.snapshot', {}, deadline=deadline)
        if snapshot['cluster_id'] != self.adapter.cluster_id:
            raise _InventoryUnavailable('Control and provider cluster identities differ')
        return snapshot

    def _roster(self, pin, maximum, deadline):
        expected = _uint(pin['ready']['ready_count'])
        if not 0 < expected <= maximum:
            raise _InventoryUnavailable('Ready roster exceeds the automatic policy ceiling')
        enrollments = {}
        after = None
        for _ in range(maximum + 1):
            page = self.controller._read('storage.growth.workers',
                                         {'pin': pin, 'after': after}, deadline=deadline)
            workers = page['workers']
            if page['pin'] != pin or len(workers) > 32 or (not workers and not page['done']):
                raise _InventoryUnavailable('Worker inventory page is invalid or stale')
            for enrollment in workers:
                worker = _worker(enrollment)
                current = identity(worker['worker_id'])
                if (after is not None and current <= after) or len(enrollments) >= expected:
                    raise _InventoryUnavailable('Worker roster order or count differs')
                enrollments[current] = worker
                after = current
            if page['after'] != after:
                raise _InventoryUnavailable('Worker inventory cursor differs')
            if page['done']:
                if len(enrollments) != expected:
                    raise _InventoryUnavailable('Worker roster size differs')
                return enrollments
        raise _InventoryUnavailable('Worker inventory page limit reached')

    def _samples(self, pin, enrollments, cluster_id, deadline, *, active=None):
        samples = {}
        for worker_id, worker in enrollments.items():
            endpoint = self.workers.get(worker_id)
            if endpoint is None:
                raise _InventoryUnavailable('A ready worker has no configured authenticated endpoint')
            sample = endpoint._read('storage.growth.sample', {'pin': pin, 'worker': worker}, deadline=deadline)
            inventory = sample['inventory']
            if (sample['pin'] != pin or inventory['worker'] != worker or inventory['ready'] is not True
                    or not 0 < len(inventory['pools']) <= inventory['local_open_pool_limit'] <= 8):
                raise _InventoryUnavailable('Live worker inventory differs from the Control enrollment')
            for pool in inventory['pools']:
                mount = pool.get('expected')
                pending_mount = (active is not None and active['phase'] == 'awaiting_mount'
                                 and pool.get('growth_claim') == active['intent']['claim_id']
                                 and pool['pool_id'] == active['intent']['pool_id'])
                if (mount is None or mount['worker'] != worker
                        or (pool['placement_eligible'] is not True and not pending_mount)
                        or mount['identity']['cluster_id'] != cluster_id
                        or mount['identity']['pool_id'] != pool['pool_id'] or not pool.get('mount_id')):
                    raise _InventoryUnavailable('A registered pool is not verified on its owning worker')
            samples[worker_id] = sample
        return samples

    def _plan(self, snapshot, provider, rows, deadline, *, demand=None):
        policy, pin = snapshot['policy'], snapshot['pin']
        # Until the provider ledger can attribute every retained resource to a
        # durable claim, reserve the entire excess over the configured baseline.
        extra = max(0, _uint(provider.monthly_spend_floor) - policy['initial_monthly_spend'])
        spend = _uint(pin['monthly_spend'] + extra)
        revision = pin['growth_revision']
        lines = [dict(policy=policy, now=int(time.time()), revision=revision,
                      last_attempt=snapshot['last_attempt'], monthly_spend=spend,
                      retained_workers=_uint(provider.retained_workers),
                      unavailable_worker_zones=[zone for zone in policy['zones'] if zone not in provider.worker_zones])]
        if demand is not None:
            lines[0]['split_needs'] = [{key: value for key, value in need.items() if key != 'free_bytes'}
                                       for need in demand.needs]
        for i in range(0, len(rows), 32):
            lines.append(dict(revision=revision, workers=rows[i:i + 32], done=i + 32 >= len(rows)))
        body = ''.join(json.dumps(line, separators=(',', ':')) + '\n' for line in lines)
        try:
            result = subprocess.run([*self.planner, 'growth-plan'], input=body, text=True,
                                    capture_output=True, check=True, timeout=_remaining(deadline))
            if len(result.stdout) > 32 << 10:
                raise ValueError
            return json.loads(result.stdout)
        except (OSError, subprocess.SubprocessError, ValueError):
            raise _InventoryUnavailable('Local growth planner failed') from None

    def _refresh_split(self, demand, snapshot, enrollments, provider, deadline, *, extra=()):
        pin = snapshot['pin']
        workers = {demand.owner, *extra, *(member['worker_id'] for member in demand.members)}
        samples = self._samples(pin, {worker: enrollments[worker] for worker in workers},
                                snapshot['cluster_id'], deadline)
        fresh = _split_demand(pin, samples, provider, snapshot['policy'])
        if (fresh is None or fresh.owner != demand.owner or fresh.anchor != demand.anchor
                or any('split_replica_move' in row for row in samples.values())
                or self._snapshot(deadline)['pin'] != pin):
            raise _InventoryUnavailable('Exact split source changed during planning')
        return fresh, samples

    def _move(self, worker, policy, authority, request, deadline):
        move_id = _split_move_id(policy, authority, request)
        record = self.workers[worker]._read('storage.replica.move', request,
                                           request_id=move_id, deadline=deadline)
        if record['id'] != move_id or record['request'] != request or record['authority'] != authority:
            raise _InventoryUnavailable('Replica move result differs from the durable request')
        state = record['phase']['state']
        if state == 'cancelled':
            return {'state': 'refused', 'reason': 'replica_move_cancelled', 'move': record}
        return {'state': 'resample' if state == 'retired' else 'placement_pending', 'move': record}

    def _owner(self, intent):
        if intent['action']['kind'] == 'add_worker':
            return self.controller
        endpoint = self.workers.get(intent['worker']['worker_id'])
        if endpoint is None:
            raise _InventoryUnavailable('Growth owner has no authenticated endpoint')
        return endpoint

    def _target(self, claim_id, action, inventories):
        target = self.adapter.target(claim_id, action, inventories)
        if target is None:
            return None
        identity(target.pool_id)
        if _uint(target.pool_generation) == 0:
            raise _InventoryUnavailable('Growth target generation is missing')
        kind = action['kind']
        if kind == 'add_worker':
            if target.worker['worker_id'] in inventories:
                raise _InventoryUnavailable('The growth worker already belongs to the ready set')
        else:
            inventory = inventories[action['worker']]['inventory']
            if inventory['worker'] != target.worker:
                raise _InventoryUnavailable('Growth target identity differs from the pinned worker')
            pools = {item['pool_id']: item['expected'] for item in inventory['pools']}
            if kind == 'expand':
                pool = pools.get(action['pool'])
                if (pool is None or target.pool_id != action['pool'] or target.pool_generation != pool['generation']
                        or target.resource_id != pool['provider_volume_id']):
                    raise _InventoryUnavailable('Expansion target differs from the observed mounted pool')
            elif target.pool_id in pools:
                row = next(item for item in inventory['pools'] if item['pool_id'] == target.pool_id)
                if (row['growth_claim'] != claim_id
                        or row['expected']['generation'] != target.pool_generation
                        or (target.resource_id is not None
                            and row['expected']['provider_volume_id'] != target.resource_id)):
                    raise _InventoryUnavailable('Pool addition would reuse an active pool')
        return target

    def _reconcile(self, snapshot, intent, inventories, deadline, *, target=None):
        claim_id, action = intent['claim_id'], intent['action']
        target = target or self._target(claim_id, action, inventories)
        if target is None:
            return {'state': 'pending', 'reason': 'target_unavailable', 'claim': snapshot['active']}
        if (target.worker, target.pool_id, target.pool_generation) != (
                intent['worker'], intent['pool_id'], intent['pool_generation']):
            raise _InventoryUnavailable('Provider target differs from the durable claim')
        owner = self._owner(intent)
        original = snapshot['pin']
        register_worker = getattr(self.adapter, 'register_worker', None) if action['kind'] == 'add_worker' else None
        provision_worker = getattr(self.adapter, 'provision_worker', None) if action['kind'] == 'add_worker' else None
        registration_pin = None
        provisioning_attempts = None

        def authorize(phase, attempts):
            nonlocal registration_pin, provisioning_attempts
            current = self._snapshot(deadline)
            pin = current['pin']
            active = current['active']
            if (any(pin[key] != original[key] for key in ('policy_id', 'ready', 'enrollment_revision', 'group_revision'))
                    or not current['active_automatic'] or active is None or active['intent'] != intent
                    or active['phase'] != phase or active['attempts'] != attempts):
                raise _InventoryUnavailable('The current Control claim or worker roster changed')
            if action['kind'] != 'add_worker':
                fresh = self._samples(pin, {intent['worker']['worker_id']: intent['worker']},
                                      current['cluster_id'], deadline, active=active)
                if self._target(claim_id, action, fresh) != target:
                    raise _InventoryUnavailable('Growth target mount or provider identity changed')
            if phase == 'provisioning':
                provider = self.adapter.observe(
                    inventories, policy=current['policy'], deadline=deadline,
                    **({'claim_id': claim_id} if register_worker is not None else {}))
                if set(provider.hosts) != set(inventories):
                    raise _InventoryUnavailable('Provider inventory no longer covers the pinned roster')
                extra = max(0, _uint(provider.monthly_spend_floor) - current['policy']['initial_monthly_spend'])
                if pin['monthly_spend'] + extra > current['policy']['monthly_spend_ceiling']:
                    raise ProviderError('Retained resource spend exceeds the current growth ceiling')
                if action['kind'] == 'add_worker':
                    if (len(inventories) + _uint(provider.retained_workers)
                            >= current['policy']['automatic']['max_workers']
                            or action['zone'] not in provider.worker_zones):
                        raise ProviderError('Provider worker ceiling or zone capability changed')
                else:
                    host = provider.hosts[intent['worker']['worker_id']]
                    if host.worker != intent['worker']:
                        raise _InventoryUnavailable('Provider worker incarnation changed')
                    if ((action['kind'] == 'add_pool' and host.attachment_slots <= 0)
                            or (action['kind'] == 'expand' and target.resource_id not in host.expandable_resources)):
                        raise ProviderError('Provider attachment or expansion headroom changed')
                latest = self._snapshot(deadline)
                if latest['pin'] != pin or latest['active'] != active:
                    raise _InventoryUnavailable('Control inventory changed during provider inspection')
                provisioning_attempts = attempts
            elif register_worker is not None:
                registration_pin = pin

        def provision(_intent):
            if provision_worker is None:
                observed = self.adapter.provision(_intent, target, deadline=deadline)
            else:
                observed = provision_worker(_intent, target, deadline=deadline,
                    authorize=lambda: authorize('provisioning', provisioning_attempts))
            if target.resource_id is not None and observed.get('resource_id') != target.resource_id:
                raise ProviderError('Provider acknowledged another resource')
            return observed

        registration = None

        def register(_intent, record):
            nonlocal registration
            if target.resource_id is not None and record['provider_resource'] != target.resource_id:
                raise ProviderError('Claim names another provider resource')
            if register_worker is not None:
                registration = register_worker(_intent, target, record, controller=owner,
                                               pin=registration_pin, deadline=deadline)
                return None
            return self.adapter.register(_intent, target, record, controller=owner, deadline=deadline)

        record = owner._advance(intent, provision, register=register, authorize=authorize, deadline=deadline)
        result = registration or {'state': 'pending'}
        if record['phase'] == 'complete':
            result['state'] = 'mounted' if register_worker is not None or action['kind'] == 'expand' else 'awaiting_placement'
        return {**result, 'claim': record}


def _split_demand(pin, samples, provider, policy):
    reported = [(worker, sample['split_deficit']) for worker, sample in samples.items()
                if 'split_deficit' in sample]
    if not reported:
        return None
    if len(reported) != 1:
        raise _InventoryUnavailable('Several workers report an exact split deficit')
    owner, deficit = reported[0]
    source = deficit['source']
    record = source['record']
    identity(record['identity']['group_id'])
    identity(record['generation'])
    voters, missing = source['voters'], deficit['pools']
    if (deficit['ready'] != pin['ready'] or record['replica_count'] != 3
            or len(voters) != 3 or not 1 <= len(missing) <= 3):
        raise _InventoryUnavailable('Split deficit differs from the pinned RF3 source')
    required = _uint(deficit['required_bytes'])
    observed = _uint(deficit['observed_committed_bytes'])
    if required <= observed * 3:
        raise _InventoryUnavailable('Split deficit is missing its copy and settlement reserve')
    members, mounts, zones = [], {}, set()
    initial = policy.get('initial_replica_zones', {})
    for voter in voters:
        member, worker = voter['member'], voter['worker']
        worker_id, pool_id = identity(worker['worker_id']), identity(member['pool_id'])
        sample = samples.get(worker_id)
        host = provider.hosts.get(worker_id)
        if (sample is None or host is None or sample['inventory']['worker'] != worker
                or host.worker != worker or host.zone not in policy['zones'] or host.zone in zones
                or initial.get(worker_id, host.zone) != host.zone or member['worker_id'] != worker_id
                or pool_id in mounts or any(old['worker_id'] == worker_id
                                             or old['node_id'] == member['node_id'] for old in members)):
            raise _InventoryUnavailable('Source voter or failure domain changed')
        pool = next((row for row in sample['inventory']['pools'] if row['pool_id'] == pool_id), None)
        registered = voter['registered_pool_generation']
        if pool is None or (registered is not None and registered != pool['expected']['generation']):
            raise _InventoryUnavailable('Source pool generation changed')
        members.append(member)
        mounts[pool_id] = (pool, worker, host.zone)
        zones.add(host.zone)
    needs, seen = [], set()
    for entry in missing:
        pool_id = identity(entry['pool'])
        if pool_id in seen or pool_id not in mounts:
            raise _InventoryUnavailable('Split deficit names another pool')
        seen.add(pool_id)
        pool, worker, zone = mounts[pool_id]
        pool_required = required * (3 if isinstance(pool['expected']['profile'], dict) else 1)
        if (entry['worker'] != worker or entry['pool_generation'] != pool['expected']['generation']
                or _uint(entry['required_bytes']) != pool_required
                or _uint(entry['available_bytes']) >= pool_required):
            raise _InventoryUnavailable('Split deficit differs from the exact native mount')
        free = _uint(pool['observed_available_bytes'])
        if free >= pool_required:
            continue
        local = worker['worker_id'] == owner
        needs.append(dict(worker_id=worker['worker_id'], pool_id=pool_id, zone=zone,
                          required_bytes=pool_required, replacement_bytes=_uint(required + observed),
                          can_move_to_local_pool=local,
                          can_move_to_new_worker=local and source['domains'] is not None,
                          free_bytes=free))
    anchor = {key: deficit[key] for key in ('ready', 'source', 'epoch', 'required_bytes')}
    anchor['mounts'] = {pool_id: pool['expected']['generation'] for pool_id, (pool, _, _) in mounts.items()}
    return _SplitDemand(owner, record, anchor, tuple(members), tuple(needs))


def _authority(ready):
    return {key: ready['view'][key] for key in ('run_id', 'owner_term')}


def _split_move_id(policy, authority, request):
    name = json.dumps([authority, request], sort_keys=True, separators=(',', ':'))
    return str(uuid5(UUID(policy['policy_id']), 'automatic-split-replica-move/' + name))


def _spare_move(demand, samples, rows, policy):
    workers = {worker['worker_id']: worker for worker in rows}
    initial = policy.get('initial_replica_zones', {})
    auto = policy['automatic']
    reserve = auto['settlement_reserve_bytes'] + auto['compaction_reserve_bytes']
    for need in demand.needs:
        worker_id, pool_id, zone = need['worker_id'], need['pool_id'], need['zone']
        source = next(member for member in demand.members if member['worker_id'] == worker_id)
        old = next(pool for pool in samples[worker_id]['inventory']['pools'] if pool['pool_id'] == pool_id)
        retained = [member for member in demand.members if member != source]
        for target_id in sorted(samples):
            sample = samples[target_id]
            target = workers[target_id]
            if (target['zone'] != zone or any(member['worker_id'] == target_id for member in retained)
                    or target['cpu_used'] >= target['cpu_limit'] or target['io_used'] >= target['io_limit']
                    or target['memory_used'] >= target['memory_limit']
                    or target['resident_shards'] + (2 if target_id == worker_id else 3)
                        > target['resident_shard_limit']):
                continue
            for pool in sorted(sample['inventory']['pools'], key=lambda row: row['pool_id']):
                target_pool = pool['pool_id']
                config = pool['expected']
                if target_pool == pool_id or any(member['pool_id'] == target_pool for member in retained):
                    continue
                docker = isinstance(config['profile'], dict)
                if target_id == worker_id and pool['device'] == old['device']:
                    continue
                if docker and target_id != worker_id:
                    continue
                if worker_id != demand.owner:
                    if (target_id != demand.owner or worker_id in initial or initial.get(target_id) != zone
                            or pool.get('growth_claim') is None or demand.anchor['source']['domains'] is None):
                        continue
                elif target_id != demand.owner and (target_id in initial
                        or demand.anchor['source']['domains'] is None):
                    continue
                required = need['replacement_bytes'] + reserve
                planned = next(item for item in target['pools'] if item['pool_id'] == target_pool)
                required += planned['write_bytes_per_second'] * auto['provisioning_seconds']
                if docker:
                    required += demand.anchor['required_bytes'] * 2
                if not planned['healthy'] or _uint(pool['observed_available_bytes']) < required:
                    continue
                generation = demand.record['generation']
                node = uuid5(UUID(generation), f'{target_id}/{target_pool}').int & ((1 << 63) - 1)
                if node == 0 or any(member['node_id'] == node for member in demand.members):
                    continue
                return dict(group=demand.record['identity'], expected_generation=generation,
                            remove_node=source['node_id'], worker=sample['inventory']['worker'],
                            target=dict(node_id=node, worker_id=target_id, pool_id=target_pool),
                            pool_generation=config['generation'])
    return None


def _planner_worker(first, second, host, auto):
    inventory, before, after = second['inventory'], first['usage'], second['usage']
    if host.worker != inventory['worker']:
        raise _InventoryUnavailable('Provider observation names another worker incarnation')
    elapsed = _uint(after['monotonic_ns'] - before['monotonic_ns'])
    if not auto['sample_seconds'] * _NS <= elapsed <= auto['observation_max_age_seconds'] * _NS:
        raise _InventoryUnavailable('Native measurement interval is stale or incomplete')
    cpu = _uint(after['cpu_time_ns'] - before['cpu_time_ns']) * 1000 // elapsed
    write = _uint(after['write_bytes'] - before['write_bytes']) * _NS // elapsed
    read = _uint(after['read_bytes'] - before['read_bytes']) * _NS // elapsed
    cpu_limit = auto['cpu_limit_millicores']
    if host.cpu_limit_millicores is not None:
        cpu_limit = min(cpu_limit, _uint(host.cpu_limit_millicores))
    # The stable queue record and version establish a lower bound on queue
    # age. Only treat this as compute pressure when all query slots were held.
    if (before['oldest_queued'] is not None and before['oldest_queued'] == after['oldest_queued']
            and all(usage['queued_count'] >= auto['queue_pressure_count']
                    and usage['allocated_query_threads'] >= usage['query_thread_limit']
                    for usage in (before, after))):
        cpu = max(cpu, cpu_limit)

    backing = {}
    pools = []
    for pool in inventory['pools']:
        device = tuple(pool['device'])
        capacity = dict(pool_id=pool['pool_id'], total_bytes=_uint(pool['observed_total_bytes']),
                        free_bytes=_uint(pool['observed_available_bytes']), reserved_bytes=0,
                        write_bytes_per_second=write, expandable=False, healthy=True)
        prior = backing.get(device)
        if prior is None:
            capacity['expandable'] = pool['expected']['provider_volume_id'] in host.expandable_resources
            backing[device] = capacity
        else:
            prior['total_bytes'] = min(prior['total_bytes'], capacity['total_bytes'])
            prior['free_bytes'] = min(prior['free_bytes'], capacity['free_bytes'])
            prior['expandable'] = False
            capacity['healthy'] = False
        pools.append(capacity)
    slots = min(_uint(host.attachment_slots), inventory['local_open_pool_limit'] - len(pools))
    memory_limit = min(_uint(before['memory_limit_bytes']), _uint(after['memory_limit_bytes']))
    if host.memory_limit_bytes is not None:
        memory_limit = min(memory_limit, _uint(host.memory_limit_bytes))
    return dict(worker_id=inventory['worker']['worker_id'], zone=host.zone, ready=True,
                observed_at=_uint(inventory['observed_at']), attachment_slots_free=slots,
                cpu_used=cpu, cpu_limit=cpu_limit, io_used=read + write,
                io_limit=auto['io_limit_bytes_per_second'],
                memory_used=max(_uint(before['charged_memory_bytes']), _uint(after['charged_memory_bytes'])),
                memory_limit=memory_limit,
                resident_shards=max(_uint(before['resident_group_count']), _uint(after['resident_group_count'])),
                resident_shard_limit=min(_uint(before['resident_group_limit']), _uint(after['resident_group_limit'])),
                pools=pools)
