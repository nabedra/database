"""EC2/EBS lifecycle operations. The supplied SDK client owns credentials.

Creation and modification report provider state only. Filesystem growth,
registration and queue/shard placement belong to the database controller.
No SDK dependency or cloud operation occurs on module import.
"""
from __future__ import annotations

import re
import time
from types import MappingProxyType

from ..providers import ProviderError
from .cloud_control import CloudLifecycleControl, ec2_worker_tags
from .docker import digest, identity
from .ebs_host import EbsLinuxHost


def _volume_tags(claim_id, pool_id, worker_id):
    return {'nabedra:claim': identity(claim_id), 'nabedra:pool': identity(pool_id),
            'nabedra:worker': identity(worker_id)}


class Ec2Provider:
    def __init__(self, *, client, cluster_id, zones, launch_template, max_volume_gib,
                 lifecycle_authority=None, local_host=None):
        self.client, self.cluster_id = client, identity(cluster_id)
        config = getattr(getattr(client, 'meta', None), 'config', None)
        retries = getattr(config, 'retries', None) or {}
        connect = getattr(config, 'connect_timeout', None)
        read = getattr(config, 'read_timeout', None)
        if (type(connect) not in (int, float) or not 0 < connect <= 10
                or type(read) not in (int, float) or not 0 < read <= 20
                or retries.get('mode') != 'standard'
                or retries.get('total_max_attempts') != 1):
            raise ProviderError('EC2 client requires finite connect/read timeouts and standard total_max_attempts=1')
        self._call_window = connect + read
        if (not zones or len(set(zones)) != len(zones) or len(zones) > 16
                or any(not isinstance(z, str) or not z or len(z) > 128 for z in zones)
                or not isinstance(launch_template, dict)
                or set(launch_template) != {'LaunchTemplateId', 'Version'}
                or type(max_volume_gib) is not int or not 1 <= max_volume_gib <= 65536):
            raise ProviderError('Explicit EC2 zones, pinned launch template and volume ceiling are required')
        self.zones, self.launch_template = tuple(zones), MappingProxyType(dict(launch_template))
        if ((lifecycle_authority is not None and type(lifecycle_authority) is not CloudLifecycleControl)
                or (local_host is not None and type(local_host) is not EbsLinuxHost)):
            raise ProviderError('EC2 maintenance requires an authenticated Control endpoint and configured Linux host')
        self.max_volume_gib, self.lifecycle_authority = max_volume_gib, lifecycle_authority
        self.local_host = local_host

    def _call(self, method, *, deadline=None, **arguments):
        if deadline is not None and deadline - time.monotonic() < self._call_window:
            raise ProviderError('EC2 inventory deadline is shorter than the configured SDK timeout')
        try:
            return getattr(self.client, method)(**arguments)
        except Exception:
            raise ProviderError('EC2 operation failed; reconcile the same durable claim') from None

    def _tags(self, claim_id, content):
        return {'nabedra:cluster': self.cluster_id, 'nabedra:claim': identity(claim_id),
                'nabedra:content': digest(content)}

    def _owned(self, record, expected=None):
        tags = {tag['Key']: tag['Value'] for tag in record.get('Tags', [])}
        if tags.get('nabedra:cluster') != self.cluster_id or (expected and any(tags.get(k) != v for k, v in expected.items())):
            raise ProviderError('EC2 resource ownership or immutable claim differs')
        return record

    def _size(self, bytes):
        if type(bytes) is not int or bytes <= 0:
            raise ProviderError('Positive storage bytes are required')
        gib = (bytes + (1 << 30) - 1) >> 30
        if gib > self.max_volume_gib:
            raise ProviderError('EC2 volume exceeds configured ceiling')
        return gib

    def inspect_volume(self, volume_id, *, deadline=None):
        if not isinstance(volume_id, str) or not re.fullmatch(r'vol-(?:[a-f0-9]{8}|[a-f0-9]{17})', volume_id):
            raise ProviderError('Exact EBS volume identity is required')
        records = self._call('describe_volumes', VolumeIds=[volume_id], deadline=deadline).get('Volumes', [])
        if len(records) != 1 or records[0].get('VolumeId') != volume_id:
            raise ProviderError('EBS volume identity changed')
        return self._owned(records[0])

    def _pool_claim(self, claim_id, pool_id, worker_id, zone, size, kms_key_id, staging_digest):
        content = dict(pool_id=identity(pool_id), worker_id=identity(worker_id), zone=zone,
                       gib=size, kms_key_id=kms_key_id)
        if staging_digest is not None:
            content['staging_digest'] = staging_digest
        return self._tags(claim_id, content) | _volume_tags(claim_id, pool_id, worker_id)

    def _staged_volume(self, record, *, claim_id, pool_id, worker_id, zone, size, kms_key_id, staging_digest):
        self._owned(record, self._pool_claim(claim_id, pool_id, worker_id, zone, size, kms_key_id, staging_digest))
        if (not re.fullmatch(r'vol-(?:[a-f0-9]{8}|[a-f0-9]{17})', record.get('VolumeId', ''))
                or record.get('AvailabilityZone') != zone or record.get('KmsKeyId') != kms_key_id
                or record.get('VolumeType') != 'gp3' or type(record.get('Size')) is not int
                or record['Size'] < size or record.get('Encrypted') is not True
                or record.get('MultiAttachEnabled') is not False or record.get('SnapshotId') not in ('', None)):
            raise ProviderError('EBS staging volume differs from the exact encrypted disk claim')
        return record

    def ensure_pool(self, *, claim_id, pool_id, worker_id, zone, bytes, kms_key_id, deadline=None,
                    staging_digest=None, authorize=None):
        if zone not in self.zones or not isinstance(kms_key_id, str) or not kms_key_id:
            raise ProviderError('Authorized EBS zone and explicit encryption key are required')
        size = self._size(bytes)
        tags = self._pool_claim(claim_id, pool_id, worker_id, zone, size, kms_key_id, staging_digest)
        # ClientToken makes a lost successful reply safe to retry with the SAME
        # parameters. Conflicting token reuse is rejected by EC2 and ownership.
        if authorize is not None:
            authorize()
        created = self._call('create_volume', ClientToken=claim_id, AvailabilityZone=zone,
            Size=size, VolumeType='gp3', Encrypted=True, KmsKeyId=kms_key_id,
            TagSpecifications=[{'ResourceType': 'volume',
            'Tags': [{'Key': k, 'Value': v} for k, v in tags.items()]}], deadline=deadline)
        record = self.inspect_volume(created['VolumeId'], deadline=deadline)
        if staging_digest is not None:
            self._staged_volume(record, claim_id=claim_id, pool_id=pool_id, worker_id=worker_id, zone=zone,
                                size=size, kms_key_id=kms_key_id, staging_digest=staging_digest)
        self._owned(record, tags)
        if (record.get('AvailabilityZone') != zone or type(record.get('Size')) is not int or record['Size'] < size
                or not record.get('Encrypted') or record.get('MultiAttachEnabled')):
            raise ProviderError('EBS volume parameters differ from its claim')
        return dict(resource_id=record['VolumeId'], state=record['State'])

    def expand_pool(self, *, volume_id, target_bytes, deadline=None):
        record = self.inspect_volume(volume_id, deadline=deadline)
        size = self._size(target_bytes)
        if not record.get('Encrypted') or record.get('MultiAttachEnabled'):
            raise ProviderError('EBS growth requires an encrypted single-writer volume')
        if size > record['Size']:
            change = self._latest_modification(volume_id, deadline)
            if change and change.get('ModificationState') in ('modifying', 'optimizing'):
                if change.get('TargetSize') != size:
                    raise ProviderError('EBS volume has another unfinished growth request')
            else:
                self._call('modify_volume', VolumeId=volume_id, Size=size, deadline=deadline)
        # The provider API does not claim that growpart/xfs_growfs completed.
        return dict(resource_id=volume_id, state='awaiting_filesystem', requested_gib=size)

    def _latest_modification(self, volume_id, deadline):
        response = self._call('describe_volumes_modifications', VolumeIds=[volume_id], deadline=deadline)
        changes = response.get('VolumesModifications', [])
        if (response.get('NextToken') or len(changes) > 1
                or (changes and changes[0].get('VolumeId') != volume_id)):
            raise ProviderError('EBS volume modification inventory is ambiguous')
        return changes[0] if changes else None

    def grow_filesystem(self, *, instance_id, expected, target_bytes, deadline):
        if self.local_host is None or self.local_host.instance_id != instance_id:
            return
        volume_id = expected['provider_volume_id']
        worker = self.inspect_worker(instance_id, deadline=deadline)
        self._owned(worker, ec2_worker_tags(expected['worker']))
        volume = self.inspect_volume(volume_id, deadline=deadline)
        attachment = self._attachment(volume, worker)
        if (worker.get('State', {}).get('Name') != 'running' or not attachment
                or attachment.get('State') != 'attached'):
            raise ProviderError('EBS filesystem is outside the exact running worker')
        size = self._size(target_bytes)
        change = self._latest_modification(volume_id, deadline)
        if volume['Size'] >= size and (change is None or change.get('ModificationState') in ('optimizing', 'completed')):
            self.local_host.grow(instance_id, expected, deadline=deadline)

    def inspect_worker(self, instance_id, *, deadline=None):
        if not isinstance(instance_id, str) or not re.fullmatch(r'i-(?:[a-f0-9]{8}|[a-f0-9]{17})', instance_id):
            raise ProviderError('Exact EC2 instance identity is required')
        response = self._call('describe_instances', InstanceIds=[instance_id], deadline=deadline)
        records = [i for r in response.get('Reservations', []) for i in r.get('Instances', [])]
        if len(records) != 1 or records[0].get('InstanceId') != instance_id:
            raise ProviderError('EC2 instance identity changed')
        return self._owned(records[0])

    def retained_inventory(self, *, deadline):
        """Bounded observations of cluster-tagged workers and retained volumes."""
        def collect(method, rows, id_field):
            records = {}
            parameters = {'Filters': [{'Name': 'tag:nabedra:cluster', 'Values': [self.cluster_id]}],
                          'MaxResults': 128}
            for _ in range(32):
                result = self._call(method, deadline=deadline, **parameters)
                for record in rows(result):
                    resource = record.get(id_field)
                    if not resource or resource in records or len(records) >= 4096:
                        raise ProviderError('EC2 retained inventory changed during paging')
                    records[resource] = self._owned(record)
                token = result.get('NextToken')
                if not token:
                    return records
                if token == parameters.get('NextToken') or len(token) > 4096:
                    raise ProviderError('EC2 retained inventory cursor is invalid')
                parameters['NextToken'] = token
            raise ProviderError('EC2 retained inventory page limit reached')

        workers = collect('describe_instances',
            lambda result: (instance for reservation in result.get('Reservations', [])
                            for instance in reservation.get('Instances', [])), 'InstanceId')
        volumes = collect('describe_volumes', lambda result: result.get('Volumes', ()), 'VolumeId')
        return workers, volumes

    def ensure_worker(self, *, claim_id, worker_id, incarnation, zone, subnet_id, user_data,
                      enrollment=None, deadline=None):
        if zone not in self.zones or not isinstance(user_data, str) or not user_data or len(user_data.encode()) > 16384:
            raise ProviderError('Bounded protected EC2 bootstrap and authorized zone are required')
        tags = self._tags(claim_id, dict(worker_id=identity(worker_id), incarnation=identity(incarnation),
                zone=zone, subnet_id=subnet_id, bootstrap_digest=digest(user_data), template=dict(self.launch_template)))
        tags.update({'nabedra:worker': worker_id, 'nabedra:incarnation': incarnation})
        if enrollment is not None:
            exact = ec2_worker_tags(enrollment)
            if exact['nabedra:worker'] != worker_id or exact['nabedra:incarnation'] != incarnation:
                raise ProviderError('EC2 bootstrap and enrollment worker identities differ')
            tags.update(exact)
        result = self._call('run_instances', ClientToken=claim_id, MinCount=1, MaxCount=1,
            LaunchTemplate=dict(self.launch_template), SubnetId=subnet_id, Placement={'AvailabilityZone': zone},
            UserData=user_data, TagSpecifications=[{'ResourceType': 'instance',
            'Tags': [{'Key': k, 'Value': v} for k, v in tags.items()]}], deadline=deadline)
        instances = result.get('Instances', [])
        if len(instances) != 1:
            raise ProviderError('EC2 launch did not return one instance')
        record = self.inspect_worker(instances[0]['InstanceId'], deadline=deadline)
        self._owned(record, tags)
        if record.get('Placement', {}).get('AvailabilityZone') != zone:
            raise ProviderError('EC2 launched in another failure domain')
        return dict(resource_id=record['InstanceId'], state=record['State']['Name'])

    def staging_template(self, *, zone, subnet_id, device, deadline):
        """Inspect one pinned, private launch template with no guest bootstrap."""
        template_id, version = (self.launch_template[key] for key in ('LaunchTemplateId', 'Version'))
        if (not isinstance(template_id, str) or not re.fullmatch(r'lt-(?:[a-f0-9]{8}|[a-f0-9]{17})', template_id)
                or not isinstance(version, str) or not re.fullmatch(r'[1-9][0-9]{0,18}', version)
                or zone not in self.zones or not isinstance(subnet_id, str)
                or not re.fullmatch(r'subnet-(?:[a-f0-9]{8}|[a-f0-9]{17})', subnet_id)):
            raise ProviderError('EC2 staging requires a numeric template version and exact subnet')
        reply = self._call('describe_launch_template_versions', LaunchTemplateId=template_id,
                           Versions=[version], deadline=deadline)
        rows = reply.get('LaunchTemplateVersions', [])
        if (reply.get('NextToken') or len(rows) != 1 or rows[0].get('LaunchTemplateId') != template_id
                or rows[0].get('VersionNumber') != int(version)):
            raise ProviderError('EC2 staging launch template identity differs')
        data = rows[0].get('LaunchTemplateData', {})
        root = data.get('BlockDeviceMappings', ())
        metadata = data.get('MetadataOptions', {})
        if (not re.fullmatch(r'ami-(?:[a-f0-9]{8}|[a-f0-9]{17})', data.get('ImageId', ''))
                or not data.get('InstanceType') or not data.get('SecurityGroupIds')
                or any(data.get(key) for key in ('UserData', 'NetworkInterfaces', 'Placement', 'TagSpecifications',
                                                  'InstanceMarketOptions', 'KeyName'))
                or metadata.get('HttpTokens') != 'required' or metadata.get('HttpEndpoint') != 'enabled'
                or len(root) != 1 or not root[0].get('DeviceName') or root[0]['DeviceName'] == device
                or root[0].get('Ebs', {}).get('Encrypted') is not True
                or root[0].get('Ebs', {}).get('DeleteOnTermination') is not True):
            raise ProviderError('EC2 staging template requires private networking, IMDSv2 and one encrypted root disk')
        reply = self._call('describe_subnets', SubnetIds=[subnet_id], deadline=deadline)
        subnets = reply.get('Subnets', [])
        if (reply.get('NextToken') or len(subnets) != 1 or subnets[0].get('SubnetId') != subnet_id
                or subnets[0].get('AvailabilityZone') != zone or subnets[0].get('State') != 'available'
                or subnets[0].get('MapPublicIpOnLaunch') is not False
                or subnets[0].get('AssignIpv6AddressOnCreation') is not False):
            raise ProviderError('EC2 staging subnet is not the exact private subnet in the requested zone')
        reply = self._call('describe_instance_types', InstanceTypes=[data['InstanceType']], deadline=deadline)
        types = reply.get('InstanceTypes', [])
        if (reply.get('NextToken') or len(types) != 1 or types[0].get('InstanceType') != data['InstanceType']
                or types[0].get('Hypervisor') != 'nitro' or 'ebs' not in types[0].get('SupportedRootDeviceTypes', ())):
            raise ProviderError('EC2 staging requires the exact Nitro instance type with an EBS root')
        return data

    def _staging_tags(self, claim_id, enrollment, zone, subnet_id, volume_id, size, staging_digest):
        return self._tags(claim_id, dict(enrollment=enrollment, zone=zone, subnet_id=subnet_id,
                          volume_id=volume_id, size=size, template=dict(self.launch_template),
                          staging_digest=staging_digest)) | ec2_worker_tags(enrollment) | {
                              'nabedra:volume': volume_id, 'nabedra:volume-gib': str(size)}

    def _staged_worker(self, record, *, claim_id, enrollment, zone, subnet_id, volume_id, size,
                       staging_digest, template):
        self._owned(record, self._staging_tags(claim_id, enrollment, zone, subnet_id, volume_id, size, staging_digest))
        if (not re.fullmatch(r'i-(?:[a-f0-9]{8}|[a-f0-9]{17})', record.get('InstanceId', ''))
                or record.get('ClientToken') != claim_id or record.get('Placement', {}).get('AvailabilityZone') != zone
                or record.get('SubnetId') != subnet_id or record.get('ImageId') != template['ImageId']
                or record.get('InstanceType') != template['InstanceType'] or record.get('PublicIpAddress')
                or {group['GroupId'] for group in record.get('SecurityGroups', ())} != set(template['SecurityGroupIds'])
                or record.get('RootDeviceType') != 'ebs'
                or record.get('RootDeviceName') != template['BlockDeviceMappings'][0]['DeviceName']):
            raise ProviderError('EC2 staging instance differs from the exact launch claim')
        return record

    def stage_worker(self, *, claim_id, enrollment, zone, subnet_id, volume_id, size, staging_digest,
                     template, authorize, deadline):
        tags = self._staging_tags(claim_id, enrollment, zone, subnet_id, volume_id, size, staging_digest)
        authorize()
        result = self._call('run_instances', ClientToken=claim_id, MinCount=1, MaxCount=1,
            LaunchTemplate=dict(self.launch_template), SubnetId=subnet_id, Placement={'AvailabilityZone': zone},
            TagSpecifications=[{'ResourceType': 'instance',
                               'Tags': [{'Key': k, 'Value': v} for k, v in tags.items()]}], deadline=deadline)
        instances = result.get('Instances', [])
        if len(instances) != 1:
            raise ProviderError('EC2 staging launch did not return one instance')
        record = self.inspect_worker(instances[0]['InstanceId'], deadline=deadline)
        return self._staged_worker(record, claim_id=claim_id, enrollment=enrollment, zone=zone, subnet_id=subnet_id,
                                   volume_id=volume_id, size=size, staging_digest=staging_digest, template=template)

    def _attachment(self, volume, worker, *, device=None):
        zone = worker.get('Placement', {}).get('AvailabilityZone')
        attachments = volume.get('Attachments', [])
        if zone not in self.zones or volume.get('AvailabilityZone') != zone:
            raise ProviderError('EBS volume and worker are outside the same configured zone')
        if not volume.get('Encrypted') or volume.get('MultiAttachEnabled') or len(attachments) > 1:
            raise ProviderError('EBS attachments require an encrypted single-writer volume')
        if not attachments:
            return None
        attachment = attachments[0]
        if (attachment.get('InstanceId') != worker['InstanceId']
                or attachment.get('VolumeId') != volume['VolumeId']
                or attachment.get('DeleteOnTermination') is not False
                or (device is not None and attachment.get('Device') != device)):
            raise ProviderError('EBS attachment owner, retained state or device differs')
        return attachment

    def attach_pool(self, *, claim_id, pool_id, worker, volume_id, instance_id, device, deadline=None,
                    authorize=None):
        if not isinstance(device, str) or not re.fullmatch(r'/dev/sd[f-p]', device):
            raise ProviderError('EBS attachment requires an explicit secondary slot')
        record = self._owned(self.inspect_worker(instance_id, deadline=deadline), ec2_worker_tags(worker))
        volume = self._owned(self.inspect_volume(volume_id, deadline=deadline),
                             _volume_tags(claim_id, pool_id, worker['worker_id']))
        if record.get('State', {}).get('Name') != 'running':
            raise ProviderError('EBS attachment requires the exact running worker')
        attachment = self._attachment(volume, record, device=device)
        if attachment is None and volume.get('State') == 'available':
            if any(entry.get('DeviceName') == device for entry in record.get('BlockDeviceMappings', [])):
                raise ProviderError('EBS attachment slot is already in use')
            if authorize is not None:
                authorize()
            self._call('attach_volume', VolumeId=volume_id, InstanceId=instance_id,
                       Device=device, deadline=deadline)
            volume = self.inspect_volume(volume_id, deadline=deadline)
            attachment = self._attachment(volume, record, device=device)
        state = 'attached' if attachment and attachment.get('State') == 'attached' else 'attaching'
        return dict(resource_id=volume_id, state=state)

    def stop_worker(self, *, claim_id, instance_id, deadline=None):
        """Gracefully stop one drained instance. claim_id is the worker drain UUID."""
        if self.lifecycle_authority is None:
            raise ProviderError('Authenticated Control is required before stopping an EC2 worker')
        cutoff = deadline if deadline is not None else time.monotonic() + 120
        record, zone = self.lifecycle_authority._closed_worker(claim_id, self.cluster_id, 'ec2', deadline=cutoff)
        intent = record['intent']
        if intent['native_target']['instance_id'] != instance_id:
            raise ProviderError('EC2 stop permit names a different instance')
        expected = record['phase']['closed']['mount']['expected']
        worker_tags = ec2_worker_tags(intent['worker']) | {'nabedra:claim': record['growth_claim']}
        volume_id = expected['provider_volume_id']
        volume_tags = _volume_tags(record['growth_claim'], expected['identity']['pool_id'], intent['worker']['worker_id'])

        def state():
            worker = self._owned(self.inspect_worker(instance_id, deadline=cutoff), worker_tags)
            volume = self._owned(self.inspect_volume(volume_id, deadline=cutoff), volume_tags)
            attachment = self._attachment(volume, worker)
            if (worker.get('Placement', {}).get('AvailabilityZone') != zone or not attachment
                    or attachment.get('State') != 'attached' or volume.get('State') != 'in-use'):
                raise ProviderError('EC2 worker lacks the exact retained same-zone database attachment')
            root = worker.get('RootDeviceName')
            mappings = worker.get('BlockDeviceMappings', [])
            data = [entry for entry in mappings if entry.get('DeviceName') != root]
            if (worker.get('RootDeviceType') != 'ebs' or not root or attachment.get('Device') == root
                    or len(data) != 1 or data[0].get('DeviceName') != attachment.get('Device')
                    or data[0].get('Ebs', {}).get('VolumeId') != volume_id
                    or data[0]['Ebs'].get('DeleteOnTermination') is not False):
                raise ProviderError('EC2 worker block device mappings differ from the stopped enrollment')
            return worker.get('State', {}).get('Name')

        observed = state()
        if observed == 'running':
            self._call('stop_instances', InstanceIds=[instance_id], Force=False,
                       SkipOsShutdown=False, Hibernate=False, deadline=cutoff)
            observed = state()
        if observed not in ('stopping', 'stopped'):
            raise ProviderError('EC2 worker stop is unresolved; reconcile the same drain')
        return {'resource_id': instance_id, 'state': observed, 'retained_volume_id': volume_id}

    def detach_pool(self, *, claim_id, volume_id, instance_id, deadline=None):
        if self.lifecycle_authority is None:
            raise ProviderError('Authenticated exact-owner Control endpoint is required before EBS detach')
        cutoff = deadline if deadline is not None else time.monotonic() + 120
        permit = self.lifecycle_authority._closed_pool(claim_id, self.cluster_id, volume_id, deadline=cutoff)
        mount = permit['mount']
        expected = mount['expected']
        worker = self._owned(self.inspect_worker(instance_id, deadline=cutoff), ec2_worker_tags(expected['worker']))
        tags = _volume_tags(permit['growth_claim'], expected['identity']['pool_id'], expected['worker']['worker_id'])
        volume = self._owned(self.inspect_volume(volume_id, deadline=cutoff), tags)
        attachment = self._attachment(volume, worker)
        if attachment and attachment.get('State') == 'attached':
            if (self.local_host is None or self.local_host.instance_id != instance_id
                    or not self.local_host.unmounted(instance_id, mount, deadline=cutoff)):
                return dict(resource_id=volume_id, state='awaiting_unmount')
            device = attachment.get('Device')
            if not isinstance(device, str) or not re.fullmatch(r'/dev/sd[f-p]', device):
                raise ProviderError('Closed EBS volume is outside a configured secondary device slot')
            self._call('detach_volume', VolumeId=volume_id, InstanceId=instance_id,
                       Device=device, Force=False, deadline=cutoff)
            volume = self._owned(self.inspect_volume(volume_id, deadline=cutoff), tags)
            attachment = self._attachment(volume, worker)
        state = 'detached' if attachment is None and volume.get('State') == 'available' else 'detaching'
        return dict(resource_id=volume_id, state=state)
