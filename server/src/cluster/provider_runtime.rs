//! Live configured callback controller. Provider effects require a retained CAS
//! claim and a fresh authorization observation by the loopback bridge.
use super::*;
use crate::cluster::{
    callback, drain,
    member::{Action, IntentState, Ownership},
    provision,
    store::{PreparedDrain, PreparedProvision},
};
use crate::node::queue_runtime::{Command, Reply};
use axum::http::Request as HttpRequest;
use nabedra_rt::{ids, time};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Bridge {
    url: String,
    callback_token: String,
    authorize_token: String,
    #[serde(skip)]
    stops: Mutex<std::collections::BTreeMap<Uuid, drain::proof::ConfirmedDrain>>,
}
impl Bridge {
    pub(super) fn validate(&self) -> Result<()> {
        let uri: axum::http::Uri = self.url.parse().map_err(|_| invalid())?;
        let authority = uri.authority().ok_or_else(invalid)?;
        if uri.scheme_str() != Some("http")
            || authority.host() != "127.0.0.1"
            || authority.port_u16().is_none_or(|p| p == 0)
            || uri.path_and_query().map(|p| p.as_str()) != Some("/private/v1/callback")
            || self.callback_token == self.authorize_token
            || [&self.callback_token, &self.authorize_token]
                .iter()
                .any(|t| {
                    t.len() != 43
                        || !t
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                })
        {
            return Err(invalid());
        }
        Ok(())
    }
}
fn now_ms() -> u64 {
    crate::node::queue_runtime::now_ms()
}
fn exact(outcome: OwnershipOutcome) -> Result<LoadedControl> {
    match outcome {
        OwnershipOutcome::Acknowledged(v) | OwnershipOutcome::ObservedExact(v) => Ok(v),
        _ => Err(unavailable()),
    }
}

impl Api {
    fn provider_authority(&self, root: &Control) -> Result<Owner> {
        let observation = OwnershipObservation::new(root).map_err(|_| unavailable())?;
        let mut guard = self.guard.lock().unwrap();
        guard.observe(&observation).map_err(|_| unavailable())?;
        let generation = guard.authorization_generation();
        guard
            .authorize(generation, &observation, sample())
            .map_err(|_| unavailable())
    }
    async fn provision_publish(
        &self,
        loaded: LoadedControl,
        candidate: provision::ProvisionCandidate,
    ) -> Result<OwnershipOutcome> {
        self.provider_authority(loaded.root())?;
        let outcome = PreparedProvision::prepare(loaded, candidate)
            .map_err(|_| unavailable())?
            .submit(&self.storage, deadline())
            .await
            .map_err(|_| unavailable())?;
        if let OwnershipOutcome::Acknowledged(root) | OwnershipOutcome::ObservedExact(root) =
            &outcome
        {
            self.provider_authority(root.root())?;
        }
        Ok(outcome)
    }
    async fn drain_publish(
        &self,
        loaded: LoadedControl,
        candidate: drain::DrainCandidate,
    ) -> Result<OwnershipOutcome> {
        self.provider_authority(loaded.root())?;
        let outcome = PreparedDrain::prepare(loaded, candidate)
            .map_err(|_| unavailable())?
            .submit(&self.storage, deadline())
            .await
            .map_err(|_| unavailable())?;
        if let OwnershipOutcome::Acknowledged(root) | OwnershipOutcome::ObservedExact(root) =
            &outcome
        {
            self.provider_authority(root.root())?;
        }
        Ok(outcome)
    }
    async fn callback_exchange(
        &self,
        body: Vec<u8>,
        context: &callback::ReplyContext,
    ) -> Result<callback::CallbackReply> {
        let bridge = self.bridge.as_ref().ok_or_else(unavailable)?;
        // The bridge independently re-reads and verifies the current authority.
        let current = self.load().await?;
        self.provider_authority(current.root())?;
        callback::authorize_command(&body, current.root(), &self.attached)
            .map_err(|_| unavailable())?;
        let request = HttpRequest::builder()
            .method("POST")
            .uri(&bridge.url)
            .header("connection", "close")
            .header("authorization", format!("Bearer {}", bridge.callback_token))
            .header("content-type", "application/json")
            .header("content-length", body.len())
            .body(Body::from(body))
            .map_err(|_| invalid())?;
        let end = time::now() + Duration::from_secs(30);
        let response = self.client.request(request, end).await?;
        let status = response.status().as_u16();
        let length = http::framing(response.headers(), 1024 * 1024, false)?.ok_or_else(invalid)?;
        let bytes = http::read_small(response.into_body(), length, end).await?;
        callback::decode_reply(status, &bytes, context).map_err(|_| unavailable())
    }
    async fn provider_inventory(
        &self,
        root: &Control,
        owner: Owner,
    ) -> Result<callback::InventoryEvidence> {
        let command = callback::InventoryCommand::build(root, owner, ids::uuid_v4())
            .map_err(|_| unavailable())?;
        let (bytes, context) = command.into_parts();
        match self.callback_exchange(bytes, &context).await? {
            callback::CallbackReply::Inventory(e) => Ok(e),
            _ => Err(unavailable()),
        }
    }
    async fn deliver_start(
        &self,
        loaded: LoadedControl,
        claim: provision::StartClaim,
        owner: Owner,
    ) -> Result<()> {
        let (candidate, pending) = claim.into_parts();
        let outcome = self.provision_publish(loaded, candidate).await?;
        let confirmed = pending.confirm(&outcome).ok_or_else(unavailable)?;
        let (bytes, context) = confirmed.into_parts();
        let reply = self.callback_exchange(bytes, &context).await;
        let loaded = self.load().await?;
        let candidate = match reply {
            Ok(reply) => provision::complete_start(
                loaded.root(),
                owner,
                &context,
                reply,
                now_ms(),
                ids::uuid_v4(),
            ),
            Err(_) => {
                provision::uncertain_start(loaded.root(), owner, &context, now_ms(), ids::uuid_v4())
            }
        }
        .map_err(|_| unavailable())?;
        exact(self.provision_publish(loaded, candidate).await?)?;
        Ok(())
    }
    async fn termination_publish(
        &self,
        loaded: LoadedControl,
        candidate: provision::ProvisionCandidate,
        owner: Owner,
    ) -> Result<()> {
        let published = exact(self.provision_publish(loaded, candidate).await?)?;
        // Only complete provider termination evidence reaches this method.
        // A heartbeat timeout cannot authorize a cold request run.
        if !published.root().nodes.is_empty()
            && published
                .root()
                .nodes
                .iter()
                .all(|n| n.state == NodeState::Stopped)
            && !self.new_run.load(Ordering::Acquire)
        {
            self.cold_start.store(true, Ordering::Release);
            if let Err(error) = self.ensure_catalog_fence(owner).await {
                // A failed proof-bound transition cannot authorize a later run
                // after another controller has acquired ownership.
                self.cold_start.store(false, Ordering::Release);
                return Err(error);
            }
        }
        Ok(())
    }
    /// One provider decision per turn. A failed/partial inventory blocks every
    /// launch/removal and changes only retry metadata, never queue contents.
    pub(super) async fn provider_tick(&self) -> Result<()> {
        if self.bridge.is_none() {
            return Ok(());
        }
        let loaded = self.load().await?;
        let root = loaded.root();
        let owner = self.provider_authority(root)?;
        if now_ms() < root.scheduler.backoff_until_ms {
            return Ok(());
        }
        let inventory = match self.provider_inventory(root, owner).await {
            Ok(v) => v,
            Err(_) => {
                let candidate =
                    provision::inventory_failed_candidate(root, owner, now_ms(), ids::uuid_v4())
                        .map_err(|_| unavailable())?;
                exact(self.provision_publish(loaded, candidate).await?)?;
                return Ok(());
            }
        };
        // Reconcile dispatched launches before introducing any new identity.
        if let Some(intent) = root.intents.iter().find(|i| {
            i.action == Action::Start
                && matches!(i.state, IntentState::InProgress | IntentState::Reconciling)
                && i.outcome.is_none()
        }) {
            let id = intent.request_id;
            match provision::reconcile_start(root, owner, id, inventory, now_ms(), ids::uuid_v4())
                .map_err(|_| unavailable())?
            {
                provision::ReconcilePlan::Change { candidate, .. } => {
                    exact(self.provision_publish(loaded, candidate).await?)?;
                }
                provision::ReconcilePlan::Unchanged(provision::ReconcileClass::Absent) => {
                    // A fresh complete inventory is consumed by each retry.
                    let inventory = self.provider_inventory(root, owner).await?;
                    let claim = provision::retry_start(
                        root,
                        owner,
                        id,
                        inventory,
                        ids::uuid_v4(),
                        &self.attached,
                        now_ms(),
                        ids::uuid_v4(),
                    )
                    .map_err(|_| unavailable())?;
                    self.deliver_start(loaded, claim, owner).await?;
                }
                provision::ReconcilePlan::Unchanged(
                    provision::ReconcileClass::NeedsTerminationReconciliation(binding),
                ) => {
                    let (_, node, _, _) = binding.into_parts();
                    let inventory = self.provider_inventory(root, owner).await?;
                    if let provision::TerminationPlan::Change(candidate) =
                        provision::reconcile_termination(
                            root,
                            owner,
                            node,
                            inventory,
                            now_ms(),
                            ids::uuid_v4(),
                        )
                        .map_err(|_| unavailable())?
                    {
                        self.termination_publish(loaded, candidate, owner).await?;
                    }
                }
                _ => {}
            }
            return Ok(());
        }
        // Inventory validates retained stop identities before another drain round.
        if let Some(intent) = root.intents.iter().find(|i| {
            i.action == Action::Stop
                && matches!(i.state, IntentState::InProgress | IntentState::Reconciling)
                && i.outcome.is_none()
        }) {
            let id = intent.request_id;
            let node = intent.node_id;
            let interrupted = (intent.state == IntentState::InProgress)
                .then(|| intent.delivery.as_ref())
                .flatten()
                .filter(|delivery| {
                    delivery.owner_id == owner.process_id && delivery.epoch == owner.epoch
                })
                .map(|delivery| delivery.invocation_id);
            match provision::reconcile_stop(root, owner, id, inventory, now_ms(), ids::uuid_v4())
                .map_err(|_| unavailable())?
            {
                provision::StopReconcilePlan::Change { candidate, .. } => {
                    exact(self.provision_publish(loaded, candidate).await?)?;
                    return Ok(());
                }
                provision::StopReconcilePlan::Unchanged(
                    provision::StopInventoryClass::NeedsTermination,
                ) => {
                    let inventory = self.provider_inventory(root, owner).await?;
                    if let provision::TerminationPlan::Change(candidate) =
                        provision::reconcile_termination(
                            root,
                            owner,
                            node,
                            inventory,
                            now_ms(),
                            ids::uuid_v4(),
                        )
                        .map_err(|_| unavailable())?
                    {
                        self.termination_publish(loaded, candidate, owner).await?;
                    }
                    return Ok(());
                }
                _ => {}
            }
            if let Some(invocation) = interrupted {
                // The previous serial tick has ended, including a local proof
                // refusal after the delivery claim. No same-owner InProgress
                // claim may prevent fresh proof-bound reconciliation forever.
                let command = callback::StopCommand::build(root, owner, id, invocation)
                    .map_err(|_| unavailable())?;
                let (_, context) = command.into_parts();
                let candidate =
                    provision::uncertain_stop(root, owner, &context, now_ms(), ids::uuid_v4())
                        .map_err(|_| unavailable())?;
                exact(self.provision_publish(loaded, candidate).await?)?;
                return Ok(());
            }
            // Keep the drain future out of the provider tick's stack frame.
            return Box::pin(self.provider_drain(node)).await;
        }
        if !provision::inventory_clean(root, owner, &inventory).map_err(|_| unavailable())? {
            let candidate =
                provision::inventory_failed_candidate(root, owner, now_ms(), ids::uuid_v4())
                    .map_err(|_| unavailable())?;
            exact(self.provision_publish(loaded, candidate).await?)?;
            return Ok(());
        }
        // Recover an API crash between the last termination publication and
        // its cold-run fence. Missing inventory is not termination evidence.
        if !root.nodes.is_empty()
            && root.nodes.iter().all(|n| n.state == NodeState::Stopped)
            && !self.new_run.load(Ordering::Acquire)
            && root.nodes.iter().all(|n| {
                inventory.records().iter().any(|r| {
                    r.node_id() == n.node_id
                        && Some(r.resource_id()) == n.resource_id.as_deref()
                        && r.state() == "terminated"
                })
            })
        {
            self.cold_start.store(true, Ordering::Release);
            if let Err(error) = self.ensure_catalog_fence(owner).await {
                // A failed proof-bound transition cannot authorize a later run
                // after another controller has acquired ownership.
                self.cold_start.store(false, Ordering::Release);
                return Err(error);
            }
            return Ok(());
        }
        if let Some(record) = inventory.records().iter().find(|r| {
            r.state() == "terminated"
                && root
                    .nodes
                    .iter()
                    .any(|n| n.node_id == r.node_id() && n.state != NodeState::Stopped)
        }) {
            let node = record.node_id();
            if let provision::TerminationPlan::Change(candidate) = provision::reconcile_termination(
                root,
                owner,
                node,
                inventory,
                now_ms(),
                ids::uuid_v4(),
            )
            .map_err(|_| unavailable())?
            {
                self.termination_publish(loaded, candidate, owner).await?;
            }
            return Ok(());
        }
        // Every inventory record must be associated before any new launch.
        for record in inventory.records() {
            if !root.intents.iter().any(|i| {
                i.action == Action::Start
                    && i.request_id == record.request_id()
                    && i.node_id == record.node_id()
                    && i.resource_id.as_deref() == Some(record.resource_id())
            }) {
                let candidate =
                    provision::inventory_failed_candidate(root, owner, now_ms(), ids::uuid_v4())
                        .map_err(|_| unavailable())?;
                exact(self.provision_publish(loaded, candidate).await?)?;
                return Ok(());
            }
        }
        if let Some(intent) = root
            .intents
            .iter()
            .find(|i| i.action == Action::Start && i.state == IntentState::Pending)
        {
            let claim = provision::claim_start(
                root,
                owner,
                intent.request_id,
                ids::uuid_v4(),
                &self.attached,
                now_ms(),
                ids::uuid_v4(),
            )
            .map_err(|_| unavailable())?;
            return self.deliver_start(loaded, claim, owner).await;
        }
        if let Some(node) = root
            .nodes
            .iter()
            .find(|n| n.state == NodeState::Draining && n.ownership == Ownership::Managed)
        {
            return Box::pin(self.provider_drain(node.node_id)).await;
        }
        if root.policy.mode == PolicyMode::Autoscale {
            if let Ok(Reply::Stats {
                pending,
                running,
                oldest_wait_ms,
                ..
            }) = self.queue_call(Command::Stats).await
            {
                if let Some(candidate) = provision::demand_candidate(
                    root,
                    owner,
                    pending,
                    running,
                    oldest_wait_ms,
                    now_ms(),
                    ids::uuid_v4(),
                )
                .map_err(|_| unavailable())?
                {
                    crate::node::queue_runtime::validate_coordination_capacity(
                        root,
                        None,
                        Some(root.policy.max_nodes),
                    )?;
                    exact(self.provision_publish(loaded, candidate).await?)?;
                    return Ok(());
                }
            }
        }
        let capacity = provision::managed_capacity(root).map_err(|_| unavailable())?;
        if capacity < root.policy.desired {
            crate::node::queue_runtime::validate_coordination_capacity(
                root,
                None,
                Some(root.policy.max_nodes),
            )?;
            let candidate = provision::allocate_start(
                root,
                owner,
                ids::uuid_v4(),
                ids::uuid_v4(),
                now_ms(),
                ids::uuid_v4(),
            )
            .map_err(|_| unavailable())?;
            exact(self.provision_publish(loaded, candidate).await?)?;
        } else if capacity > root.policy.desired {
            let eligible: Vec<_> = root
                .nodes
                .iter()
                .filter(|n| n.state == NodeState::Ready && n.registration == Registration::Verified)
                .collect();
            // Autoscaling retains its scheduler and a complete queue holder.
            if eligible.len() > 1 {
                if let Some(node) = eligible.iter().rev().find(|n| {
                    n.ownership == Ownership::Managed
                        && Some(n.node_id) != root.scheduler.coordinator_node
                }) {
                    let node_id = node.node_id;
                    let Reply::Joined { token } = self.queue_call(Command::Seal).await? else {
                        return Err(unavailable());
                    };
                    let candidate = drain::begin_drain(
                        root,
                        owner,
                        node_id,
                        ids::uuid_v4(),
                        now_ms(),
                        ids::uuid_v4(),
                    )
                    .map_err(|_| unavailable())?;
                    let changed = match candidate {
                        drain::DrainPlan::Change(c) => self
                            .drain_publish(loaded, c)
                            .await
                            .and_then(exact)
                            .map(|_| ()),
                        drain::DrainPlan::Existing(_) => Ok(()),
                    };
                    // Unseal only after every survivor acknowledged its complete copy.
                    let unsealed = self.queue_call(Command::Unseal { token }).await;
                    changed?;
                    unsealed?;
                }
            } else if eligible.len() == 1
                && root.policy.mode == PolicyMode::Fixed
                && root.policy.desired == 0
                && eligible[0].ownership == Ownership::Managed
            {
                // Explicit zero is an operator shutdown, not an idle autoscale.
                // Wait for every accepted request and coordination task to settle.
                let Reply::Stats {
                    pending: 0,
                    running: 0,
                    ..
                } = self.queue_call(Command::Stats).await?
                else {
                    return Ok(());
                };
                let Reply::Joined { token } = self.queue_call(Command::Seal).await? else {
                    return Err(unavailable());
                };
                let node_id = eligible[0].node_id;
                // Recheck after the seal excludes another acceptance racing Stats.
                let Reply::Stats {
                    pending: 0,
                    running: 0,
                    ..
                } = self.queue_call(Command::Stats).await?
                else {
                    self.queue_call(Command::Unseal { token }).await?;
                    return Ok(());
                };
                let plan = drain::begin_drain(
                    root,
                    owner,
                    node_id,
                    ids::uuid_v4(),
                    now_ms(),
                    ids::uuid_v4(),
                )
                .map_err(|_| unavailable())?;
                let changed = match plan {
                    drain::DrainPlan::Change(c) => self
                        .drain_publish(loaded, c)
                        .await
                        .and_then(exact)
                        .map(|_| ()),
                    drain::DrainPlan::Existing(_) => Ok(()),
                };
                if let Err(error) = changed {
                    let _ = self.queue_call(Command::Unseal { token }).await;
                    return Err(error);
                }
                // No ready receiver remains; keep admission sealed through exit.
                self.ensure_catalog_fence(owner).await?;
            }
        }
        Ok(())
    }

    async fn provider_drain(&self, node_id: Uuid) -> Result<()> {
        use drain::proof::{DrainCheck, DrainRound, IdlePlan};
        // Fence a relinquished last coordinator before constructing drain proof.
        // Repeating this after an API restart does not infer an empty/new run.
        let current = self.load().await?;
        let last = current
            .root()
            .nodes
            .iter()
            .all(|n| n.state != NodeState::Ready);
        if last {
            let owner = self.provider_authority(current.root())?;
            self.ensure_catalog_fence(owner).await?;
        }
        let loaded = self.load().await?;
        let root = loaded.root();
        let owner = self.provider_authority(root)?;
        if root.scheduler.coordinator_node == Some(node_id) {
            return Err(unavailable());
        }
        if root
            .nodes
            .iter()
            .filter(|n| n.state == NodeState::Ready && n.registration == Registration::Verified)
            .count()
            == 0
        {
            let node = root
                .nodes
                .iter()
                .find(|n| n.node_id == node_id && n.state == NodeState::Draining)
                .ok_or_else(unavailable)?;
            let body = serde_json::to_vec(&Command::Stats).map_err(|_| invalid())?;
            let bytes = crate::node::queue_transport::exchange(
                &self.client,
                &self.signer,
                root,
                node,
                body,
            )
            .await?;
            if !matches!(
                serde_json::from_slice::<Reply>(&bytes).map_err(|_| invalid())?,
                Reply::Stats {
                    pending: 0,
                    running: 0,
                    ..
                }
            ) {
                return Ok(());
            }
        }
        let node = root
            .nodes
            .iter()
            .find(|n| n.node_id == node_id && n.state == NodeState::Draining)
            .ok_or_else(unavailable)?
            .clone();
        let drain_id = node.drain.as_ref().ok_or_else(unavailable)?.id;
        let drain::DrainPlan::Existing(binding) =
            drain::begin_drain(root, owner, node_id, drain_id, now_ms(), ids::uuid_v4())
                .map_err(|_| unavailable())?
        else {
            return Err(unavailable());
        };
        let bytes =
            serde_json::to_vec(&json!({"version":1,"drain_id":drain_id})).map_err(|_| invalid())?;
        let context = self.signer.context(
            root,
            &node,
            auth::Route::Drain,
            auth::RequestIds::default(),
            bytes.clone(),
        )?;
        let round = DrainRound::new(
            root,
            owner,
            binding,
            context.auth().clone(),
            bytes,
            &mut self.guard.lock().unwrap(),
            sample().map_err(|_| unavailable())?,
        )
        .map_err(|_| unavailable())?;
        let end = deadline();
        let request = self.signer.request(
            node.endpoint.as_deref().ok_or_else(unavailable)?,
            round.context().map_err(|_| unavailable())?,
            Body::from(
                round
                    .context()
                    .map_err(|_| unavailable())?
                    .initial_bytes()
                    .to_vec(),
            ),
            false,
        )?;
        let response = self.client.request(request, end).await?;
        let status = response.status().as_u16();
        let signed_auth = http::one(response.headers(), "x-nabedra-auth")?.to_owned();
        let signature = http::one(response.headers(), "x-nabedra-signature")?.to_owned();
        let length = http::framing(response.headers(), 1024 * 1024, false)?.ok_or_else(invalid)?;
        let bytes = http::read_small(response.into_body(), length, end).await?;
        let signed = auth::verify_reply(
            &signed_auth,
            &signature,
            status,
            round.context().map_err(|_| unavailable())?,
        )
        .map_err(|_| invalid())?;
        let content = auth::check_reply_content(&signed, bytes).map_err(|_| invalid())?;
        let eligible =
            auth::check_reply_authority(&signed, root, round.context().map_err(|_| unavailable())?)
                .map_err(|_| invalid())?;
        let bound = round
            .bind_reply(eligible, content)
            .map_err(|_| unavailable())?;
        let binding = heartbeat_binding(root, &node)?;
        let heartbeat = load_heartbeat(&self.storage, &binding, end)
            .await
            .map_err(|_| unavailable())?
            .ok_or_else(unavailable)?;
        let heartbeat = self
            .observations
            .lock()
            .unwrap()
            .drain_heartbeat(
                root,
                &binding,
                &heartbeat,
                time::now(),
                Duration::from_millis(root.timing.expiry_ms),
            )
            .map_err(|_| unavailable())?;
        let proof = {
            let mut observations = self.observations.lock().unwrap();
            let mut guard = self.guard.lock().unwrap();
            match drain::proof::check_drain_reply(
                root,
                owner,
                bound,
                self.signer.sender(),
                heartbeat,
                &mut observations,
                &mut guard,
                sample().map_err(|_| unavailable())?,
            )
            .map_err(|_| unavailable())?
            {
                DrainCheck::Positive(p) => p,
                DrainCheck::Pending => return Ok(()),
            }
        };
        let plan = {
            let mut observations = self.observations.lock().unwrap();
            let mut guard = self.guard.lock().unwrap();
            drain::proof::confirm_idle(
                root,
                owner,
                proof,
                ids::uuid_v4(),
                &mut observations,
                &mut guard,
                sample().map_err(|_| unavailable())?,
            )
            .map_err(|_| unavailable())?
        };
        let (loaded, proof) = match plan {
            IdlePlan::Existing(proof) => (loaded, proof),
            IdlePlan::Change(candidate, pending) => {
                let outcome = self.drain_publish(loaded, candidate).await?;
                let loaded = match &outcome {
                    OwnershipOutcome::Acknowledged(v) | OwnershipOutcome::ObservedExact(v) => {
                        v.clone()
                    }
                    _ => return Err(unavailable()),
                };
                (loaded, pending.confirm(outcome).map_err(|_| unavailable())?)
            }
        };
        let proof = {
            let mut observations = self.observations.lock().unwrap();
            let mut guard = self.guard.lock().unwrap();
            proof
                .check_live(
                    loaded.root(),
                    owner,
                    &mut observations,
                    &mut guard,
                    sample().map_err(|_| unavailable())?,
                )
                .map_err(|_| unavailable())?
        };
        let (loaded, proof, stop) = match drain::allocate_stop(
            loaded.root(),
            owner,
            proof,
            ids::uuid_v4(),
            now_ms(),
            ids::uuid_v4(),
        )
        .map_err(|_| unavailable())?
        {
            drain::StopPlan::Existing(existing) => {
                let (proof, id) = existing.into_parts();
                (loaded, proof, id)
            }
            drain::StopPlan::Change(candidate, pending) => {
                let outcome = self.drain_publish(loaded, candidate).await?;
                let loaded = match &outcome {
                    OwnershipOutcome::Acknowledged(v) | OwnershipOutcome::ObservedExact(v) => {
                        v.clone()
                    }
                    _ => return Err(unavailable()),
                };
                let proof = pending.confirm(outcome).map_err(|_| unavailable())?;
                let id = loaded
                    .root()
                    .intents
                    .iter()
                    .find(|i| i.action == Action::Stop && i.node_id == node_id)
                    .ok_or_else(unavailable)?
                    .request_id;
                (loaded, proof, id)
            }
        };
        let proof = {
            let mut observations = self.observations.lock().unwrap();
            let mut guard = self.guard.lock().unwrap();
            proof
                .check_live(
                    loaded.root(),
                    owner,
                    &mut observations,
                    &mut guard,
                    sample().map_err(|_| unavailable())?,
                )
                .map_err(|_| unavailable())?
        };
        let intent = loaded
            .root()
            .intents
            .iter()
            .find(|i| i.request_id == stop)
            .ok_or_else(unavailable)?;
        let claim = if intent.state == IntentState::Pending {
            provision::claim_stop(
                loaded.root(),
                owner,
                stop,
                proof,
                ids::uuid_v4(),
                now_ms(),
                ids::uuid_v4(),
            )
        } else {
            let inventory = self.provider_inventory(loaded.root(), owner).await?;
            provision::retry_stop(
                loaded.root(),
                owner,
                stop,
                proof,
                inventory,
                ids::uuid_v4(),
                now_ms(),
                ids::uuid_v4(),
            )
        }
        .map_err(|_| unavailable())?;
        let (candidate, pending) = claim.into_parts();
        let outcome = self.provision_publish(loaded, candidate).await?;
        let confirmed = pending.confirm(outcome).map_err(|_| unavailable())?;
        let (bytes, context, proof) = confirmed.into_parts();
        let loaded = self.load().await?;
        let proof = {
            let mut observations = self.observations.lock().unwrap();
            let mut guard = self.guard.lock().unwrap();
            proof
                .check_live(
                    loaded.root(),
                    owner,
                    &mut observations,
                    &mut guard,
                    sample().map_err(|_| unavailable())?,
                )
                .map_err(|_| unavailable())?
        };
        #[derive(Deserialize)]
        struct DeliveryId {
            invocation_id: Uuid,
        }
        let invocation = serde_json::from_slice::<DeliveryId>(&bytes)
            .map_err(|_| invalid())?
            .invocation_id;
        self.bridge
            .as_ref()
            .ok_or_else(unavailable)?
            .stops
            .lock()
            .unwrap()
            .insert(invocation, proof);
        let reply = self.callback_exchange(bytes, &context).await;
        self.bridge
            .as_ref()
            .ok_or_else(unavailable)?
            .stops
            .lock()
            .unwrap()
            .remove(&invocation);
        let loaded = self.load().await?;
        let candidate = match reply {
            Ok(callback::CallbackReply::Failure { code, .. })
                if matches!(
                    code.as_str(),
                    "ownership_unavailable"
                        | "ownership_changed"
                        | "callback_capacity"
                        | "bridge_not_active"
                        | "callback_timeout"
                        | "callback_in_progress"
                ) =>
            {
                // A transient bridge refusal supplies no permission to abandon
                // the stop lifecycle. Retry only with fresh inventory and proof.
                provision::uncertain_stop(loaded.root(), owner, &context, now_ms(), ids::uuid_v4())
            }
            Ok(reply) => provision::complete_stop(
                loaded.root(),
                owner,
                &context,
                reply,
                now_ms(),
                ids::uuid_v4(),
            ),
            Err(_) => {
                provision::uncertain_stop(loaded.root(), owner, &context, now_ms(), ids::uuid_v4())
            }
        }
        .map_err(|_| unavailable())?;
        exact(self.provision_publish(loaded, candidate).await?)?;
        Ok(())
    }
}

async fn authorize(State(api): State<Arc<Api>>, request: Request) -> Response {
    let Some(bridge) = &api.bridge else {
        return response(EncodedResponse {
            status: 503,
            body: br#"{"version":1,"invocation_id":null,"error":{"code":"ownership_unavailable"}}"#
                .to_vec(),
        });
    };
    let result=async {
        if !api.accepting.load(Ordering::Acquire){return Err((503,"ownership_unavailable"));}
        if !http::bearer(request.headers(),&bridge.authorize_token){return Err((401,"unauthorized"));}
        let _admitted=api.controls.try_admit().map_err(|_|(503,"ownership_unavailable"))?;
        let length=http::framing(request.headers(),1024*1024,false).map_err(|_|(413,"limit_exceeded"))?.ok_or((400,"invalid_command"))?;
        let bytes=http::read_small(request.into_body(),length,deadline()).await.map_err(|_|(400,"invalid_command"))?;
        let loaded=api.load().await.map_err(|_|(503,"ownership_unavailable"))?;
        let (invocation,owner,digest,is_stop)=callback::authorize_command(&bytes,loaded.root(),&api.attached).map_err(|_|(409,"ownership_changed"))?;
        if api.provider_authority(loaded.root()).map_err(|_|(503,"ownership_unavailable"))?!=owner{return Err((409,"ownership_changed"));}
        if is_stop {
            let proof=bridge.stops.lock().unwrap().remove(&invocation).ok_or((409,"ownership_changed"))?;
            let mut observations=api.observations.lock().unwrap();let mut guard=api.guard.lock().unwrap();
            proof.check_live(loaded.root(),owner,&mut observations,&mut guard,sample().map_err(|_|(503,"ownership_unavailable"))?).map_err(|_|(409,"ownership_changed"))?;
        }
        Ok(json!({"version":1,"invocation_id":invocation,"command_sha256":digest,"authorized":true}))
    }.await;
    let (status, value) = match result {
        Ok(v) => (200, v),
        Err((s, c)) => (
            s,
            json!({"version":1,"invocation_id":null,"error":{"code":c}}),
        ),
    };
    response(EncodedResponse {
        status,
        body: serde_json::to_vec(&value).unwrap(),
    })
}
pub(super) async fn authorizer(
    api: Arc<Api>,
    listener: TcpListener,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    axum::serve(
        listener,
        Router::new()
            .route("/private/v1/authorize", post(authorize))
            .with_state(api),
    )
    .with_graceful_shutdown(async move {
        let _ = stop.changed().await;
    })
    .await
    .map_err(|_| unavailable())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlRequest {
    #[serde(deserialize_with = "crate::cluster::member::uuid")]
    request_id: Uuid,
    operation: String,
    payload: Box<serde_json::value::RawValue>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyControl {}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScaleControl {
    nodes: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AutoscaleControl {
    min_nodes: u64,
    max_nodes: u64,
}
fn control_object<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    crate::cluster::configuration::input(bytes, crate::storage::MAX_CONTROL_BODY_BYTES)
        .map_err(|_| invalid())?;
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let value = protocol::deserialize_object(&mut decoder).map_err(|_| invalid())?;
    decoder.end().map_err(|_| invalid())?;
    Ok(value)
}
fn admit_control(bytes: &[u8]) -> Result<ControlRequest> {
    let request: ControlRequest = control_object(bytes)?;
    match request.operation.as_str() {
        "cluster.status" => {
            let _: EmptyControl = control_object(request.payload.get().as_bytes())?;
        }
        "cluster.scale" => {
            let payload: ScaleControl = control_object(request.payload.get().as_bytes())?;
            let _ = payload.nodes;
        }
        "cluster.autoscale" => {
            let payload: AutoscaleControl = control_object(request.payload.get().as_bytes())?;
            let _ = (payload.min_nodes, payload.max_nodes);
        }
        _ => return Err(failure(ErrorCode::UnsupportedOperation)),
    }
    Ok(request)
}
fn requested_capacity(request: &ControlRequest) -> Result<Option<u64>> {
    Ok(match request.operation.as_str() {
        "cluster.scale" => {
            let p: ScaleControl = control_object(request.payload.get().as_bytes())?;
            (p.nodes <= 1024).then_some(p.nodes)
        }
        "cluster.autoscale" => {
            let p: AutoscaleControl = control_object(request.payload.get().as_bytes())?;
            (p.min_nodes >= 1 && p.min_nodes <= p.max_nodes && p.max_nodes <= 1024)
                .then_some(p.max_nodes)
        }
        _ => None,
    })
}
impl Api {
    async fn control_status(&self) -> Result<serde_json::Value> {
        let end = time::now() + Duration::from_secs(5);
        let loaded = self
            .storage
            .load_control(end)
            .await
            .map_err(|_| unavailable())?;
        let root = loaded.root();
        if root.nodes.len() + root.intents.len() > 4096 {
            return Err(failure(ErrorCode::LimitExceeded));
        }
        self.status_observations
            .lock()
            .unwrap()
            .reconcile_members(root, time::now())
            .map_err(|_| unavailable())?;
        let mut nodes = Vec::with_capacity(root.nodes.len());
        let mut all_fresh = true;
        for node in &root.nodes {
            let heartbeat = if matches!(node.state, NodeState::Ready | NodeState::Draining)
                && node.registration != Registration::Unregistered
            {
                if let Ok(binding) = heartbeat_binding(root, node) {
                    match load_heartbeat(&self.storage, &binding, end).await {
                        Ok(Some(loaded)) => {
                            let mut observations = self.status_observations.lock().unwrap();
                            observations
                                .observe(&binding, loaded.heartbeat(), time::now())
                                .map_err(|_| unavailable())?;
                            observations
                                .status_heartbeat(
                                    &binding,
                                    time::now(),
                                    Duration::from_millis(root.timing.expiry_ms),
                                )
                                .map_err(|_| unavailable())?
                        }
                        _ => {
                            self.status_observations
                                .lock()
                                .unwrap()
                                .read_failed(&binding, time::now())
                                .map_err(|_| unavailable())?;
                            None
                        }
                    }
                } else {
                    None
                }
            } else {
                None
            };
            if matches!(node.state, NodeState::Ready | NodeState::Draining) && heartbeat.is_none() {
                all_fresh = false;
            }
            let h = heartbeat.as_ref().map(|(h, _)| h);
            nodes.push(json!({"node_id":node.node_id,"incarnation":node.incarnation,"resource_id":node.resource_id,"ownership":node.ownership,"state":node.state,
                "operation_slots":node.operation_slots,"partition_slots":node.partition_slots,
                "running_operations":h.map_or(0,|h|h.running_operations),"running_partitions":h.map_or(0,|h|h.running_partitions),
                "queued_operations":h.map_or(0,|h|h.queued_operations),"queued_partitions":h.map_or(0,|h|h.queued_partitions),
                "heartbeat_age_seconds":heartbeat.as_ref().map(|(_,age)|age.as_secs_f64()),"resources":node.resources,
                "available_threads":h.and_then(|h|h.available_threads),"available_memory_bytes":h.and_then(|h|h.available_memory_bytes)}));
        }
        // Status observes the retained lease. It never advances or invalidates
        // the live authorization guard while another task owns a drain proof.
        let active = root.controller.lease_state == LeaseState::Held
            && root.controller.owner_id.is_some()
            && root
                .controller
                .lease_until_ms
                .is_some_and(|until| until > now_ms());
        let managed = self.bridge.is_some() && root.provisioning == Provisioning::Managed;
        let mut value = serde_json::to_value(&root.policy).map_err(|_| invalid())?;
        let object = value.as_object_mut().ok_or_else(invalid)?;
        object.extend(json!({"version":1,"cluster_id":root.cluster_id,"name":root.name,
            "capabilities":{"provision":managed,"terminate":managed,"autoscale":managed},
            "controller":{"epoch":root.controller.epoch,"active":active,"degraded":now_ms()<root.scheduler.backoff_until_ms},
            "counts":{"ready":root.nodes.iter().filter(|n|n.state==NodeState::Ready).count(),"starting":root.nodes.iter().filter(|n|n.state==NodeState::Starting).count(),"draining":root.nodes.iter().filter(|n|n.state==NodeState::Draining).count(),"lost":root.nodes.iter().filter(|n|n.state==NodeState::Lost).count()},
            "metrics_fresh":all_fresh,"nodes":nodes,
            "intents":root.intents.iter().map(|i|json!({"request_id":i.request_id,"node_id":i.node_id,"resource_id":i.resource_id,"action":i.action,"state":i.state,"attempts":i.attempts,"error_code":i.error_code})).collect::<Vec<_>>()}).as_object().ok_or_else(invalid)?.clone());
        Ok(value)
    }
    async fn control_operation(&self, bytes: Vec<u8>) -> Result<EncodedResponse> {
        use crate::cluster::{
            admin_history::{AdminHistoryBudget, AdminLookup},
            store::{AdminPublicationOutcome, PreparedAdmin},
        };
        let request = admit_control(&bytes)?;
        if request.operation == "cluster.status" {
            let data = self.control_status().await?;
            return Ok(EncodedResponse {
                status: 200,
                body: serde_json::to_vec(&json!({"request_id":request.request_id,"data":data}))
                    .map_err(|_| invalid())?,
            });
        }
        // Authentication/admission happened before this single fixed budget and
        // original-byte history search. Only a proven CAS loser may recompute.
        let budget = AdminHistoryBudget::new(deadline()).map_err(|_| unavailable())?;
        let _serial = self.changes.lock().await;
        for _ in 0..4 {
            let loaded = self
                .storage
                .load_control(budget.deadline())
                .await
                .map_err(|_| unavailable())?;
            match self
                .storage
                .lookup_admin(&loaded, request.request_id, &bytes, budget)
                .await
                .map_err(|_| unknown())?
            {
                AdminLookup::Found(d) => {
                    return Ok(EncodedResponse {
                        status: d.response_status,
                        body: d.response_body.into_bytes(),
                    });
                }
                AdminLookup::IdentityConflict => return Err(failure(ErrorCode::Conflict)),
                AdminLookup::Absent => {}
            }
            if loaded.root().provisioning == Provisioning::Managed {
                if let Some(desired) = requested_capacity(&request)? {
                    crate::node::queue_runtime::validate_coordination_capacity(
                        loaded.root(),
                        None,
                        Some(desired),
                    )?;
                }
            }
            let prepared =
                PreparedAdmin::prepare(loaded, request.request_id, &bytes, ids::uuid_v4())
                    .map_err(|_| invalid())?;
            match prepared
                .publish(&self.storage, budget)
                .await
                .map_err(|_| unknown())?
            {
                AdminPublicationOutcome::Terminal(d) => {
                    return Ok(EncodedResponse {
                        status: d.response_status,
                        body: d.response_body.into_bytes(),
                    });
                }
                AdminPublicationOutcome::IdentityConflict => {
                    return Err(failure(ErrorCode::Conflict));
                }
                AdminPublicationOutcome::NeedsRecompute => {}
                AdminPublicationOutcome::Unknown => return Err(unknown()),
            }
        }
        Err(unknown())
    }
}
pub(super) async fn cluster_operations(State(api): State<Arc<Api>>, request: Request) -> Response {
    if !api.accepting.load(Ordering::Acquire) {
        return response(encode_error(None, unavailable()));
    }
    if !http::bearer(
        request.headers(),
        &api.attached.local().prepared.auth.admin_token,
    ) {
        return response(encode_error(None, failure(ErrorCode::Unauthorized)));
    }
    let admitted = match api.controls.try_admit() {
        Ok(value) => value,
        Err(error) => return response(encode_error(None, error)),
    };
    let work = admitted.spawn(move || async move {
        let result = async {
            let length = http::framing(
                request.headers(),
                crate::storage::MAX_CONTROL_BODY_BYTES,
                false,
            )?
            .ok_or_else(invalid)?;
            let bytes = http::read_small(request.into_body(), length, deadline()).await?;
            let identity = admit_control(&bytes)?.request_id;
            Ok((identity, api.control_operation(bytes).await))
        }
        .await;
        Ok(response(match result {
            Ok((_, Ok(value))) => value,
            Ok((id, Err(error))) => encode_error(Some(id), error),
            Err(error) => encode_error(None, error),
        }))
    });
    match work {
        Ok(work) => work
            .receive()
            .await
            .unwrap_or_else(|e| response(encode_error(None, e))),
        Err(e) => response(encode_error(None, e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn control_admission_rejects_duplicates_wrong_payloads_and_noncanonical_ids() {
        let id = "00000000-0000-4000-8000-000000000001";
        for bytes in [
            format!(r#"{{"request_id":"{id}","operation":"cluster.scale","payload":{{"nodes":2,"nodes":3}}}}"#),
            format!(r#"{{"request_id":"{id}","operation":"cluster.status","payload":{{"unexpected":1}}}}"#),
            format!(r#"{{"request_id":"{id}","operation":"cluster.scale","payload":{{"nodes":true}}}}"#),
            format!(r#"{{"request_id":"{id}","request_id":"{id}","operation":"cluster.status","payload":{{}}}}"#),
            r#"{"request_id":"00000000000040008000000000000001","operation":"cluster.status","payload":{}}"#.into(),
            r#"["00000000-0000-4000-8000-000000000001","cluster.status",{}]"#.into(),
        ] {assert!(admit_control(bytes.as_bytes()).is_err());}
        assert!(admit_control(format!(r#"{{"request_id":"{id}","operation":"cluster.autoscale","payload":{{"min_nodes":1,"max_nodes":2}}}}"#).as_bytes()).is_ok());
    }
}
