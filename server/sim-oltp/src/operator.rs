//! The operator of a simulated cluster. Its requests run in their own tasks, so they go on while
//! the nemesis waits for a restart, and it asks every worker, because it can't know which one
//! holds the role that an operation needs.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use nabedra_protocol::protocol::{ErrorCode, Outcome};
use nabedra_rt::{
    ids::uuid_v4,
    sim::{World, in_node},
    time,
};
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::{
    bootstrap::{BOOTSTRAP_CLIENT, Mounts},
    client::{operation, request},
    history::Completion,
    invariants::{Invariants, TraceObserver, elapsed_ms},
};

/// What the operator needs of a cluster. It outlives every process.
#[derive(Clone)]
pub(crate) struct Operator {
    pub world: Arc<World>,
    pub checks: Invariants,
    pub trace: TraceObserver,
    pub token: String,
    pub data_token: String,
    pub cluster_id: Uuid,
    pub mounts: Mounts,
    /// The current workers. Only the membership workload replaces one.
    pub workers: [u64; 3],
}

impl Operator {
    pub(crate) fn record(&self, line: String) {
        (self.trace)(elapsed_ms(&self.world), line);
    }

    pub(crate) async fn id(&self) -> Uuid {
        in_node(BOOTSTRAP_CLIENT, async { uuid_v4() }).await
    }

    /// One administration request to `node`. A lost reply is an unknown outcome.
    pub(crate) async fn ask(
        &self,
        node: u64,
        id: Uuid,
        name: &str,
        payload: Value,
        limit: Duration,
    ) -> Completion {
        let body = operation(id, name, payload);
        let path = "/v1/cluster/operations";
        request(BOOTSTRAP_CLIENT, node, path, &self.token, Some(body), limit)
            .await
            .and_then(|reply| reply.completion(id))
            .unwrap_or_else(Completion::Unknown)
    }

    pub(crate) async fn stats(&self, node: u64) -> Result<Value, String> {
        let path = "/v1/runtime/stats";
        let limit = Duration::from_secs(2);
        let reply = request(BOOTSTRAP_CLIENT, node, path, &self.data_token, None, limit).await?;
        if reply.status != 200 {
            return Err(format!("n{node} stats: {} {}", reply.status, reply.body));
        }
        Ok(reply.body)
    }

    #[cfg(test)]
    pub(crate) fn for_test(world: &Arc<World>) -> Self {
        Self {
            world: world.clone(),
            checks: Invariants::new(),
            trace: Arc::new(|_, _| {}),
            token: String::new(),
            data_token: String::new(),
            cluster_id: Uuid::nil(),
            mounts: Mounts::for_test(),
            workers: crate::bootstrap::WORKERS,
        }
    }

    /// The first success of one request on any worker.
    pub(crate) async fn any(
        &self,
        id: Uuid,
        name: &str,
        payload: &Value,
        limit: Duration,
    ) -> Result<Value, String> {
        let mut errors = Vec::new();
        for node in self.workers {
            match self.ask(node, id, name, payload.clone(), limit).await {
                Completion::Ok(data) => return Ok(data),
                other => errors.push(format!("n{node} {}", shown(&other))),
            }
        }
        Err(format!("{name}: {}", errors.join("; ")))
    }
}

pub(crate) fn shown(answer: &Completion) -> String {
    match answer {
        Completion::Ok(value) => format!("ok {value}"),
        Completion::Refused(error) => format!("{:?} {}", error.code, error.message),
        Completion::Unknown(reason) => format!("unknown ({reason})"),
    }
}

const SPLIT_PAUSE: Duration = Duration::from_millis(250);
const SPLIT_REQUEST: Duration = Duration::from_secs(10);
/// A plan that was refused this often in a row is given up for a new one.
const REFUSALS: u32 = 8;
/// New plans per worker run.
const PLANS: u32 = 8;
/// Every plan that was given up is asked for again once in this many rounds. An exact retry
/// may settle it, and an unsettled plan may hold the range.
const REVISIT: u32 = 8;

/// One split request: an ID, and how it was answered.
#[derive(Clone, Debug)]
pub(crate) struct Plan {
    pub id: Uuid,
    /// The last answer said that the split did not happen.
    pub refused: bool,
    pub last: String,
    /// Refusals in a row.
    in_a_row: u32,
    /// Every worker stopped after it was requested.
    pub abandoned: bool,
}

#[derive(Default)]
struct SplitState {
    plans: Vec<Plan>,
    /// Plans of the current worker run.
    run: u32,
    /// Every definite refusal, by its message.
    refusals: BTreeMap<String, u64>,
    published: Option<(u64, Uuid, Value)>,
}

/// How a split round ended.
enum Round {
    Published(Value),
    /// Refused for good: the same plan has no other answer until something settles it.
    Refused(String, bool),
    Pending(String),
}

/// A split that an operator wants. The same ID is retried while its outcome is open. A plan that
/// is refused again and again is replaced by a new one under a new ID, a bounded number of
/// times, and every refusal is kept.
pub(crate) struct Split {
    op: Operator,
    payload: Value,
    state: Arc<Mutex<SplitState>>,
    task: Option<JoinHandle<()>>,
}

impl Split {
    pub(crate) async fn start(op: Operator, source: Value, boundary: i64) -> Self {
        let mut split = Self {
            op,
            payload: json!({"source": source, "boundary": boundary, "page_bytes": 1 << 20}),
            state: Arc::default(),
            task: None,
        };
        split.resume().await;
        split
    }

    fn state(&self) -> MutexGuard<'_, SplitState> {
        self.state.lock().unwrap()
    }

    /// A new plan for the current worker run, and the task that asks for it.
    pub(crate) async fn resume(&mut self) {
        if self.state().published.is_some() {
            return;
        }
        let id = self.op.id().await;
        self.op.record(format!("operator split plan {id}"));
        let mut state = self.state();
        state.run = 1;
        state.plans.push(plan(id));
        drop(state);
        let drive = drive(self.op.clone(), self.payload.clone(), self.state.clone());
        self.task = Some(nabedra_rt::task::spawn(drive));
    }

    /// Before every worker stops: ask once more, so that the last answer is known, and stop
    /// asking. The stop abandons every plan.
    pub(crate) async fn pause(&mut self) {
        self.stop().await;
        if self.state().published.is_some() {
            return;
        }
        let current = self.state().plans.len() - 1;
        step(&self.op, &self.payload, &self.state, current).await;
        for plan in &mut self.state().plans {
            plan.abandoned = true;
        }
    }

    pub(crate) async fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }

    /// When it published, under which ID, and the publication.
    pub(crate) fn published(&self) -> Option<(u64, Uuid, Value)> {
        self.state().published.clone()
    }

    pub(crate) fn plans(&self) -> Vec<Plan> {
        self.state().plans.clone()
    }

    pub(crate) fn summary(&self) -> Value {
        let state = self.state();
        let published = state.published.as_ref();
        json!({
            "plans": state.plans.len(),
            "refusals": state.refusals,
            "published_ms": published.map(|(at, _, _)| at),
            "published_plan": published
                .and_then(|(_, id, _)| state.plans.iter().position(|plan| plan.id == *id)),
            "last": state.plans.iter().map(|plan| &plan.last).collect::<Vec<_>>(),
        })
    }

    /// A plan that a full stop abandoned must not have published in the next run.
    pub(crate) async fn check_abandoned(&self) {
        for plan in self.plans().into_iter().filter(|plan| plan.abandoned) {
            let round = ask(&self.op, plan.id, &self.payload).await;
            let published = match round {
                Round::Published(result) => Some(result),
                Round::Refused(..) | Round::Pending(_) => None,
            };
            self.op.record(format!(
                "abandoned split {} refused before the stop: {}, published: {published:?}",
                plan.id, plan.refused
            ));
            if let (true, Some(result)) = (plan.refused, published) {
                self.op.checks.violate(
                    &self.op.world,
                    "AbandonedSplitReplayed",
                    format!(
                        "split {} was refused before a full stop and published after it: {result}",
                        plan.id
                    ),
                );
            }
        }
    }
}

impl Drop for Split {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

fn plan(id: Uuid) -> Plan {
    Plan {
        id,
        refused: false,
        last: String::new(),
        in_a_row: 0,
        abandoned: false,
    }
}

async fn drive(op: Operator, payload: Value, state: Arc<Mutex<SplitState>>) {
    for round in 1_u32.. {
        let current = state.lock().unwrap().plans.len() - 1;
        if step(&op, &payload, &state, current).await {
            return;
        }
        if round.is_multiple_of(REVISIT) {
            for older in 0..current {
                if !state.lock().unwrap().plans[older].abandoned
                    && step(&op, &payload, &state, older).await
                {
                    return;
                }
            }
        }
        if state.lock().unwrap().gives_up(current) {
            let id = op.id().await;
            let mut state = state.lock().unwrap();
            let last = &state.plans[current];
            op.record(format!(
                "operator gives up split plan {} ({}), asks for {id}",
                last.id, last.last
            ));
            state.plans.push(plan(id));
        }
        time::sleep(SPLIT_PAUSE).await;
    }
}

/// One round for one plan. Returns whether the split published.
async fn step(op: &Operator, payload: &Value, state: &Mutex<SplitState>, index: usize) -> bool {
    let id = state.lock().unwrap().plans[index].id;
    let round = ask(op, id, payload).await;
    let mut state = state.lock().unwrap();
    if let Some(line) = state.answered(elapsed_ms(&op.world), index, round) {
        op.record(format!("operator split {id}: {line}"));
    }
    state.published.is_some()
}

impl SplitState {
    /// Keep the answer of one round. Returns a line for the trace when it is a new one.
    fn answered(&mut self, at: u64, index: usize, round: Round) -> Option<String> {
        let plan = &mut self.plans[index];
        plan.refused = matches!(round, Round::Refused(_, true));
        let last = match round {
            Round::Published(result) => {
                let line = format!("published {result}");
                self.published = Some((at, plan.id, result));
                line
            }
            Round::Refused(answer, _) => {
                plan.in_a_row += 1;
                *self.refusals.entry(answer.clone()).or_default() += 1;
                answer
            }
            Round::Pending(answer) => {
                plan.in_a_row = 0;
                answer
            }
        };
        let new = plan.last != last;
        plan.last.clone_from(&last);
        new.then_some(last)
    }

    /// Whether the current plan is given up for a new one.
    fn gives_up(&mut self, current: usize) -> bool {
        let gives_up = self.plans[current].in_a_row >= REFUSALS && self.run < PLANS;
        self.run += u32::from(gives_up);
        gives_up
    }
}

/// One request on each worker in turn, until one that can run it answers.
async fn ask(op: &Operator, id: Uuid, payload: &Value) -> Round {
    let mut last = Vec::new();
    for node in op.workers {
        let payload = payload.clone();
        let answer = op
            .ask(node, id, "storage.shard.split", payload, SPLIT_REQUEST)
            .await;
        match &answer {
            Completion::Ok(result) if result["state"] == "published" => {
                return Round::Published(result.clone());
            }
            Completion::Ok(result) => return Round::Pending(result.to_string()),
            Completion::Refused(error)
                if matches!(
                    error.code,
                    ErrorCode::Conflict
                        | ErrorCode::ValidationError
                        | ErrorCode::NotFound
                        | ErrorCode::LimitExceeded
                ) =>
            {
                let not_applied = error.outcome == Outcome::NotApplied;
                return Round::Refused(shown(&answer), not_applied);
            }
            _ => last.push(format!("n{node} {}", shown(&answer))),
        }
    }
    Round::Pending(last.join("; "))
}

#[cfg(test)]
mod tests {
    use super::*;

    const FREEZE: &str = "Conflict source refused the exact split freeze";

    /// Review 115, H4: only the first ID was ever asked for, so its first refusal decided the
    /// seed. A refused plan is replaced, the refusals stay, and the plans are bounded.
    #[test]
    fn a_refused_split_gets_a_new_plan_and_keeps_its_refusals() {
        let mut state = SplitState {
            run: 1,
            plans: vec![plan(Uuid::nil())],
            ..SplitState::default()
        };
        let mut rounds = 0;
        for _ in 0..REFUSALS * PLANS * 2 {
            let current = state.plans.len() - 1;
            state.answered(1, current, Round::Refused(FREEZE.into(), true));
            rounds += 1;
            if state.gives_up(current) {
                assert!(state.plans[current].refused);
                state.plans.push(plan(Uuid::nil()));
            }
        }
        assert_eq!(state.plans.len() as u32, PLANS);
        assert_eq!(state.refusals[FREEZE], rounds);
        // A retry that finds the split open is no refusal, and starts the count again.
        let current = state.plans.len() - 1;
        state.run = 1;
        state.answered(2, current, Round::Pending("importing".into()));
        assert!(!state.plans[current].refused && !state.gives_up(current));
        // An older plan that publishes settles the split.
        state.answered(3, 0, Round::Published(json!({"state": "published"})));
        assert_eq!(state.published.as_ref().map(|(at, ..)| *at), Some(3));
    }

    /// "Belongs to a fenced scheduler owner" replaces the plan, but says nothing about what it
    /// did, so a stop after it proves no replay.
    #[test]
    fn an_unknown_outcome_is_not_a_refusal_before_a_stop() {
        let mut state = SplitState {
            run: 1,
            plans: vec![plan(Uuid::nil())],
            ..SplitState::default()
        };
        for _ in 0..REFUSALS {
            state.answered(1, 0, Round::Refused("fenced".into(), false));
        }
        assert!(!state.plans[0].refused && state.gives_up(0));
    }
}
