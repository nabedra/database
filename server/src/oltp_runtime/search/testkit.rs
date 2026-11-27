//! Fakes of the search ports (contract-w2 section 3). Each fake is scripted:
//! it records every call and answers the replies pushed into it, in order; an
//! empty script answers `UnsupportedOperation`. The owner of a port grows its
//! fake as its tests need (W: `FakeFeed` with generated logs, `FakeLeader` with
//! committed versus applied indexes; I: `FakeWorker` with a brute-force table;
//! K: `FakeProvider` as a local HTTP server).
#![allow(dead_code)]
use super::ports::*;
use crate::{
    oltp::mvcc::store::{ParticipantHandle, search::SearchReply},
    oltp_replication::{search_rpc::*, types::ColumnId},
    protocol::{ErrorCode, Outcome, ProtocolError},
};
use bytes::Bytes;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
use uuid::Uuid;

/// Replies in order, and every request seen.
pub(crate) struct Script<Req, Rep> {
    calls: Mutex<Vec<Req>>,
    replies: Mutex<VecDeque<Result<Rep, ProtocolError>>>,
}
impl<Req, Rep> Default for Script<Req, Rep> {
    fn default() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            replies: Mutex::new(VecDeque::new()),
        }
    }
}
impl<Req: Clone, Rep> Script<Req, Rep> {
    pub(crate) fn push(&self, reply: Result<Rep, ProtocolError>) {
        self.replies.lock().unwrap().push_back(reply);
    }
    pub(crate) fn calls(&self) -> Vec<Req> {
        self.calls.lock().unwrap().clone()
    }
    pub(crate) fn answer(&self, request: Req) -> Result<Rep, ProtocolError> {
        self.calls.lock().unwrap().push(request);
        self.replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Err(unscripted()))
    }
}
pub(crate) fn unscripted() -> ProtocolError {
    ProtocolError::new(
        ErrorCode::UnsupportedOperation,
        "no scripted reply",
        Outcome::NotApplied,
    )
}

/// W's feed: per voter scripted pages.
#[derive(Default)]
pub(crate) struct FakeFeed {
    pub tail: Script<(VoterAddr, TailRequest), TailReply>,
    pub open: Script<(VoterAddr, SnapshotOpen), SnapshotOpenReply>,
    pub page: Script<(VoterAddr, SnapshotPageRequest), SnapshotPageReply>,
    pub close: Script<(VoterAddr, SnapshotClose), ()>,
}
impl FeedClient for FakeFeed {
    fn tail<'a>(&'a self, voter: &'a VoterAddr, req: TailRequest) -> SearchFuture<'a, TailReply> {
        Box::pin(async move { self.tail.answer((voter.clone(), req)) })
    }
    fn snapshot_open<'a>(
        &'a self,
        voter: &'a VoterAddr,
        req: SnapshotOpen,
    ) -> SearchFuture<'a, SnapshotOpenReply> {
        Box::pin(async move { self.open.answer((voter.clone(), req)) })
    }
    fn snapshot_page<'a>(
        &'a self,
        voter: &'a VoterAddr,
        req: SnapshotPageRequest,
    ) -> SearchFuture<'a, SnapshotPageReply> {
        Box::pin(async move { self.page.answer((voter.clone(), req)) })
    }
    fn snapshot_close<'a>(
        &'a self,
        voter: &'a VoterAddr,
        req: SnapshotClose,
    ) -> SearchFuture<'a, ()> {
        Box::pin(async move { self.close.answer((voter.clone(), req)) })
    }
}

/// W's leaders: barrier answers and floors.
#[derive(Default)]
pub(crate) struct FakeLeader {
    pub barrier: Script<(VoterAddr, CallId, BarrierRequest), BarrierReply>,
    pub close: Script<(VoterAddr, CloseInterest), CloseInterestReply>,
}
impl LeaderClient for FakeLeader {
    fn barrier<'a>(
        &'a self,
        node: &'a VoterAddr,
        call: CallId,
        req: BarrierRequest,
    ) -> SearchFuture<'a, BarrierReply> {
        Box::pin(async move { self.barrier.answer((node.clone(), call, req)) })
    }
    fn close_interest<'a>(
        &'a self,
        node: &'a VoterAddr,
        req: CloseInterest,
    ) -> SearchFuture<'a, CloseInterestReply> {
        Box::pin(async move { self.close.answer((node.clone(), req)) })
    }
}

/// I's search workers.
#[derive(Default)]
pub(crate) struct FakeWorker {
    pub query: Script<(SearchEndpoint, QueryRequest, Bytes), (QueryReply, Bytes)>,
    pub stats: Script<(SearchEndpoint, StatsRequest), StatsReply>,
    pub floors: Script<(SearchEndpoint, FloorsRequest), FloorsReply>,
}
impl WorkerClient for FakeWorker {
    fn query<'a>(
        &'a self,
        worker: &'a SearchEndpoint,
        req: QueryRequest,
        vectors: Bytes,
    ) -> SearchFuture<'a, (QueryReply, Bytes)> {
        Box::pin(async move { self.query.answer((worker.clone(), req, vectors)) })
    }
    fn stats<'a>(
        &'a self,
        worker: &'a SearchEndpoint,
        req: StatsRequest,
    ) -> SearchFuture<'a, StatsReply> {
        Box::pin(async move { self.stats.answer((worker.clone(), req)) })
    }
    fn floors<'a>(
        &'a self,
        worker: &'a SearchEndpoint,
        req: FloorsRequest,
    ) -> SearchFuture<'a, FloorsReply> {
        Box::pin(async move { self.floors.answer((worker.clone(), req)) })
    }
}

/// Control as W, I and K see it.
#[derive(Default)]
pub(crate) struct FakeControl {
    pub assignment: Script<Uuid, Option<SearchAssignment>>,
    pub members: Script<(), Vec<SearchMember>>,
    pub models: Script<(), Vec<EmbedModel>>,
    pub report: Script<SearchReport, ()>,
    pub routes: Script<RoutesRequest, RoutesReply>,
}
impl SearchControl for FakeControl {
    fn assignment<'a>(&'a self, table_id: Uuid) -> SearchFuture<'a, Option<SearchAssignment>> {
        Box::pin(async move { self.assignment.answer(table_id) })
    }
    fn members<'a>(&'a self) -> SearchFuture<'a, Vec<SearchMember>> {
        Box::pin(async move { self.members.answer(()) })
    }
    fn models<'a>(&'a self) -> SearchFuture<'a, Vec<EmbedModel>> {
        Box::pin(async move { self.models.answer(()) })
    }
    fn report<'a>(&'a self, report: SearchReport) -> SearchFuture<'a, ()> {
        Box::pin(async move { self.report.answer(report) })
    }
    fn routes<'a>(&'a self, req: RoutesRequest) -> SearchFuture<'a, RoutesReply> {
        Box::pin(async move { self.routes.answer(req) })
    }
}

/// K's embedder's leaders.
#[derive(Default)]
pub(crate) struct FakeEmbedLeader {
    pub lease: Script<(VoterAddr, LeaseRequest), LeaseReply>,
    pub set: Script<(VoterAddr, SetRequest, Bytes), SetReply>,
    pub fail: Script<(VoterAddr, FailRequest), FailReply>,
}
impl EmbedLeaderClient for FakeEmbedLeader {
    fn lease<'a>(&'a self, node: &'a VoterAddr, req: LeaseRequest) -> SearchFuture<'a, LeaseReply> {
        Box::pin(async move { self.lease.answer((node.clone(), req)) })
    }
    fn set<'a>(
        &'a self,
        node: &'a VoterAddr,
        req: SetRequest,
        vector: Bytes,
    ) -> SearchFuture<'a, SetReply> {
        Box::pin(async move { self.set.answer((node.clone(), req, vector)) })
    }
    fn fail<'a>(&'a self, node: &'a VoterAddr, req: FailRequest) -> SearchFuture<'a, FailReply> {
        Box::pin(async move { self.fail.answer((node.clone(), req)) })
    }
}

/// K's leader handler without the executor extension.
#[derive(Default)]
pub(crate) struct FakeSetVectorProposer {
    pub set: Script<SetVectorCall, SearchReply>,
}
impl SetVectorProposer for FakeSetVectorProposer {
    fn set_vector<'a>(
        &'a self,
        _handle: &'a ParticipantHandle,
        req: SetVectorCall,
    ) -> SearchFuture<'a, SearchReply> {
        Box::pin(async move { self.set.answer(req) })
    }
}

/// K's provider, in process. K replaces it with a local HTTP server (late,
/// twice, never, 500, 429, oversize, NaN, wrong dimension, redirect).
#[derive(Default)]
pub(crate) struct FakeProvider {
    pub embed: Script<(String, Vec<String>), Vec<Vec<f32>>>,
}
impl EmbeddingProvider for FakeProvider {
    fn embed<'a>(
        &'a self,
        model: &'a EmbedModel,
        texts: &'a [&'a str],
    ) -> SearchFuture<'a, Vec<Vec<f32>>> {
        Box::pin(async move {
            let texts = texts.iter().map(|text| text.to_string()).collect();
            self.embed.answer((model.name.as_str().to_owned(), texts))
        })
    }
}

/// I's query-string embedding.
#[derive(Default)]
pub(crate) struct FakeQueryEmbedder {
    pub embed: Script<(Uuid, ColumnId, String, String), Arc<[f32]>>,
}
impl QueryEmbedder for FakeQueryEmbedder {
    fn embed_query<'a>(
        &'a self,
        table_id: Uuid,
        column: ColumnId,
        model: &'a str,
        text: &'a str,
    ) -> SearchFuture<'a, Arc<[f32]>> {
        Box::pin(async move {
            self.embed
                .answer((table_id, column, model.to_owned(), text.to_owned()))
        })
    }
}

#[tokio::test]
async fn vs00_fakes_answer_their_script_then_unsupported() {
    let control = FakeControl::default();
    control.assignment.push(Ok(None));
    let table = Uuid::from_u128(3);
    assert_eq!(control.assignment(table).await.unwrap(), None);
    assert_eq!(
        control.assignment(table).await.unwrap_err().code,
        ErrorCode::UnsupportedOperation
    );
    assert_eq!(control.assignment.calls(), vec![table, table]);
    let embedder = FakeQueryEmbedder::default();
    embedder.embed.push(Ok(Arc::from(vec![1.0f32, 0.0])));
    let vector = embedder.embed_query(table, 1, "m", "hello").await.unwrap();
    assert_eq!(&vector[..], &[1.0, 0.0]);
    assert_eq!(
        embedder.embed.calls(),
        vec![(table, 1, "m".to_owned(), "hello".to_owned())]
    );
}
