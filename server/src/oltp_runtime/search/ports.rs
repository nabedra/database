//! In-process interfaces between the search tasks (contract-w2 2.8). Each port
//! has one production implementation, owned by the task named on it, and one
//! fake in `testkit.rs`, so every task tests against fakes of the others.
//!
//! Futures are boxed. Errors are `ProtocolError`s with the outcome the caller
//! reports: a port never turns a sent write into `not_applied`.
// The owners wire the ports; until then only the fakes implement them.
#![allow(dead_code)]
use crate::{
    oltp::mvcc::{
        store::{ParticipantHandle, search::SearchReply},
        vector::VectorValue,
    },
    oltp_replication::{
        search_rpc::{
            BarrierReply, BarrierRequest, CallId, CloseInterest, CloseInterestReply, EmbedModel,
            FailReply, FailRequest, FloorsReply, FloorsRequest, LeaseReply, LeaseRequest,
            QueryReply, QueryRequest, RoutesReply, RoutesRequest, SearchAssignment, SearchMember,
            SearchReport, SetReply, SetRequest, SnapshotClose, SnapshotOpen, SnapshotOpenReply,
            SnapshotPageReply, SnapshotPageRequest, StatsReply, StatsRequest, TailReply,
            TailRequest,
        },
        types::{ColumnId, CommitTs, GroupIdentity, Text, WorkerEnrollment},
    },
    protocol::{Key, ProtocolError},
};
use bytes::Bytes;
use std::{future::Future, pin::Pin, sync::Arc};
use uuid::Uuid;

pub(crate) type SearchFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ProtocolError>> + Send + 'a>>;

/// One voter of a Data or Catalog group, as a search process calls it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VoterAddr {
    pub group: GroupIdentity,
    pub node_id: u64,
    pub worker: WorkerEnrollment,
}
/// One search worker, as a gateway calls it (from Control's `SearchMember`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SearchEndpoint {
    pub worker_id: Uuid,
    pub enrollment: WorkerEnrollment,
    pub zone: Text<64>,
}

/// W uses; E's RPC client implements; `testkit::FakeFeed`.
pub(crate) trait FeedClient: Send + Sync {
    fn tail<'a>(&'a self, voter: &'a VoterAddr, req: TailRequest) -> SearchFuture<'a, TailReply>;
    fn snapshot_open<'a>(
        &'a self,
        voter: &'a VoterAddr,
        req: SnapshotOpen,
    ) -> SearchFuture<'a, SnapshotOpenReply>;
    fn snapshot_page<'a>(
        &'a self,
        voter: &'a VoterAddr,
        req: SnapshotPageRequest,
    ) -> SearchFuture<'a, SnapshotPageReply>;
    fn snapshot_close<'a>(
        &'a self,
        voter: &'a VoterAddr,
        req: SnapshotClose,
    ) -> SearchFuture<'a, ()>;
}
/// W uses; F's RPC client implements; `testkit::FakeLeader`.
pub(crate) trait LeaderClient: Send + Sync {
    fn barrier<'a>(
        &'a self,
        node: &'a VoterAddr,
        call: CallId,
        req: BarrierRequest,
    ) -> SearchFuture<'a, BarrierReply>;
    fn close_interest<'a>(
        &'a self,
        node: &'a VoterAddr,
        req: CloseInterest,
    ) -> SearchFuture<'a, CloseInterestReply>;
}
/// I uses; W's server answers; `testkit::FakeWorker`. `vectors` is section 1.
pub(crate) trait WorkerClient: Send + Sync {
    fn query<'a>(
        &'a self,
        worker: &'a SearchEndpoint,
        req: QueryRequest,
        vectors: Bytes,
    ) -> SearchFuture<'a, (QueryReply, Bytes)>;
    fn stats<'a>(
        &'a self,
        worker: &'a SearchEndpoint,
        req: StatsRequest,
    ) -> SearchFuture<'a, StatsReply>;
    fn floors<'a>(
        &'a self,
        worker: &'a SearchEndpoint,
        req: FloorsRequest,
    ) -> SearchFuture<'a, FloorsReply>;
}
/// W, I, K use; J implements; `testkit::FakeControl`.
pub(crate) trait SearchControl: Send + Sync {
    fn assignment<'a>(&'a self, table_id: Uuid) -> SearchFuture<'a, Option<SearchAssignment>>;
    fn members<'a>(&'a self) -> SearchFuture<'a, Vec<SearchMember>>;
    fn models<'a>(&'a self) -> SearchFuture<'a, Vec<EmbedModel>>;
    fn report<'a>(&'a self, report: SearchReport) -> SearchFuture<'a, ()>;
    fn routes<'a>(&'a self, req: RoutesRequest) -> SearchFuture<'a, RoutesReply>;
}
/// K's embedder uses; K's RPC client implements; `testkit::FakeEmbedLeader`.
pub(crate) trait EmbedLeaderClient: Send + Sync {
    fn lease<'a>(&'a self, node: &'a VoterAddr, req: LeaseRequest) -> SearchFuture<'a, LeaseReply>;
    fn set<'a>(
        &'a self,
        node: &'a VoterAddr,
        req: SetRequest,
        vector: Bytes,
    ) -> SearchFuture<'a, SetReply>;
    fn fail<'a>(&'a self, node: &'a VoterAddr, req: FailRequest) -> SearchFuture<'a, FailReply>;
}
/// K's leader handler uses; the executor extension (`direct/search_commit.rs`,
/// through fx-oltp-direct) implements; `testkit::FakeSetVectorProposer`.
pub(crate) trait SetVectorProposer: Send + Sync {
    fn set_vector<'a>(
        &'a self,
        handle: &'a ParticipantHandle,
        req: SetVectorCall,
    ) -> SearchFuture<'a, SearchReply>;
}
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SetVectorCall {
    pub table_id: Uuid,
    pub primary_key: Key,
    pub column: ColumnId,
    pub source_version: CommitTs,
    pub value: VectorValue,
}
/// K; `testkit::FakeProvider` (K turns it into a local HTTP server).
pub(crate) trait EmbeddingProvider: Send + Sync {
    fn embed<'a>(
        &'a self,
        model: &'a EmbedModel,
        texts: &'a [&'a str],
    ) -> SearchFuture<'a, Vec<Vec<f32>>>;
}
/// I uses; K implements (`query_embed.rs`); `testkit::FakeQueryEmbedder`.
pub(crate) trait QueryEmbedder: Send + Sync {
    fn embed_query<'a>(
        &'a self,
        table_id: Uuid,
        column: ColumnId,
        model: &'a str,
        text: &'a str,
    ) -> SearchFuture<'a, Arc<[f32]>>;
}
