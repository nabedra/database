//! The search worker's table index (owner: W, contract-w2 2.8): one
//! `TableIndex` per assigned table composes the dense, impact and multi-vector
//! indexes of its columns over the ranges it holds, applies feed pages, pins
//! every column in one step and answers `TableQuery`s. Pure: no I/O except
//! through `nabedra-rt` `Fs`, no serde.
//!
//! Skeleton (S): empty until W lands it.
