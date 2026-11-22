//! Shared assertions for the protocol model tests.
//!
//! Each test file includes this module with `#[path = "../models/common.rs"] mod common;`.
//! Every helper prints the checker's report, so `cargo test -- --nocapture` shows the
//! explored state counts and each counterexample trace.

#![allow(dead_code)]

use nabedra_check::model::{Explored, Model, Outcome, explore};

/// Explores `model` and asserts that the bounded space is exhausted with no violation.
pub fn expect_passed<M: Model>(name: &str, model: &M, max_states: usize) -> Explored {
    let out = explore(model, max_states);
    println!("[{name}] {}", out.report());
    match out {
        Outcome::Passed(e) => e,
        other => panic!("[{name}] expected Passed, got:\n{}", other.report()),
    }
}

/// Explores `model` and asserts a violation of the invariant whose name is `invariant`
/// (the text before the first ':'). Returns the trace length in steps.
pub fn expect_violated<M: Model>(name: &str, model: &M, max_states: usize, invariant: &str) -> usize {
    let out = explore(model, max_states);
    println!("[{name}] {}", out.report());
    match &out {
        Outcome::Violated { invariant: got, trace } => {
            let got_name = got.split(':').next().unwrap_or("").trim();
            assert_eq!(
                got_name, invariant,
                "[{name}] wrong invariant violated:\n{}",
                out.report()
            );
            trace.len() - 1
        }
        other => panic!(
            "[{name}] expected a violation of {invariant}, got:\n{}",
            other.report()
        ),
    }
}
