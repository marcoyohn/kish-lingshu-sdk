//! Transport-neutral contracts for invoking and observing published Workflows.
//!
//! This crate deliberately contains no database, HTTP, ACP, or presentation
//! dependencies. Runtime implementations live in higher-level crates.

mod authorization;
mod catalog;
mod client;
mod context;
mod conversation;
mod error;
mod event;
mod facade;
mod identity;
mod ports;
mod problem;
mod request;
mod result_projection;
mod suspension;
pub mod user_context;
mod user_task;
mod user_task_completion;
mod workflow;

#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use authorization::*;
pub use catalog::*;
pub use client::*;
pub use context::*;
pub use conversation::*;
pub use error::*;
pub use event::*;
pub use facade::*;
pub use identity::*;
pub use ports::*;
pub use problem::*;
pub use request::*;
pub use result_projection::*;
pub use suspension::*;
pub use user_task::*;
pub use user_task_completion::*;
pub use workflow::*;
