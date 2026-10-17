//! Transport-neutral contracts for invoking and observing published Workflows.
//!
//! This crate deliberately contains no database, HTTP, ACP, or presentation
//! dependencies. Runtime implementations live in higher-level crates.

pub mod admission;
mod authorization;
mod catalog;
mod client;
mod context;
mod conversation;
mod error;
mod event;
mod facade;
pub mod human_task;
mod identity;
mod pagination;
pub mod participant_configuration;
mod ports;
mod problem;
mod request;
mod result_projection;
pub mod retry;
pub mod service;
mod suspension;
pub mod user_context;
mod workflow;
pub mod workflow_catalog;

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
pub use human_task::*;
pub use identity::*;
pub use pagination::*;
pub use ports::*;
pub use problem::*;
pub use request::*;
pub use result_projection::*;
pub use suspension::*;
pub use workflow::*;

pub mod provider;

pub mod llm_routing;
