//! Product-facing Rust SDK for Kish Lingshu application integrations.
//!
//! Public modules follow the product domains. Transport and in-process
//! placement are private binding details shared by those modules.

#![deny(unreachable_pub)]

extern crate self as kish_lingshu_sdk;

mod auth;
mod binding;
mod client;
mod config;
mod error;
mod request;
#[cfg(feature = "service-auth")]
mod service_budget;

pub mod assets;
pub mod event_dispatch;
#[cfg(feature = "service-auth")]
pub mod service_auth;
#[cfg(feature = "service-manifest")]
pub mod services;
pub mod user_task;
pub mod workflow;
pub mod workspaces;
#[cfg(feature = "service-auth")]
pub use service_auth::{ServiceAuthError, ServiceConnection};
#[cfg(feature = "service-auth")]
pub use service_budget::ServiceExecutionBudget;

pub use auth::{
    AuthenticatedUser, CallbackCredential, ConsumerGroupRegistrationCredential, CredentialError,
    ServiceCredential, UserCredential,
};
pub use client::{Client, ClientBuilder, NoPrincipal, Principal, ServicePrincipal, UserPrincipal};
pub use config::{ClientConfig, ClientMetadata};
pub use error::{
    ApplicationFailure, ConfigurationError, ContractViolation, Error, ProtocolDirection,
    ProtocolError, TransportFailure, TransportKind,
};
pub use request::{MutationOptions, RequestOptions};

#[cfg(feature = "event-consumer")]
pub use kish_lingshu_sdk_macros::{event_consumer, event_dispatch};
#[cfg(feature = "event-manifest")]
pub use kish_lingshu_sdk_macros::{event_job, EventPayload};

#[cfg(feature = "service-manifest")]
pub use kish_lingshu_sdk_macros::{
    lingshu_service, service_call, service_event, user_task_completion_handler,
};
