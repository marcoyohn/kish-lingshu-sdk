//! SQLx journal adapters for direct reliable Event Dispatch publication.
//!
//! This crate stores producer publication intent until Kish Lingshu returns a
//! custody receipt. It is independent from application Eventing/outbox ledgers.

#![deny(unreachable_pub)]

mod record;

#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "sqlite")]
mod sqlite;

#[cfg(feature = "mysql")]
pub use mysql::MySqlEventPublicationJournal;
#[cfg(feature = "sqlite")]
pub use sqlite::SqliteEventPublicationJournal;
