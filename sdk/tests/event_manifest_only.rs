#![cfg(all(feature = "event-manifest", not(feature = "event-consumer")))]

use kish_lingshu_sdk::{
    event_dispatch::{JobTrigger, ProducerMetadata, SourceCatalog},
    event_job, EventPayload,
};
use schemars::JsonSchema;
use serde::Serialize;

#[derive(EventPayload, JsonSchema, Serialize)]
#[event(
    key = "billing.invoice-due",
    topic = "billing",
    event_type = "invoice.due",
    schema_version = "1",
    description = "An invoice is ready for collection",
    topic_name = "Billing"
)]
struct InvoiceDue {
    invoice_id: u64,
}

#[event_job(
    key = "billing.collect-overdue",
    event = InvoiceDue,
    interval_milliseconds = 86400000,
    anchor_at = "2030-01-01T00:00:00Z",
    overlap = "serialize"
)]
fn collect_overdue() -> InvoiceDue {
    InvoiceDue { invoice_id: 42 }
}

#[test]
fn manifest_feature_exports_event_and_job_without_consumer_runtime() {
    let catalog = SourceCatalog::collect().unwrap();
    assert_eq!(catalog.events().len(), 1);
    assert!(catalog.consumers().is_empty());
    assert_eq!(catalog.jobs().len(), 1);
    assert!(matches!(
        catalog.jobs()[0].trigger,
        JobTrigger::Interval { .. }
    ));

    let manifest = catalog
        .export_json(ProducerMetadata {
            package_name: "billing-application".into(),
            package_version: "1.0.0".into(),
        })
        .unwrap();
    assert!(!manifest.is_empty());
}
