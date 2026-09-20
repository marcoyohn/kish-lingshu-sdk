#![cfg(feature = "event-consumer")]
use kish_lingshu_sdk::{
    event_dispatch,
    event_dispatch::{ConsumerError, ConsumerRegistry, EventContext, SourceCatalog},
    EventPayload,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
#[derive(Deserialize, Serialize, JsonSchema, EventPayload)]
#[event(
    key = "duplicate.changed",
    topic = "duplicate",
    event_type = "duplicate.changed",
    schema_version = "1",
    topic_name = "Duplicate"
)]
struct Changed {}
struct First;
struct Second;
#[event_dispatch]
impl First {
    #[event_consumer(consumer_group="projection",event=Changed)]
    async fn first(&self, _: EventContext, _: Changed) -> Result<(), ConsumerError> {
        Ok(())
    }
}
#[event_dispatch]
impl Second {
    #[event_consumer(key="legacy-second-key",consumer_group="projection",event=Changed)]
    async fn second(&self, _: EventContext, _: Changed) -> Result<(), ConsumerError> {
        Ok(())
    }
}
#[test]
fn conflicting_handlers_fail_registration_with_both_names_and_full_route() {
    let mut b = ConsumerRegistry::builder("app").unwrap();
    b.bind(Arc::new(First)).unwrap();
    b.bind(Arc::new(Second)).unwrap();
    let error = b.build().err().unwrap().to_string();
    for expected in [
        "duplicate.changed",
        "projection",
        "First::first",
        "Second::second",
    ] {
        assert!(error.contains(expected), "{error}");
    }
    assert!(SourceCatalog::collect().is_err());
}
