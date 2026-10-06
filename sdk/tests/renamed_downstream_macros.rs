#![cfg(feature = "event-consumer")]

use std::{fs, process::Command};

#[test]
fn declaration_macros_link_and_collect_when_the_sdk_dependency_is_renamed() {
    let fixture = tempfile::tempdir().unwrap();
    let sdk_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = format!(
        r#"[package]
name = "renamed-sdk-consumer"
version = "0.1.0"
edition = "2021"

[workspace]

[dependencies]
lingshu = {{ package = "kish-lingshu-sdk", path = {sdk_path:?}, default-features = false, features = ["event-consumer"] }}
schemars = "1.2"
serde = {{ version = "1", features = ["derive"] }}
"#,
        sdk_path = sdk_path
    );
    fs::write(fixture.path().join("Cargo.toml"), manifest).unwrap();
    fs::create_dir(fixture.path().join("src")).unwrap();
    fs::write(
        fixture.path().join("src/main.rs"),
        r#"
use lingshu::event_dispatch::{ConsumerError, EventContext, EventPayload};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, lingshu::EventPayload, JsonSchema, Serialize)]
#[event(
    key = "fixture.created",
    topic = "fixture",
    event_type = "fixture.created",
    schema_version = "1",
    topic_name = "Fixture"
)]
struct Created { value: u64 }

struct Handlers;

#[lingshu::event_dispatch(group = "fixture-workers")]
impl Handlers {
    #[event_consumer(key = "fixture.on-created", event = Created)]
    async fn created(
        &self,
        _context: EventContext,
        event: Created,
    ) -> Result<u64, ConsumerError> {
        Ok(event.value)
    }
}

#[lingshu::event_job(
    key = "fixture.daily",
    event = Created,
    once = "2030-01-01T00:00:00Z"
)]
fn daily() -> Created { Created { value: 1 } }

fn main() {
    let _ = std::any::TypeId::of::<Handlers>();
    let catalog = lingshu::event_dispatch::SourceCatalog::collect().unwrap();
    assert_eq!(catalog.events().len(), 1);
    assert_eq!(catalog.consumers().len(), 1);
    assert_eq!(catalog.jobs().len(), 1);
    let manifest = catalog
        .export_json(lingshu::event_dispatch::ProducerMetadata {
            package_name: "renamed-sdk-consumer".into(),
            package_version: "0.1.0".into(),
        })
        .unwrap();
    assert!(!manifest.is_empty());
}
"#,
    )
    .unwrap();

    let target_dir = sdk_path.join("../../target/renamed-downstream-macros");
    for release in [false, true] {
        let mut command = Command::new(env!("CARGO"));
        command.args(["run", "--quiet", "--manifest-path"]);
        command.arg(fixture.path().join("Cargo.toml"));
        if release {
            command.arg("--release");
        }
        let output = command
            .env("CARGO_TARGET_DIR", &target_dir)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "renamed downstream crate failed to link and run in {} mode:\n{}",
            if release { "release" } else { "debug" },
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
