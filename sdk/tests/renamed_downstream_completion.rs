#![cfg(feature = "user-task-completion")]

use std::{fs, process::Command};

#[test]
fn completion_macros_work_when_the_sdk_dependency_is_renamed() {
    let fixture = tempfile::tempdir().unwrap();
    let sdk_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = format!(
        r#"[package]
name = "renamed-completion-sdk"
version = "0.1.0"
edition = "2021"

[workspace]

[dependencies]
lingshu = {{ package = "kish-lingshu-sdk", path = {sdk_path:?}, default-features = false, features = ["user-task-completion"] }}
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
use std::sync::Arc;

use lingshu::user_task::completion::{
    CompletionContext, CompletionProducerMetadata, CompletionRegistry, CompletionResult,
    CompletionSourceCatalog,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, JsonSchema)]
struct Submission { approved: bool }

#[derive(JsonSchema, Serialize)]
struct Output { applied: bool }

struct Handlers;

#[lingshu::user_task_handlers]
impl Handlers {
    #[completion_handler(task_type = "fixture.approval.v1")]
    async fn complete(
        &self,
        _context: CompletionContext,
        submission: Submission,
    ) -> CompletionResult<Output> {
        Ok(Output { applied: submission.approved })
    }
}

fn main() {
    let mut builder = CompletionRegistry::builder("fixture-app").unwrap();
    builder.bind(Arc::new(Handlers)).unwrap();
    let registry = builder.build().unwrap();
    assert_eq!(registry.application_id(), "fixture-app");

    let contract = CompletionSourceCatalog::collect()
        .unwrap()
        .contract(CompletionProducerMetadata {
            package_name: "renamed-completion-sdk".into(),
            package_version: "0.1.0".into(),
        })
        .unwrap();
    assert_eq!(contract.handlers[0].task_type, "fixture.approval.v1");
}
"#,
    )
    .unwrap();

    let target_dir = sdk_path.join("../../target/renamed-downstream-completion");
    let output = Command::new(env!("CARGO"))
        .args(["run", "--quiet", "--offline", "--manifest-path"])
        .arg(fixture.path().join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target_dir)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "renamed downstream completion crate failed to link and run:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
