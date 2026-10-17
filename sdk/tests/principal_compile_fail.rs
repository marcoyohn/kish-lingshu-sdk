#[test]
fn principal_and_builder_capabilities_are_compile_time_bounded() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/local_task_api_removed.rs");
    cases.compile_fail("tests/ui/user_cannot_publish_events.rs");
}

#[cfg(feature = "http-client")]
#[test]
fn builder_without_principal_cannot_connect() {
    use std::{fs, path::PathBuf, process::Command};

    let fixture = tempfile::tempdir().unwrap();
    let sdk_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    fs::write(
        fixture.path().join("Cargo.toml"),
        format!(
            r#"[package]
name = "principal-boundary-fixture"
version = "0.1.0"
edition = "2021"
[workspace]
[dependencies]
kish-lingshu-sdk = {{ path = {sdk_path:?}, default-features = false, features = ["http-client"] }}
"#
        ),
    )
    .unwrap();
    fs::create_dir(fixture.path().join("src")).unwrap();
    fs::write(
        fixture.path().join("src/main.rs"),
        include_str!("ui/builder_requires_principal.rs"),
    )
    .unwrap();
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| sdk_path.join("../target"))
        .join("principal-boundary");
    let output = Command::new(env!("CARGO"))
        .args(["check", "--offline", "--message-format=json", "--manifest-path"])
        .arg(fixture.path().join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target)
        .output()
        .unwrap();
    assert!(!output.status.success(), "unauthenticated builder compiled");
    // rustc may qualify NoPrincipal differently in the source and exported
    // workspaces. Check the actual capability error, not its presentation.
    let rejected_for_principal = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .any(|event| {
            let diagnostic = &event["message"];
            event["reason"] == "compiler-message"
                && diagnostic["level"] == "error"
                && diagnostic["code"]["code"] == "E0599"
                && diagnostic["message"].as_str().is_some_and(|message| {
                    message.contains("connect") && message.contains("trait bounds")
                })
                && diagnostic["rendered"].as_str().is_some_and(|rendered| {
                    rendered.contains("NoPrincipal: Principal")
                })
        });
    assert!(
        rejected_for_principal,
        "expected missing Principal bound on connect, got:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
