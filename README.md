# Kish Lingshu Rust SDK

Rust SDK and protocol-neutral contracts for integrating applications with Kish Lingshu.
This repository is generated from a private source repository. Make SDK changes in
that source; this repository is the distribution endpoint.

## Using the SDK

Pin a reviewed commit from this repository in your Cargo workspace:

```toml
[workspace.dependencies]
kish-lingshu-sdk = { git = "https://github.com/marcoyohn/kish-lingshu-sdk.git", rev = "<public SDK commit>", default-features = false }
kish-lingshu-foundation-contract = { git = "https://github.com/marcoyohn/kish-lingshu-sdk.git", rev = "<same public SDK commit>" }
```

Enable the features required by each consuming crate, for example
`http-client`, `event-consumer-http`, or `user-task-completion-http`.
Use the same Git URL and revision for all SDK and Contract packages so Rust types
come from one source. Commit the consuming application's `Cargo.lock` and build
with `cargo build --locked`. The public SDK commit is different from the private
source commit recorded in `sdk-source.json`.

## Packages

| Package | Purpose |
| --- | --- |
| `kish-lingshu-sdk` | Client, event publication/consumption, workflow and user task APIs |
| `kish-lingshu-sdk-macros` | SDK declaration macros |
| `kish-lingshu-foundation-contract` | Shared contract values |
| `kish-lingshu-event-dispatch-contract` | Event Dispatch contracts |
| `kish-lingshu-runtime-contract` | Workflow and User Task contracts |
| `kish-lingshu-event-publication-sqlx` | Optional SQLx producer journal and its MySQL/SQLite migrations |

Package license declarations are preserved in each `Cargo.toml`.

## Build and test

```bash
cargo check --locked --workspace --all-targets --all-features
cargo test --locked --workspace --all-features
```

All source dependencies are included here. Building the SDK does not require
access to the private Lingshu server repository or private Git dependencies.

## Releases

The source repository publishes snapshots through a manual workflow and releases
through `sdk-v<version>` tags. Release tags appear here as `v<version>` and are
immutable. `main` contains the latest published SDK snapshot. Only maintainers
and the publishing automation can write to this repository.

Local Workspace/Sandbox execution, permission journals and CLI presentation belong
to the source product and are not part of the public SDK export.
