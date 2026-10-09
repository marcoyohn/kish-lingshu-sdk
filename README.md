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
`http-client`, `event-consumer-http`, or `service-http`.
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

All six packages declare Apache-2.0 in `Cargo.toml`; the reviewed license text is
included at [LICENSE-APACHE](LICENSE-APACHE) and in the SDK package.

## Build and test

```bash
cargo check --locked --workspace --all-targets --all-features
cargo test --locked --workspace --all-features
```

Product source dependencies are included here; shared client transport uses an
immutable commit of the public ZenSS Client SDK/contracts. Building does not require
access to the private Lingshu server repository or private Git dependencies.

## Releases

Maintainers publish snapshots with explicit local Make commands from committed
source. Source `sdk-v<version>` tags select releases; pushes and tags do not publish automatically. Release tags appear here as `v<version>` and are
immutable. `main` contains the latest published SDK snapshot. Only maintainers and the explicit local publishing tool write to this repository.

Local Workspace/Sandbox execution, permission journals and CLI presentation belong
to the source product and are not part of the public SDK export.


### v0.5.0

Native physical transport now delegates to public `zenss-client-sdk` v0.5.2:
managed session pools, TLS/authenticated intranet TCP profiles, local key/CSR
creation, connectivity and bounded cleanup. Existing Service/Event declarations,
Lingshu bootstrap verification, role leases, route readiness, rotation handoff and
Dispatch acknowledgements are unchanged. Lingshu native Host/Build Kit remains
v0.5.0. Product protocol declarations still use official Zenoh APIs.

The dependency is pinned to a full public Git commit. Only ZenSS Client SDK and
contracts are permitted in the exported closure; private Host, Plugin SDK and
patched native Zenoh dependencies remain excluded. Existing default HTTP features
and optional native feature names remain available.

### v0.4.1

Fixes initial native Provider/Call/Consumer registration when the platform clock
is slightly ahead of the client. Registration uses the authenticated reply's
issuance time and the existing 5-second future-clock tolerance, while preserving
the 30-second lease limit, monotonic request-time budget and physical authorization
cap. No API, wire, database or ZenSS Host/Build Kit upgrade is required from
SDK v0.4.0; the matching Host/Build Kit remains v0.5.0.

## Outbound Zenoh application (candidate, opt-in)

The Client SDK uses official crates.io Zenoh **1.10.1**, TCP/TLS only. It requires
no inbound listener, Router, ZenSS daemon or private repository. Product plugins
are built separately using the native ZenSS Plugin SDK and matching build kit.
A connected socket, registered role or recently confirmed route is not business
execution readiness; each invocation still checks current permission and capacity.

| Feature | Purpose |
| --- | --- |
| `service-channel` | HTTPS bootstrap and local CSR key ownership |
| `service-zenoh` | Outbound mTLS Sessions by default, Provider catalog, exact role proof/renewal/rotation |
| `service-plaintext` | Explicit authenticated intranet TCP, official Zenoh RSA possession handshake |
| `service-call-zenoh` | Typed Service Sync/Async bindings, original completion authority |
| `event-consumer-zenoh` | Directed Consumer execution chosen by Lingshu Dispatch |
| `event-publication-zenoh` | PublishEvent on the managed native control channel |
| `service-http`, `event-consumer-http` | Explicit inbound HTTP compatibility adapters |

The reviewed [native example](sdk/examples/native_application.rs) keeps existing
Service/Event annotations and shares one execution budget across Call and Consumer.
Run from this SDK workspace:

```bash
export LINGSHU_APPLICATION_ID=my-app
# Credentials are unnecessary for inspecting the source catalog.
cargo run --locked -p kish-lingshu-sdk --no-default-features \
  --features service-call-zenoh,event-consumer-zenoh,event-publication-zenoh \
  --example native-application -- --print-catalog
```

Set `LINGSHU_URL` to the platform HTTPS base URL, `LINGSHU_API_KEY` via your secret
manager, and `LINGSHU_INSTANCE_ID` to a stable replica slot (not a PID). Optional
`LINGSHU_EXPECTED_DEPLOYMENT` pins deployment identity. Bootstrap discovers the
TLS endpoint/trust from the authenticated platform; do not configure a public
callback, copy an SDK key into the platform, or disable TLS name/CA verification.

1. Start the same command with `--catalog-only`. It advertises the source Provider
   and manages its finite lease/route/certificate. It does not register Call or Consumer.
2. Explicitly preview/import/publish that Provider in platform governance, including
   the `native-example-workers` group. Import is separate from business execution.
3. Stop the catalog-only process, then start without either mode flag. A fresh
   incarnation registers Provider/Call/Consumer on the same stable instance slot.
   Unknown/conflicting registration fails; never clear a generation and retry inside
   the same process to take ownership back automatically.

`enable_async_calls` installs **both** Sync and Async support. Do not call it after
`enable_sync_calls` on the same role: an execution binding is unique. This example
uses one physical lane and an explicit rotation supervisor with a fixed role set;
rotating supervisors currently accept **1–2** lanes so old/new pools fit the shared
four-Session budget. Bare 1–4 Session preparation is not multi-lane business acceptance.

Subscribe to channel, role and rotation status. Authorization/route snapshots expire
at their original deadlines; transient errors do not renew them. On terminal state,
stop accepting work, call and await `close`, and surface cleanup failure. A remote
cleanup failure can remain in role status even after local cleanup succeeds. Do not
re-enroll automatically, replay an unknown Invoke, or fall back to HTTP for the same
attempt. SIGINT in the example joins bounded local cleanup. Applications must also
map their deployment's shutdown signal to that same close operation.

With `http-client`, Workflow `wait()` reserves up to 10% of the original wait
budget (at most 500 ms) for one read-only terminal snapshot if its event stream
stays pending or fails. This can confirm another replica's persisted completion;
it never resumes work, resends Invoke or changes the safe event cursor. Completion
requires matching Workflow/run/root identities, persisted Completed state and a
present output. Explicit JSON null is a result; an omitted output is unavailable.
`wait_to_boundary()` keeps its original event semantics. Workflow control and
observation use the existing product API; native Call and report traffic use Zenoh.

For native publication, obtain a normal service-principal Client, then bind its
Event Dispatch API to this managed channel:

```rust,ignore
let events = client.event_dispatch().with_managed_channel(&managed)?;
let receipt = events.publish(event, MutationOptions::new("business-event-key")?).await?;
```

The receipt confirms center custody, not finished consumption. Dispatch selects
group/member, schedules retries and commits completion; Zenoh is not a broadcast
replacement. Publish after business commit, preserve atomic consumer idempotency,
and use the optional SQLx journal crate for `PERSIST_ON_FAILURE` / `DURABLE`.
Journal ownership is `(application_id, stable publisher_id)`; replicas of a producer
share that ID, independent producers use distinct IDs. Recovery is an externally
triggered bounded pass; the connection supervisor never scans journals or retries
Handlers. A publication timeout must preserve the original idempotency key.

These interfaces are a candidate source delivery. Public tags, production enablement,
CA trust rotation, multiple Router topology and capacity/fault acceptance are separate
release gates; a compiled example does not claim they have passed.

连接状态可通过 `ServiceChannelSessions::subscribe_connectivity()` 或
`ManagedRoleChannel::subscribe_connectivity()` 观察；后者跟随当前证书池。
这只是每秒采样、可能合并的物理状态，不能代替 `ChannelRoleStatus` 的有限
角色授权与路由确认。已观察到数据连接中断后，连接恢复不会直接恢复业务
准入；需平台签名探测。SDK原生重连只使用bootstrap验证的TLS入口集合，
保留原generation，不自动重新注册或重放业务请求。当前同Router多入口恢复
已验证；不同Router上的原通道入口授权仍有交付门禁，不能仅凭连接成功启用。

原生入口在解码前同时限制 Query 数量和字节（包含 key、attachment 和保守
对象预留），不支持 selector parameters。一个物理池的各角色/lane共享
64项/8MiB普通入口额度；BindLane/CancelCall另有8项/512KiB预留，单控制
请求≤32KiB。角色worker连续处理至多四项控制后给已排队业务一轮机会。
分类只决定额度，主体/签名/精确路由和有限call权限仍由正常处理链验证；
过载后的未确认Reply不能被视为“业务未执行”，不会触发Invoke重放。
这里的额度是应用入口预留，不代表整个Router或进程RSS；换证的old/candidate
池分别持有额度，仍受同一逻辑连接四个物理Session上限限制。

## Linux native receive-window prerequisite

The Lingshu native TLS profile requests `transport.link.tls.so_rcvbuf = 1048576`
through official Zenoh configuration; the gated Host applies the same request to
TCP/TLS. This kernel receive buffer is distinct from the 65535-byte native RX
batch pool. Linux normally accounts twice the socket request. Host admission
charges that receive storage within the existing aggregate allocation; it is
not a bound on all TLS, allocator, kernel send or business memory.

Verify `sysctl -n net.core.rmem_max` on **both the Host and Linux SDK machine**.
The deployment prerequisite is at least1048576 bytes. Linux silently clamps
ordinary SO_RCVBUF when this limit is lower; checking configuration alone cannot
prove the requested window was granted. For containers/Kubernetes, configure
and verify the underlying node through its normal provisioning process; this
is not an application-level or portable Pod sysctl.

An operator can provision the required cap with
`sudo sysctl -w net.core.rmem_max=1048576`, retaining a larger existing value.
Persist it using the machine's normal sysctl provisioning only after deployment
review. The SDK and Host never change system limits or use SO_RCVBUFFORCE.
Official configuration and the fixed resource/performance gates must be verified
with the actual deployment cap; diagnostic socket injection is not acceptance.
The accepted Linux fixture uses glibc arena2 and records its node cap separately.
macOS kernel-window behavior has not been accepted by the Linux measurements.

## Explicit intranet plaintext (0.3.0)

For SDK v0.4.x, a matching ZenSS v0.5.0 Host/Build Kit and enabled product
profile are required.
Add `service-plaintext` to the application's native features and call
`bootstrap_channel_with_transport(instance, expected_deployment,
ChannelTransport::IntranetPlaintext)`. The enum is exported from
`kish_lingshu_sdk::service_channel`. `bootstrap_channel` remains mTLS.
For `native-application`, set `LINGSHU_CHANNEL_TRANSPORT=intranet_plaintext`.

Endpoints use `tcp/hostname:7447`; the authenticated HTTPS bootstrap binds a
fresh local RSA transport key to the signing identity. Neither private key is
uploaded. Grants, route readiness and managed rotation stay unchanged. Data and
TCP framing are unencrypted; use only trusted intranet/VPN access. No automatic
fallback, mixed TLS/TCP endpoint pool or inbound application listener is added.
Existing Service/Event annotations stay unchanged. Dispatch still chooses event
consumer groups and owns retry and acknowledgement.
