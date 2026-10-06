//! Outbound-only Provider, Call and directed Consumer on one managed connection.
//! See the public README for governance, environment and terminal-state handling.
use kish_lingshu_foundation_contract::ServiceInstanceRegistration;
use kish_lingshu_runtime_contract::provider::ProviderCatalog;
use kish_lingshu_sdk::{
    event_dispatch::{
        ConsumerError, ConsumerRegistry, EventContext, ProducerMetadata, SourceCatalog,
    },
    service_channel::{
        ChannelCertificateRotationConfig, ChannelSessionConfig, ChannelSupervisorStatus,
    },
    services::{self, ServiceContext, ServiceError, ServiceRegistryBuilder},
    ServiceConnection, ServiceCredential, ServiceExecutionBudget,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{error::Error, sync::Arc};

type ExampleResult<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const GROUP: &str = "native-example-workers";
const CAPACITY: u32 = 4;

#[derive(Deserialize, Serialize, JsonSchema)]
struct Echo {
    value: String,
}
struct Application;
#[kish_lingshu_sdk::lingshu_service(key = "native-example")]
impl Application {
    #[service_call(operation = "echo", version = "1", action = "example.echo", idempotent = true, modes = ["sync", "async"])]
    async fn echo(
        &self,
        _context: ServiceContext,
        input: Echo,
    ) -> std::result::Result<String, ServiceError> {
        // Real writes must atomically bind context.idempotency_key() to their effects.
        Ok(input.value)
    }
}
#[derive(Deserialize, Serialize, JsonSchema, kish_lingshu_sdk::event_dispatch::EventPayload)]
#[event(
    key = "native.example.changed",
    topic = "native.example",
    event_type = "changed",
    schema_version = "1",
    topic_name = "Native Example"
)]
struct Changed {
    value: String,
}
#[kish_lingshu_sdk::event_dispatch(maximum_concurrency = 4)]
impl Application {
    #[event_consumer(consumer_group = "native-example-workers", event = Changed)]
    async fn changed(
        &self,
        _context: EventContext,
        _event: Changed,
    ) -> std::result::Result<(), ConsumerError> {
        // One bounded attempt. Return retryable/permanent; Dispatch owns scheduling.
        // Persist consumer idempotency atomically with any business effect.
        Ok(())
    }
}
fn env(key: &str) -> ExampleResult<String> {
    Ok(std::env::var(key)?)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> ExampleResult<()> {
    let app = env("LINGSHU_APPLICATION_ID")?;
    let catalog = ProviderCatalog {
        format_version: 1,
        application_id: app.clone(),
        provider_key: "native-example".into(),
        release: "1".into(),
        services: Some(services::export_manifest(&app)?),
        events: Some(SourceCatalog::collect()?.manifest(ProducerMetadata {
            package_name: "native-example".into(),
            package_version: "1".into(),
        })?),
        workflows: vec![],
    };
    if std::env::args().any(|arg| arg == "--print-catalog") {
        println!("{}", serde_json::to_string_pretty(&catalog)?);
        return Ok(());
    }
    let catalog_only = std::env::args().any(|arg| arg == "--catalog-only");
    let application = Arc::new(Application);
    let mut services = ServiceRegistryBuilder::new(catalog.services.clone().unwrap())?;
    application.clone().bind_lingshu_services(&mut services)?;
    let registry = Arc::new(services.build()?);
    let mut consumers = ConsumerRegistry::builder(&app)?;
    consumers.bind(application)?;
    let consumers = Arc::new(consumers.build()?);
    let connection = ServiceConnection::connect(
        &env("LINGSHU_URL")?,
        ServiceCredential::new(&app, env("LINGSHU_API_KEY")?)?,
    )
    .await?;
    let deployment = std::env::var("LINGSHU_EXPECTED_DEPLOYMENT")
        .ok()
        .map(kish_lingshu_foundation_contract::service_transport::RouteIdentity::new)
        .transpose()?;
    let identity = connection
        .bootstrap_channel(
            ServiceInstanceRegistration {
                // A stable replica slot, not a PID. A new explicit process start has a new incarnation.
                instance_id: env("LINGSHU_INSTANCE_ID")?,
                incarnation_id: uuid::Uuid::new_v4().to_string(),
                generation: None,
            },
            deployment,
        )
        .await?;
    let mut sessions = identity
        .open_sessions(ChannelSessionConfig::default())
        .await?;
    let roles: ExampleResult<_> = async {
        let provider = sessions.register_provider_role(&catalog).await?;
        let mut roles = vec![provider];
        if !catalog_only {
            // These require an explicitly reviewed/published catalog and Consumer group.
            let budget = ServiceExecutionBudget::new(CAPACITY)?;
            let mut call = sessions
                .register_service_role("native-example-call", CAPACITY, &registry)
                .await?;
            sessions.enable_async_calls(&mut call, registry, budget.clone())?;
            let mut consumer = sessions
                .register_consumer_role(GROUP, "native-example-consumer", CAPACITY)
                .await?;
            sessions.enable_sync_consumers(&mut consumer, consumers, budget)?;
            roles.extend([call, consumer]);
        }
        Ok(roles)
    }
    .await;
    let roles = match roles {
        Ok(roles) => roles,
        Err(error) => {
            sessions.close().await?;
            return Err(error);
        }
    };
    let mut managed =
        sessions.manage_roles_with_rotation(roles, ChannelCertificateRotationConfig::default())?;
    let mut status = managed.subscribe_status();
    let mut role_status = managed.subscribe_roles();
    let mut rotation = managed.subscribe_rotation();
    println!(
        "Outbound channel established; role/route evidence does not imply execution readiness."
    );
    let stopped = async {
        loop {
            tokio::select! {
                result = tokio::signal::ctrl_c() => { result?; break; }
                result = status.changed() => {
                    if result.is_err() || !matches!(*status.borrow(), ChannelSupervisorStatus::Active { .. }) { break; }
                }
                result = role_status.changed() => {
                    if result.is_err() { break; }
                    let roles = role_status.borrow();
                    println!("Roles authorized: {}, routes confirmed: {}", roles.iter().filter(|role| role.authorized()).count(), roles.iter().filter(|role| role.route_confirmed()).count());
                }
                result = rotation.changed() => {
                    if result.is_err() { break; }
                    println!("Certificate rotation: {:?}", rotation.borrow().phase);
                }
            }
        }
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    }.await;
    managed.close().await?;
    stopped
}
