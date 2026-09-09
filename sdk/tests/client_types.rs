use std::time::Duration;

use kish_lingshu_runtime_contract::{
    test_support::ContractProductRuntimeFixture, InvocationSource, PrincipalKind,
    TrustedContextFactory,
};
use kish_lingshu_sdk::{
    Client, ClientBuilder, ClientConfig, Error, ServiceCredential, ServicePrincipal,
    UserCredential, UserPrincipal,
};

fn context_factory(principal: PrincipalKind) -> TrustedContextFactory {
    TrustedContextFactory::new(
        match principal {
            PrincipalKind::Service => "contract-service",
            PrincipalKind::User => "contract-actor",
            PrincipalKind::Internal => unreachable!(),
        },
        principal,
        Some("contract-app".to_string()),
        InvocationSource::EmbeddedSdk,
    )
    .unwrap()
}

fn assert_service_capabilities(client: &Client<ServicePrincipal>) {
    let _ = client.workflows();
    let _ = client.event_dispatch();
    let _ = client.user_tasks();
}

fn assert_user_capabilities(client: &Client<UserPrincipal>) {
    let _ = client.workflows();
    let _ = client.user_tasks();
}

#[test]
fn principal_credentials_construct_distinct_in_process_clients() {
    let service_fixture = ContractProductRuntimeFixture::default();
    let service = ClientBuilder::new(ClientConfig::in_process())
        .service_credential(ServiceCredential::new("contract-app", "service-secret").unwrap())
        .bind_runtime(
            service_fixture.facade,
            context_factory(PrincipalKind::Service),
        )
        .unwrap();
    assert_service_capabilities(&service);

    let user_fixture = ContractProductRuntimeFixture::default();
    let user =
        ClientBuilder::new(ClientConfig::in_process().with_selected_application("contract-app"))
            .user_credential(UserCredential::new("user-secret").unwrap())
            .bind_runtime(user_fixture.facade, context_factory(PrincipalKind::User))
            .unwrap();
    assert_user_capabilities(&user);
}

#[test]
fn configuration_is_validated_before_runtime_binding() {
    let fixture = ContractProductRuntimeFixture::default();
    let error = ClientBuilder::new(
        ClientConfig::new("ftp://example.test")
            .with_timeout(Duration::ZERO)
            .with_retry_limit(100),
    )
    .service_credential(ServiceCredential::new("contract-app", "service-secret").unwrap())
    .bind_runtime(fixture.facade, context_factory(PrincipalKind::Service))
    .unwrap_err();
    assert!(matches!(error, Error::Configuration(_)));
}

#[test]
fn service_principal_cannot_select_an_application_override() {
    let fixture = ContractProductRuntimeFixture::default();
    let error =
        ClientBuilder::new(ClientConfig::in_process().with_selected_application("contract-app"))
            .service_credential(ServiceCredential::new("contract-app", "service-secret").unwrap())
            .bind_runtime(fixture.facade, context_factory(PrincipalKind::Service))
            .unwrap_err();
    assert!(matches!(error, Error::Configuration(_)));
}

#[test]
fn client_and_builder_debug_output_never_contains_credentials() {
    let builder = ClientBuilder::new(ClientConfig::in_process())
        .service_credential(ServiceCredential::new("contract-app", "service-secret").unwrap());
    let rendered = format!("{builder:?}");
    assert!(rendered.contains("[REDACTED]"));
    assert!(!rendered.contains("service-secret"));
}

#[test]
fn runtime_binding_rejects_a_factory_from_another_principal_or_application() {
    let fixture = ContractProductRuntimeFixture::default();
    let principal_error = ClientBuilder::new(ClientConfig::in_process())
        .service_credential(ServiceCredential::new("contract-app", "service-secret").unwrap())
        .bind_runtime(fixture.facade, context_factory(PrincipalKind::User))
        .unwrap_err();
    assert!(matches!(principal_error, Error::Configuration(_)));

    let fixture = ContractProductRuntimeFixture::default();
    let service_application_error = ClientBuilder::new(ClientConfig::in_process())
        .service_credential(ServiceCredential::new("other-app", "service-secret").unwrap())
        .bind_runtime(fixture.facade, context_factory(PrincipalKind::Service))
        .unwrap_err();
    assert!(matches!(service_application_error, Error::Configuration(_)));

    let fixture = ContractProductRuntimeFixture::default();
    let other_application = TrustedContextFactory::new(
        "contract-actor",
        PrincipalKind::User,
        Some("other-app".to_string()),
        InvocationSource::EmbeddedSdk,
    )
    .unwrap();
    let application_error =
        ClientBuilder::new(ClientConfig::in_process().with_selected_application("contract-app"))
            .user_credential(UserCredential::new("user-secret").unwrap())
            .bind_runtime(fixture.facade, other_application)
            .unwrap_err();
    assert!(matches!(application_error, Error::Configuration(_)));
}

#[cfg(feature = "http-client")]
#[test]
fn authenticated_http_clients_construct_without_performing_network_io() {
    let service = ClientBuilder::new(ClientConfig::new("http://127.0.0.1:9"))
        .service_credential(ServiceCredential::new("contract-app", "service-secret").unwrap())
        .connect()
        .unwrap();
    assert_service_capabilities(&service);
    let rendered = format!("{service:?}");
    assert!(rendered.contains("http"));
    assert!(!rendered.contains("service-secret"));

    let user = ClientBuilder::new(
        ClientConfig::new("http://127.0.0.1:9").with_selected_application("contract-app"),
    )
    .user_credential(UserCredential::new("user-secret").unwrap())
    .connect()
    .unwrap();
    assert_user_capabilities(&user);
}
