#![cfg(feature = "service-http")]
use axum::{
    body::Body,
    http::{Request, StatusCode},
    routing::get,
    Json, Router,
};
use kish_lingshu_foundation_contract::service_auth::{ServiceSigner, SIGNATURE_HEADER};
use kish_lingshu_sdk::{provider::*, ServiceAuthError, ServiceConnection, ServiceCredential};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tower::ServiceExt;

async fn setup() -> (
    ServiceConnection,
    ServiceSigner,
    tokio::task::JoinHandle<()>,
) {
    let signer = ServiceSigner::new(&[19; 32]).unwrap();
    let trust = signer.trust("app", chrono::Utc::now().timestamp()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route(
                "/api/user/services/v1/service-auth/trust",
                get(move || {
                    let trust = trust.clone();
                    async move { Json(trust) }
                }),
            ),
        )
        .await
        .unwrap();
    });
    let connection = ServiceConnection::connect(
        &format!("http://{address}"),
        ServiceCredential::new("app", "root").unwrap(),
    )
    .await
    .unwrap();
    (connection, signer, task)
}
fn catalog() -> ProviderCatalog {
    ProviderCatalog {
        format_version: 1,
        application_id: "app".into(),
        provider_key: "backend".into(),
        release: "v2".into(),
        services: None,
        events: None,
        workflows: vec![ProviderWorkflow {
            key: "refund".into(),
            name: "Refund".into(),
            description: None,
            define_schema: BTreeMap::from([("nodes".into(), serde_json::json!([]))]),
        }],
    }
}
#[tokio::test]
async fn catalog_requires_signed_app_and_exact_audience_and_keeps_identity() {
    let (connection, signer, server) = setup().await;
    let target = "https://provider.example/internal/catalog";
    let adapter = ProviderHttpAdapter::new(connection.clone(), catalog()).unwrap();
    let router = Router::new().nest("/internal/catalog", adapter.router(target).unwrap());
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/internal/catalog")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    for (app, url, expected) in [
        ("other", target, StatusCode::UNAUTHORIZED),
        (
            "app",
            "https://other.example/internal/catalog",
            StatusCode::UNAUTHORIZED,
        ),
        ("app", target, StatusCode::OK),
    ] {
        let proof = signer
            .sign_callback(app, "GET", url, &[], chrono::Utc::now().timestamp())
            .unwrap();
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/internal/catalog")
                    .header(SIGNATURE_HEADER, proof)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        if expected == StatusCode::OK {
            assert!(response.headers().contains_key("etag"));
            let bytes = axum::body::to_bytes(response.into_body(), MAX_PROVIDER_CATALOG_BYTES)
                .await
                .unwrap();
            let read: ProviderCatalog = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(read.digest().unwrap(), catalog().digest().unwrap());
        }
    }
    connection.shutdown().await;
    server.abort();
}
#[tokio::test]
async fn pending_activation_retries_missing_catalog_but_not_authentication() {
    let (connection, _, server) = setup().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let result = connection
        .wait_for_catalog(|| {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt == 0 {
                    Err(ServiceAuthError::Http(422))
                } else {
                    Ok("active")
                }
            }
        })
        .await
        .unwrap();
    assert_eq!(result, "active");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(
        connection
            .wait_for_catalog(|| async { Err::<(), _>(ServiceAuthError::Http(401)) })
            .await,
        Err(ServiceAuthError::Http(401))
    );
    connection.shutdown().await;
    server.abort();
}
#[tokio::test]
async fn pending_activation_stops_when_connection_closes() {
    let (connection, _, server) = setup().await;
    let pending = connection.clone();
    let task = tokio::spawn(async move {
        pending
            .wait_for_catalog(|| async { Err::<(), _>(ServiceAuthError::Http(404)) })
            .await
    });
    tokio::task::yield_now().await;
    connection.shutdown().await;
    assert_eq!(task.await.unwrap(), Err(ServiceAuthError::Closed));
    server.abort();
}
