#![cfg(all(feature = "http-client", feature = "event-consumer-http"))]

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, Method, Uri},
    response::{IntoResponse, Response},
    Json, Router,
};
use kish_lingshu_sdk::{
    assets::AssetUpload, workspaces::WorkspaceApprovalMode, ClientBuilder, ClientConfig, Error,
    RequestOptions, UserCredential,
};
use serde_json::{json, Value};

async fn endpoint(
    State(base): State<String>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if uri.path().starts_with("/storage/") {
        assert!(!headers.contains_key("x-token"));
        assert!(!headers.contains_key("authorization"));
        return match uri.path() {
            "/storage/image" => b"image content".as_slice().into_response(),
            "/storage/upload" => {
                assert_eq!(method, Method::POST);
                assert!(body.windows(13).any(|chunk| chunk == b"image content"));
                ().into_response()
            }
            _ => panic!("unexpected storage route"),
        };
    }
    assert_eq!(headers["x-token"], "private-client-token");
    assert_eq!(headers["x-correlation-id"], "asset-workspace-request");
    let data = match uri.path() {
        "/api/user/client-workspaces/id" => {
            assert_eq!(method, Method::POST);
            json!({"workspace_id": 17})
        }
        "/api/user/apps/test/workspaces/17/tool-approval-mode" => {
            assert_eq!(method, Method::PUT);
            assert_eq!(
                serde_json::from_slice::<Value>(&body).unwrap(),
                json!({"approval_mode":"review_changes"})
            );
            json!({"approval_mode":"review_changes"})
        }
        "/api/user/osss/request-upload-url" => {
            json!({"upload_url":format!("{base}/storage/upload"), "oss_key":"image@test"})
        }
        "/api/user/osss/request-download-url" => json!(format!("{base}/storage/image")),
        _ => panic!("unexpected SDK route {uri}"),
    };
    Json(json!({"status":true,"data":data})).into_response()
}

#[tokio::test]
async fn public_asset_and_workspace_operations_preserve_authentication_and_wire_behavior() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new().fallback(endpoint).with_state(base.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let client = ClientBuilder::new(ClientConfig::new(&base))
        .user_credential(UserCredential::new("private-client-token").unwrap())
        .connect()
        .unwrap();
    let options = RequestOptions::new()
        .with_correlation_id("asset-workspace-request")
        .unwrap();
    assert_eq!(
        client
            .workspaces()
            .allocate_id(options.clone())
            .await
            .unwrap(),
        17
    );
    assert_eq!(
        client
            .workspaces()
            .update_approval_mode(
                "test",
                17,
                WorkspaceApprovalMode::ReviewChanges,
                options.clone()
            )
            .await
            .unwrap(),
        WorkspaceApprovalMode::ReviewChanges
    );
    let asset = client
        .assets()
        .upload(
            AssetUpload {
                file_name: "sample.png".into(),
                media_type: "image/png".into(),
                bytes: b"image content".to_vec(),
            },
            options.clone(),
        )
        .await
        .unwrap();
    assert_eq!(asset.asset_id, "image@test");
    assert_eq!(
        client
            .assets()
            .download_image(&asset.url, options.clone())
            .await
            .unwrap(),
        b"image content"
    );
    assert_eq!(
        client
            .assets()
            .download_image("data:image/png;base64,aGk=", options.clone())
            .await
            .unwrap(),
        b"hi"
    );
    assert!(matches!(
        client
            .assets()
            .download_image("file:///tmp/image.png", options)
            .await,
        Err(Error::Configuration(_))
    ));
    server.abort();
}
