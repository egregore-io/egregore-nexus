use std::path::Path;

use std::collections::HashMap;

use axum::{
    extract::{Path as AxumPath, Query},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use nexus::cli::read_client::ReadClient;
use nexus_contracts::{codes, HistoryRequest, SearchMode, SearchRequest};
use serde_json::json;

async fn search() -> impl IntoResponse {
    Json(json!([{
        "messageId": "m_search",
        "from": "ada",
        "when": 41,
        "snippet": "gateway-owned result",
        "score": 1.0
    }]))
}

async fn history() -> impl IntoResponse {
    Json(json!([{
        "messageId": "m_history",
        "from": "ada",
        "when": 42,
        "body": "gateway-owned history"
    }]))
}

async fn message(
    AxumPath(id): AxumPath<String>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    if query.get("me").map(String::as_str) != Some("operator") {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "missing caller"})),
        )
            .into_response();
    }
    Json(json!({
        "id": id,
        "project": "default",
        "from": "ada",
        "scope": "dm",
        "body": "gateway-owned message",
        "provenance": { "from": "ada", "kind": "agent" },
        "createdAt": 43
    }))
    .into_response()
}

async fn serve_gateway(home: &Path) -> tokio::task::JoinHandle<()> {
    let app = Router::new()
        .route("/api/v1/search", get(search))
        .route("/api/v1/history", get(history))
        .route("/api/v1/messages/:id", get(message));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    std::fs::write(
        home.join("gateway.json"),
        serde_json::to_vec(&json!({
            "pid": std::process::id(),
            "url": format!("http://{address}")
        }))
        .unwrap(),
    )
    .unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    })
}

#[tokio::test]
async fn memory_reads_use_the_discovered_gateway_instead_of_daemon_query() {
    let home = tempfile::tempdir().unwrap();
    let server = serve_gateway(home.path()).await;
    let client = ReadClient::from_daemon_for_tests(home.path().to_path_buf());

    let search = client
        .search(SearchRequest {
            query: "gateway".into(),
            mode: SearchMode::Fts,
            limit: Some(10),
            thread: None,
            with: None,
            since: None,
        })
        .await
        .unwrap();
    assert_eq!(search.hits.len(), 1);
    assert_eq!(search.hits[0].message_id.0, "m_search");

    let history = client
        .history(HistoryRequest {
            thread: Some("design".into()),
            with: None,
            topic: None,
            limit: Some(10),
            before: None,
        })
        .await
        .unwrap();
    assert_eq!(history.entries.len(), 1);
    assert_eq!(history.entries[0].body, "gateway-owned history");

    let message = client.message("m_full").await.unwrap();
    assert_eq!(message.id.0, "m_full");
    assert_eq!(message.body, "gateway-owned message");
    server.abort();
}

#[tokio::test]
async fn memory_reads_report_gateway_unavailable_without_falling_back_to_daemon() {
    let home = tempfile::tempdir().unwrap();
    let client = ReadClient::from_daemon_for_tests(home.path().to_path_buf());
    let error = client
        .history(HistoryRequest {
            thread: Some("design".into()),
            with: None,
            topic: None,
            limit: Some(10),
            before: None,
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, codes::INTERNAL_ERROR);
    assert!(error.message.contains("Gateway unavailable"), "{error}");
}
