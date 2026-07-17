use std::path::Path;

use http_body_util::{BodyExt, Empty};
use hyper::body::Bytes;
use hyper::{Method, Request, StatusCode};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use nexus_contracts::{
    codes, ContractError, HistoryEntry, HistoryRequest, HistoryResponse, Message, SearchHit,
    SearchMode, SearchRequest, SearchResponse,
};
use serde::de::DeserializeOwned;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GatewayDiscovery {
    url: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GatewaySearchHit {
    message_id: String,
    from: String,
    when: i64,
    snippet: String,
    score: f32,
}

#[derive(Debug, Deserialize)]
struct GatewayHistoryEntry {
    from: String,
    when: i64,
    summary: Option<String>,
    body: String,
}

#[derive(Debug, Deserialize)]
struct GatewayErrorEnvelope {
    error: GatewayErrorBody,
}

#[derive(Debug, Deserialize)]
struct GatewayErrorBody {
    message: String,
}

#[derive(Clone)]
pub(crate) struct GatewayReadClient {
    base_url: String,
    client: Client<HttpConnector, Empty<Bytes>>,
}

impl GatewayReadClient {
    pub(crate) fn discover(home: &Path) -> Result<Self, ContractError> {
        let path = home.join("gateway.json");
        let raw = std::fs::read_to_string(&path)
            .map_err(|error| unavailable(&format!("cannot read {}: {error}", path.display())))?;
        let discovery: GatewayDiscovery = serde_json::from_str(&raw)
            .map_err(|error| unavailable(&format!("invalid {}: {error}", path.display())))?;
        if !discovery.url.starts_with("http://") {
            return Err(unavailable("discovery URL must use local http"));
        }
        Ok(Self {
            base_url: discovery.url.trim_end_matches('/').to_string(),
            client: Client::builder(TokioExecutor::new()).build_http(),
        })
    }

    pub(crate) async fn search(
        &self,
        caller: &str,
        request: SearchRequest,
    ) -> Result<SearchResponse, ContractError> {
        let mut query = vec![
            ("q", request.query),
            (
                "mode",
                match request.mode {
                    SearchMode::Fts => "fts",
                    SearchMode::Semantic => "semantic",
                    SearchMode::Hybrid => "hybrid",
                }
                .to_string(),
            ),
            ("me", caller.to_string()),
        ];
        push_opt(
            &mut query,
            "limit",
            request.limit.map(|value| value.to_string()),
        );
        push_opt(&mut query, "thread", request.thread);
        push_opt(&mut query, "with", request.with);
        push_opt(
            &mut query,
            "since",
            request.since.map(|value| value.to_string()),
        );
        let hits: Vec<GatewaySearchHit> = self.get("/api/v1/search", &query).await?;
        Ok(SearchResponse {
            hits: hits
                .into_iter()
                .map(|hit| SearchHit {
                    message_id: nexus_contracts::MessageId(hit.message_id),
                    from: hit.from,
                    when: hit.when,
                    snippet: hit.snippet,
                    score: hit.score,
                })
                .collect(),
        })
    }

    pub(crate) async fn history(
        &self,
        caller: &str,
        request: HistoryRequest,
    ) -> Result<HistoryResponse, ContractError> {
        let mut query = vec![("me", caller.to_string())];
        push_opt(&mut query, "thread", request.thread);
        push_opt(&mut query, "with", request.with);
        push_opt(&mut query, "topic", request.topic);
        push_opt(
            &mut query,
            "limit",
            request.limit.map(|value| value.to_string()),
        );
        push_opt(
            &mut query,
            "before",
            request.before.map(|value| value.to_string()),
        );
        let entries: Vec<GatewayHistoryEntry> = self.get("/api/v1/history", &query).await?;
        Ok(HistoryResponse {
            entries: entries
                .into_iter()
                .map(|entry| HistoryEntry {
                    from: entry.from,
                    when: entry.when,
                    summary: entry.summary,
                    body: entry.body,
                })
                .collect(),
        })
    }

    pub(crate) async fn message(&self, caller: &str, id: &str) -> Result<Message, ContractError> {
        self.get(
            &format!("/api/v1/messages/{}", encode_component(id)),
            &[("me", caller.to_string())],
        )
        .await
    }

    async fn get<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T, ContractError> {
        let suffix = if query.is_empty() {
            String::new()
        } else {
            format!(
                "?{}",
                query
                    .iter()
                    .map(|(key, value)| format!(
                        "{}={}",
                        encode_component(key),
                        encode_component(value)
                    ))
                    .collect::<Vec<_>>()
                    .join("&")
            )
        };
        let uri = format!("{}{}{}", self.base_url, path, suffix);
        let request = Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .header("accept", "application/json")
            .body(Empty::<Bytes>::new())
            .map_err(|error| unavailable(&format!("invalid request: {error}")))?;
        let response = self
            .client
            .request(request)
            .await
            .map_err(|error| unavailable(&format!("request failed: {error}")))?;
        let status = response.status();
        let bytes = response
            .into_body()
            .collect()
            .await
            .map_err(|error| unavailable(&format!("response failed: {error}")))?
            .to_bytes();
        if !status.is_success() {
            let message = serde_json::from_slice::<GatewayErrorEnvelope>(&bytes)
                .map(|body| body.error.message)
                .unwrap_or_else(|_| format!("Gateway returned HTTP {status}"));
            return Err(ContractError {
                code: if status == StatusCode::NOT_FOUND {
                    codes::NOT_FOUND
                } else if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
                    codes::UNAUTHORIZED
                } else {
                    codes::INTERNAL_ERROR
                },
                message,
            });
        }
        serde_json::from_slice(&bytes).map_err(|error| ContractError {
            code: codes::INTERNAL_ERROR,
            message: format!("Gateway returned an invalid response: {error}"),
        })
    }
}

fn push_opt(query: &mut Vec<(&'static str, String)>, key: &'static str, value: Option<String>) {
    if let Some(value) = value {
        query.push((key, value));
    }
}

fn unavailable(detail: &str) -> ContractError {
    ContractError {
        code: codes::INTERNAL_ERROR,
        message: format!("Gateway unavailable: {detail}; start it with `nexus gateway start`"),
    }
}

fn encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            let _ = write!(encoded, "%{byte:02X}");
        }
    }
    encoded
}
