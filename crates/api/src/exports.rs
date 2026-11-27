//! Bounded CSV and NDJSON exports for indexed collections.
//!
//! Exports are synchronous responses with a hard row cap. They are intended
//! for analyst snapshots and wallet history downloads, not for mirroring the
//! full chain; the archive and index remain the complete sources.

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde::Deserialize;
use serde_json::{Value, json};
use store::{EventFilter, TransactionFilter};

use crate::dto::{event_value, transaction_value};
use crate::{ApiError, AppState};

const MAX_LIMIT: u32 = 10_000;

pub fn router() -> Router<AppState> {
    Router::new().route("/v1/export/{resource}", get(export))
}

#[derive(Debug, Default, Deserialize)]
struct ExportQuery {
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    epoch: Option<u16>,
    #[serde(default)]
    tick: Option<u32>,
    #[serde(default)]
    min_tick: Option<u32>,
    #[serde(default)]
    identity: Option<String>,
    #[serde(default, rename = "type")]
    type_name: Option<String>,
    #[serde(default)]
    issuer: Option<String>,
    #[serde(default)]
    contract_index: Option<u32>,
}

fn columns(resource: &str) -> Option<&'static [&'static str]> {
    Some(match resource {
        "transactions" => &[
            "epoch",
            "tick",
            "tx_index",
            "hash",
            "source",
            "destination",
            "amount",
            "input_type",
            "input_data",
            "status",
            "timestamp",
            "target_tick",
        ],
        "events" => &[
            "epoch",
            "tick",
            "log_id",
            "log_type",
            "type_name",
            "tx_hash",
            "tx_index",
            "body",
            "log_digest",
            "timestamp",
        ],
        "identities" => &[
            "identity",
            "public_key",
            "balance",
            "incoming_amount",
            "outgoing_amount",
            "transfer_count",
            "first_seen_tick",
            "last_seen_tick",
        ],
        "assets" => &[
            "issuer",
            "name",
            "decimals",
            "unit",
            "managing_contract",
            "shares",
            "issued_tick",
            "log_id",
            "timestamp",
        ],
        "contracts" => &["contract_index", "event_count", "first_tick", "last_tick"],
        "dividends" => &[
            "epoch",
            "tick",
            "start_log_id",
            "contract_index",
            "contract",
            "total_amount",
            "transfer_count",
            "recipient_count",
            "share_count",
            "amount_per_share",
            "fingerprint",
            "timestamp",
        ],
        _ => return None,
    })
}

async fn export(
    State(state): State<AppState>,
    Path(resource): Path<String>,
    Query(params): Query<ExportQuery>,
) -> Result<Response, ApiError> {
    let columns = columns(&resource).ok_or_else(|| {
        ApiError::not_found(format!(
            "unknown export resource {resource:?}; expected transactions, events, identities, assets, contracts, or dividends"
        ))
    })?;
    let format = params
        .format
        .as_deref()
        .unwrap_or("csv")
        .to_ascii_lowercase();
    if format != "csv" && format != "ndjson" {
        return Err(ApiError::bad_request(format!(
            "unknown export format {format:?}; expected csv or ndjson"
        )));
    }
    let limit = params.limit.unwrap_or(1_000).clamp(1, MAX_LIMIT);

    let rows: Vec<Value> = match resource.as_str() {
        "transactions" => {
            let identity = match params.identity.as_deref() {
                Some(value) if !value.trim().is_empty() => {
                    let identity = value.trim().to_ascii_uppercase();
                    qubic::PublicKey::from_identity(&identity)
                        .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
                    Some(identity)
                }
                _ => None,
            };
            state
                .store
                .transactions_page(
                    &TransactionFilter {
                        epoch: params.epoch,
                        tick: params.tick,
                        min_tick: params.min_tick,
                        identity,
                    },
                    limit,
                    0,
                )
                .await?
                .iter()
                .map(transaction_value)
                .collect()
        }
        "events" => state
            .store
            .events_page(
                &EventFilter {
                    epoch: params.epoch,
                    tick: params.tick,
                    min_tick: params.min_tick,
                    type_name: params.type_name.clone(),
                },
                limit,
                0,
            )
            .await?
            .into_iter()
            .map(event_value)
            .collect(),
        "identities" => state
            .store
            .identities(limit, 0)
            .await?
            .into_iter()
            .filter_map(|row| serde_json::to_value(row).ok())
            .collect(),
        "assets" => {
            let issuer = match params.issuer.as_deref() {
                Some(value) if !value.trim().is_empty() => {
                    let issuer = value.trim().to_ascii_uppercase();
                    qubic::PublicKey::from_identity(&issuer)
                        .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
                    Some(issuer)
                }
                _ => None,
            };
            state
                .store
                .assets(issuer.as_deref(), limit, 0)
                .await?
                .into_iter()
                .filter_map(|row| serde_json::to_value(row).ok())
                .collect()
        }
        "contracts" => state
            .store
            .contracts(limit, 0)
            .await?
            .into_iter()
            .filter_map(|row| serde_json::to_value(row).ok())
            .collect(),
        "dividends" => state
            .store
            .contract_dividends(params.contract_index, params.epoch, limit, 0)
            .await?
            .into_iter()
            .map(|row| {
                json!({
                    "epoch": row.epoch,
                    "tick": row.tick,
                    "start_log_id": row.start_log_id,
                    "contract_index": row.contract_index,
                    "contract": row.contract,
                    "total_amount": row.total_amount,
                    "transfer_count": row.transfer_count,
                    "recipient_count": row.recipient_count,
                    "share_count": row.share_count,
                    "amount_per_share": row.amount_per_share,
                    "fingerprint": hex::encode(row.fingerprint),
                    "timestamp": row.timestamp,
                })
            })
            .collect(),
        _ => unreachable!("resource was checked above"),
    };

    let body = if format == "csv" {
        csv_body(columns, &rows)
    } else {
        let mut body = String::new();
        for row in &rows {
            body.push_str(&serde_json::to_string(row).unwrap_or_else(|_| "null".into()));
            body.push('\n');
        }
        body
    };
    let content_type = if format == "csv" {
        "text/csv; charset=utf-8"
    } else {
        "application/x-ndjson; charset=utf-8"
    };
    let filename = format!(
        "glyph-{resource}-{}.{}",
        chrono::Utc::now().format("%Y%m%dT%H%M%SZ"),
        format
    );
    Ok((
        [
            (header::CONTENT_TYPE, HeaderValue::from_static(content_type)),
            (header::CACHE_CONTROL, HeaderValue::from_static("no-store")),
            (
                header::CONTENT_DISPOSITION,
                HeaderValue::from_str(&format!("attachment; filename=\"{filename}\""))
                    .unwrap_or_else(|_| HeaderValue::from_static("attachment")),
            ),
            (
                header::HeaderName::from_static("x-export-rows"),
                HeaderValue::from_str(&rows.len().to_string())
                    .unwrap_or_else(|_| HeaderValue::from_static("0")),
            ),
        ],
        body,
    )
        .into_response())
}

fn csv_body(columns: &[&str], rows: &[Value]) -> String {
    let mut out = String::new();
    out.push_str(&columns.join(","));
    out.push('\n');
    for row in rows {
        for (index, column) in columns.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push_str(&csv_cell(row.get(*column)));
        }
        out.push('\n');
    }
    out
}

fn csv_cell(value: Option<&Value>) -> String {
    let text = match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(value)) => value.clone(),
        Some(value) => value.to_string(),
    };
    if text.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", text.replace('"', "\"\""))
    } else {
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_cells_quote_commas_quotes_and_newlines() {
        assert_eq!(csv_cell(Some(&json!("plain"))), "plain");
        assert_eq!(csv_cell(Some(&json!("a,b"))), "\"a,b\"");
        assert_eq!(csv_cell(Some(&json!("a\"b"))), "\"a\"\"b\"");
        assert_eq!(csv_cell(Some(&json!("a\nb"))), "\"a\nb\"");
        assert_eq!(csv_cell(None), "");
        assert_eq!(csv_cell(Some(&json!({"a": 1}))), "\"{\"\"a\"\":1}\"");
    }

    #[test]
    fn csv_body_keeps_a_stable_header_for_empty_results() {
        let body = csv_body(&["a", "b"], &[]);
        assert_eq!(body, "a,b\n");
        let body = csv_body(&["a", "b"], &[json!({"a": 1, "b": null})]);
        assert_eq!(body, "a,b\n1,\n");
    }

    #[test]
    fn export_resources_are_closed() {
        assert!(columns("transactions").is_some());
        assert!(columns("events").is_some());
        assert!(columns("dividends").is_some());
        assert!(columns("wallet").is_none());
    }
}
