//! Bounded identity relationship graph.
//!
//! The graph is derived from the same aggregated counterparty rows used by
//! identity history. It is not a consensus object and does not claim to be a
//! complete wallet clustering: an edge means indexed transfers were observed
//! between two identities in the selected direction and window.

use std::collections::BTreeMap;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, header};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{ApiError, AppState, wallet};

const DEFAULT_LIMIT: u32 = 20;
const MAX_NODES: usize = 100;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/identities/{identity}/graph", get(identity_graph))
        .route("/v1/graph", get(graph))
}

#[derive(Debug, Default, Deserialize)]
struct GraphQuery {
    #[serde(default)]
    identity: Option<String>,
    #[serde(default, rename = "depth")]
    depth: Option<u8>,
    #[serde(default)]
    direction: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

async fn identity_graph(
    State(state): State<AppState>,
    Path(identity): Path<String>,
    Query(params): Query<GraphQuery>,
) -> Result<impl IntoResponse, ApiError> {
    graph_inner(state, identity, params).await
}

async fn graph(
    State(state): State<AppState>,
    Query(params): Query<GraphQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let identity = params
        .identity
        .clone()
        .ok_or_else(|| ApiError::bad_request("query parameter identity is required"))?;
    graph_inner(state, identity, params).await
}

async fn graph_inner(
    state: AppState,
    identity: String,
    params: GraphQuery,
) -> Result<impl IntoResponse, ApiError> {
    let (identity, _) = wallet::parse_identity(&identity)?;
    let depth = params.depth.unwrap_or(1).clamp(1, 2);
    let direction = match params.direction.as_deref() {
        None | Some("") | Some("all") => "all",
        Some("incoming") => "incoming",
        Some("outgoing") => "outgoing",
        Some(other) => {
            return Err(ApiError::bad_request(format!(
                "unknown graph direction {other:?}; expected incoming, outgoing, or all"
            )));
        }
    };
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, 50);

    let center = state.store.identity(&identity).await?;
    let mut nodes: BTreeMap<String, Value> = BTreeMap::new();
    let center_depth = 0u8;
    add_node(
        &state,
        &mut nodes,
        &identity,
        center_depth,
        center.is_some(),
    );
    let mut edges: BTreeMap<(String, String), Value> = BTreeMap::new();

    let first = state
        .store
        .counterparties(&identity, Some(direction), limit)
        .await?;
    for row in &first {
        add_node(&state, &mut nodes, &row.identity, 1, true);
        add_edge(&mut edges, &identity, row, direction);
    }

    if depth == 2 && nodes.len() < MAX_NODES {
        let neighbours: Vec<String> = first.iter().map(|row| row.identity.clone()).collect();
        for neighbour in neighbours {
            if nodes.len() >= MAX_NODES {
                break;
            }
            let rows = state
                .store
                .counterparties(&neighbour, Some(direction), 5)
                .await?;
            for row in &rows {
                if nodes.len() >= MAX_NODES {
                    break;
                }
                add_node(&state, &mut nodes, &row.identity, 2, true);
                add_edge(&mut edges, &neighbour, row, direction);
            }
        }
    }

    let edge_values: Vec<Value> = edges.into_values().collect();
    Ok((
        [(
            header::CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=15, stale-while-revalidate=30"),
        )],
        Json(json!({
            "data": {
                "center": identity,
                "nodes": nodes.into_values().collect::<Vec<_>>(),
                "edges": edge_values,
            },
            "meta": {
                "source": "index",
                "depth": depth,
                "direction": direction,
                "limit": limit,
                "node_limit": MAX_NODES,
                "generated_at": chrono::Utc::now(),
                "note": "Edges are observed indexed transfers, not proof of common ownership.",
            }
        })),
    ))
}

fn add_node(
    state: &AppState,
    nodes: &mut BTreeMap<String, Value>,
    identity: &str,
    depth: u8,
    indexed: bool,
) {
    if nodes.contains_key(identity) {
        return;
    }
    let mut value = json!({
        "identity": identity,
        "depth": depth,
        "indexed": indexed,
    });
    state.labels.enrich_identity(&mut value, identity);
    nodes.insert(identity.to_owned(), value);
}

fn add_edge(
    edges: &mut BTreeMap<(String, String), Value>,
    source: &str,
    row: &store::CounterpartyRow,
    direction: &str,
) {
    let (from, to) = match direction {
        "incoming" => (row.identity.clone(), source.to_owned()),
        "outgoing" => (source.to_owned(), row.identity.clone()),
        _ if row.net_amount >= 0 => (row.identity.clone(), source.to_owned()),
        _ => (source.to_owned(), row.identity.clone()),
    };
    edges.entry((from.clone(), to.clone())).or_insert_with(|| {
        json!({
            "from": from,
            "to": to,
            "incoming_amount": row.incoming_amount,
            "outgoing_amount": row.outgoing_amount,
            "net_amount": row.net_amount,
            "transaction_count": row.transaction_count,
            "first_tick": row.first_tick,
            "last_tick": row.last_tick,
        })
    });
}
