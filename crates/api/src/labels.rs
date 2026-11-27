//! Operator-maintained labels for identities, assets, and contract indices.
//!
//! Labels are presentation metadata, never chain state. The registry is loaded
//! from `GLYPH_LABELS_FILE` when set, otherwise from the embedded starter file,
//! and can be replaced without touching the index.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{ApiError, AppState};

const EMBEDDED_LABELS: &str = include_str!("../assets/labels.json");

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Label {
    pub name: String,
    pub category: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asset: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub website: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub verified: bool,
}

#[derive(Debug, Default, Deserialize)]
struct LabelFile {
    #[serde(default)]
    labels: Vec<Label>,
}

#[derive(Debug, Default, Clone)]
pub struct Labels {
    labels: Arc<Vec<Label>>,
    by_identity: Arc<HashMap<String, usize>>,
    by_contract: Arc<HashMap<u32, usize>>,
    by_asset: Arc<HashMap<String, usize>>,
}

impl Labels {
    pub fn from_env() -> Self {
        let source = std::env::var("GLYPH_LABELS_FILE").ok();
        match source.as_deref() {
            Some(path) if !path.trim().is_empty() => match Self::from_path(path) {
                Ok(labels) => labels,
                Err(error) => {
                    tracing::warn!(%error, path, "labels file could not be loaded; using embedded labels");
                    Self::from_json(EMBEDDED_LABELS).unwrap_or_default()
                }
            },
            _ => Self::from_json(EMBEDDED_LABELS).unwrap_or_default(),
        }
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, String> {
        let bytes = std::fs::read(path.as_ref())
            .map_err(|error| format!("read {}: {error}", path.as_ref().display()))?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|error| format!("{} is not UTF-8: {error}", path.as_ref().display()))?;
        Self::from_json(text)
    }

    pub fn from_json(text: &str) -> Result<Self, String> {
        let file: LabelFile =
            serde_json::from_str(text).map_err(|error| format!("labels JSON: {error}"))?;
        Ok(Self::new(file.labels))
    }

    pub fn new(labels: Vec<Label>) -> Self {
        let mut by_identity = HashMap::new();
        let mut by_contract = HashMap::new();
        let mut by_asset = HashMap::new();
        for (index, label) in labels.iter().enumerate() {
            if let Some(identity) = &label.identity {
                by_identity
                    .entry(identity.trim().to_ascii_uppercase())
                    .or_insert(index);
            }
            if let Some(contract_index) = label.contract_index {
                by_contract.entry(contract_index).or_insert(index);
            }
            if let Some(asset) = &label.asset {
                by_asset
                    .entry(asset.trim().to_ascii_uppercase())
                    .or_insert(index);
            }
        }
        Self {
            labels: Arc::new(labels),
            by_identity: Arc::new(by_identity),
            by_contract: Arc::new(by_contract),
            by_asset: Arc::new(by_asset),
        }
    }

    pub fn all(&self) -> &[Label] {
        &self.labels
    }

    pub fn identity(&self, identity: &str) -> Option<&Label> {
        self.by_identity
            .get(&identity.trim().to_ascii_uppercase())
            .and_then(|index| self.labels.get(*index))
    }

    pub fn contract(&self, contract_index: u32) -> Option<&Label> {
        self.by_contract
            .get(&contract_index)
            .and_then(|index| self.labels.get(*index))
    }

    pub fn asset(&self, issuer: &str, name: &str) -> Option<&Label> {
        let key = format!(
            "{}/{}",
            issuer.trim().to_ascii_uppercase(),
            name.trim().to_ascii_uppercase()
        );
        self.by_asset
            .get(&key)
            .and_then(|index| self.labels.get(*index))
    }

    pub fn search(&self, query: &str, limit: u32) -> Vec<&Label> {
        let query = query.trim().to_ascii_lowercase();
        let mut matches: Vec<&Label> = self
            .labels
            .iter()
            .filter(|label| {
                label.name.to_ascii_lowercase().contains(&query)
                    || label.category.to_ascii_lowercase().contains(&query)
                    || label
                        .identity
                        .as_deref()
                        .is_some_and(|value| value.to_ascii_lowercase().contains(&query))
                    || label
                        .asset
                        .as_deref()
                        .is_some_and(|value| value.to_ascii_lowercase().contains(&query))
                    || label
                        .description
                        .as_deref()
                        .is_some_and(|value| value.to_ascii_lowercase().contains(&query))
            })
            .collect();
        matches.sort_by(|a, b| {
            b.verified
                .cmp(&a.verified)
                .then_with(|| a.name.cmp(&b.name))
        });
        matches.truncate(limit.clamp(1, 200) as usize);
        matches
    }

    pub fn enrich_identity(&self, value: &mut Value, identity: &str) {
        if let Some(label) = self.identity(identity) {
            value["label"] = json!(label);
        }
    }

    pub fn enrich_asset(&self, value: &mut Value, issuer: &str, name: &str) {
        if let Some(label) = self.asset(issuer, name) {
            value["label"] = json!(label);
        }
    }

    pub fn enrich_contract(&self, value: &mut Value, contract_index: u32) {
        if let Some(label) = self.contract(contract_index) {
            value["label"] = json!(label);
        }
    }

    /// Adds `source_label` and `destination_label` when both ends of a
    /// transfer are known.
    pub fn enrich_transaction(&self, value: &mut Value) {
        let source = value
            .get("source")
            .and_then(Value::as_str)
            .and_then(|identity| self.identity(identity).cloned());
        let destination = value
            .get("destination")
            .and_then(Value::as_str)
            .and_then(|identity| self.identity(identity).cloned());
        if let Some(label) = source {
            value["source_label"] = json!(label);
        }
        if let Some(label) = destination {
            value["destination_label"] = json!(label);
        }
    }

    /// Labels an identity and every asset entry in a wallet payload.
    pub fn enrich_wallet(&self, value: &mut Value, identity: &str) {
        self.enrich_identity(value, identity);
        if let Some(assets) = value.get_mut("assets") {
            self.enrich_asset_lists(assets);
        }
        self.enrich_asset_lists(value);
    }

    fn enrich_asset_lists(&self, value: &mut Value) {
        for key in ["issued", "owned", "possessed"] {
            let Some(entries) = value.get_mut(key).and_then(Value::as_array_mut) else {
                continue;
            };
            for entry in entries {
                let issuer = entry
                    .get("issuer")
                    .and_then(Value::as_str)
                    .or_else(|| entry.get("issuance")?.get("issuer")?.as_str())
                    .map(str::to_string);
                let name = entry
                    .get("name")
                    .and_then(Value::as_str)
                    .or_else(|| entry.get("issuance")?.get("name")?.as_str())
                    .map(str::to_string);
                if let (Some(issuer), Some(name)) = (issuer, name) {
                    self.enrich_asset(entry, &issuer, &name);
                }
            }
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct LabelsQuery {
    #[serde(default)]
    q: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

async fn list_labels(
    State(state): State<AppState>,
    Query(params): Query<LabelsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let limit = params.limit.unwrap_or(100).clamp(1, 200);
    let labels: Vec<&Label> = match params.q.as_deref().map(str::trim) {
        Some(query) if !query.is_empty() => state.labels.search(query, limit),
        _ => state.labels.all().iter().take(limit as usize).collect(),
    };
    Ok((
        [(
            header::CACHE_CONTROL,
            "public, max-age=300, stale-while-revalidate=600",
        )],
        Json(json!({
            "data": labels,
            "meta": {
                "count": labels.len(),
                "limit": limit,
                "query": params.q,
                "source": "operator-registry"
            }
        })),
    ))
}

pub fn router() -> Router<AppState> {
    Router::new().route("/v1/labels", get(list_labels))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels() -> Labels {
        Labels::new(vec![
            Label {
                name: "Exchange".into(),
                category: "exchange".into(),
                identity: Some(
                    "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK".into(),
                ),
                contract_index: None,
                asset: None,
                website: None,
                description: None,
                verified: true,
            },
            Label {
                name: "QX".into(),
                category: "contract".into(),
                identity: None,
                contract_index: Some(1),
                asset: None,
                website: None,
                description: None,
                verified: true,
            },
            Label {
                name: "QX".into(),
                category: "asset".into(),
                identity: None,
                contract_index: None,
                asset: Some(
                    "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK/QX".into(),
                ),
                website: None,
                description: None,
                verified: false,
            },
        ])
    }

    #[test]
    fn lookup_is_case_insensitive() {
        let labels = labels();
        assert_eq!(
            labels
                .identity("bzbqfllbncxemglobhuvftluplvcpquassilfaboffbcadqssupnwlzbqexk")
                .unwrap()
                .name,
            "Exchange"
        );
        assert_eq!(labels.contract(1).unwrap().name, "QX");
        assert_eq!(
            labels
                .asset(
                    "bzbqfllbncxemglobhuvftluplvcpquassilfaboffbcadqssupnwlzbqexk",
                    "qx"
                )
                .unwrap()
                .name,
            "QX"
        );
    }

    #[test]
    fn search_matches_name_and_prefers_verified() {
        let labels = labels();
        let found = labels.search("qx", 10);
        assert_eq!(found.len(), 2);
        assert!(found[0].verified);
    }

    #[test]
    fn wallet_enrichment_labels_identity_and_nested_assets() {
        let labels = labels();
        let mut wallet = json!({
            "identity": "bzbqfllbncxemglobhuvftluplvcpquassilfaboffbcadqssupnwlzbqexk",
            "assets": {
                "issued": [{
                    "issuer": "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK",
                    "name": "QX"
                }]
            }
        });
        labels.enrich_wallet(
            &mut wallet,
            "bzbqfllbncxemglobhuvftluplvcpquassilfaboffbcadqssupnwlzbqexk",
        );
        assert_eq!(wallet["label"]["name"], "Exchange");
        assert_eq!(wallet["assets"]["issued"][0]["label"]["name"], "QX");
    }
}
