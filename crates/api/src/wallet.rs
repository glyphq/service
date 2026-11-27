//! Peer-backed wallet and asset-holder views.
//!
//! These routes join the local index with a direct, per-request Qubic peer
//! connection. The index supplies recent confirmed transactions while the peer
//! supplies the live spectrum entity and asset tree. Nothing is written back:
//! every route here is read-only.
//!
//! Routes:
//! - `GET /v1/wallets/{identity}` wallet entity, assets, and indexed transactions
//! - `GET /v1/wallets/{identity}/assets?kind=issued|owned|possessed`
//! - `GET /v1/live/wallets/{identity}` point-in-time peer snapshot
//! - `GET /v1/assets/{issuer}/{name}/holders?limit=` ownership holders

use std::collections::HashMap;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, header};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use node::{AssetRecordResponse, Peer};
use qubic::PublicKey;
use qubic::messages::{
    ASSET_REQUEST_ANY_OWNER, ASSET_REQUEST_ANY_OWNERSHIP_MANAGING_CONTRACT,
    ASSET_REQUEST_OWNERSHIP, AssetIssuanceRecord, AssetRecord, AssetRecordKind, RequestAssets,
    RespondIssuedAssets, RespondOwnedAssets, RespondPossessedAssets,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::dto::PageParams;
use crate::{ApiError, AppState};

const WALLET_FRESH: Duration = Duration::from_secs(60);
const ASSET_FRESH: Duration = Duration::from_secs(60);
const ASSET_FETCH_TIMEOUT: Duration = Duration::from_secs(2);
const CACHE_HEADER: &str = "public, max-age=30, stale-while-revalidate=60";

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/v1/wallets/{identity}", get(wallet))
        .route("/v1/wallets/{identity}/assets", get(wallet_assets))
        .route("/v1/identities/{identity}/assets", get(wallet_assets))
        .route("/v1/live/wallets/{identity}", get(wallet_snapshot))
        .route("/v1/assets/{issuer}/{name}/holders", get(asset_holders))
        .route("/v1/assets/{issuer}/{name}/transfers", get(asset_transfers))
}

#[derive(Debug, Deserialize)]
struct WalletAssetsQuery {
    #[serde(default)]
    kind: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HoldersQuery {
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Debug, Default, Deserialize)]
struct TransfersQuery {
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    offset: Option<u64>,
}

impl TransfersQuery {
    fn page(&self) -> PageParams {
        PageParams {
            limit: self.limit,
            cursor: self.cursor.clone(),
            offset: self.offset,
        }
    }
}

/// One ownership holder after ranking and truncation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct Holder {
    identity: String,
    shares: i64,
}

pub(crate) async fn connect(state: &AppState) -> Result<Peer, ApiError> {
    if state.peers.is_empty() {
        return Err(ApiError::unavailable(
            "no QUBIC_PEERS configured for peer-backed routes",
        ));
    }
    state
        .pool
        .connect_best()
        .await
        .map_err(|error| ApiError::unavailable(format!("peer connection failed: {error}")))
}

pub(crate) fn parse_identity(input: &str) -> Result<(String, PublicKey), ApiError> {
    let identity = input.trim().to_ascii_uppercase();
    let key = PublicKey::from_identity(&identity)
        .map_err(|error| ApiError::invalid_identity(error.to_string()))?;
    Ok((identity, key))
}

/// Normalises and validates a 1..=7 character asset name.
///
/// Qubic asset names are uppercase ASCII letters or digits. Input is
/// uppercased first so a lower-case path segment is still accepted.
pub(crate) fn normalize_asset_name(input: &str) -> Result<String, ApiError> {
    let name = input.trim().to_ascii_uppercase();
    if name.is_empty() || name.len() > 7 {
        return Err(ApiError::bad_request(
            "asset name must contain 1 to 7 characters",
        ));
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_digit() || byte.is_ascii_uppercase())
    {
        return Err(ApiError::bad_request(
            "asset name may only contain uppercase ASCII letters and digits",
        ));
    }
    Ok(name)
}

/// Encodes a validated 1..=7 character asset name exactly like core.
///
/// Core packs the raw ASCII bytes of the name into the little-endian bytes of
/// the `u64` asset name, zero-padding the remainder. The bytes are the
/// characters themselves, not an offset from `'0'`; decoding a peer record
/// yields the same ASCII bytes.
fn encode_asset_name(name: &str) -> Result<u64, ApiError> {
    let name = normalize_asset_name(name)?;
    let mut encoded = 0u64;
    for (index, byte) in name.bytes().enumerate() {
        encoded |= u64::from(byte) << (index * 8);
    }
    Ok(encoded)
}

fn identity_of(key: &[u8; 32]) -> String {
    PublicKey(*key).to_identity()
}

fn issuance_fields(record: &AssetIssuanceRecord) -> Value {
    json!({
        "issuer": identity_of(&record.public_key),
        "name": record.name(),
        "decimals": record.decimals,
        "unit": record.unit(),
    })
}

fn truncation_warning(entity: &crate::entity_cache::CachedEntity) -> Option<&'static str> {
    (entity.spectrum_index < 0)
        .then_some("identity has no spectrum entry on the peer; peer balances are unavailable")
}

fn issued_entry(response: &RespondIssuedAssets) -> Option<Value> {
    let AssetRecordKind::Issuance(record) = &response.asset.kind else {
        return None;
    };
    let mut entry = issuance_fields(record);
    entry["universe_index"] = json!(response.universe_index);
    entry["tick"] = json!(response.tick);
    Some(entry)
}

fn owned_entry(response: &RespondOwnedAssets) -> Option<Value> {
    let AssetRecordKind::Ownership(ownership) = &response.asset.kind else {
        return None;
    };
    let AssetRecordKind::Issuance(issuance) = &response.issuance_asset.kind else {
        return None;
    };
    let mut entry = issuance_fields(issuance);
    entry["owner"] = json!(identity_of(&ownership.public_key));
    entry["shares"] = json!(ownership.shares);
    entry["universe_index"] = json!(response.universe_index);
    entry["tick"] = json!(response.tick);
    entry["managing_contract_index"] = json!(ownership.managing_contract_index);
    entry["ownership_index"] = json!(ownership.issuance_index);
    entry["issuance"] = issuance_fields(issuance);
    Some(entry)
}

fn possessed_entry(response: &RespondPossessedAssets) -> Option<Value> {
    let AssetRecordKind::Possession(possession) = &response.asset.kind else {
        return None;
    };
    let AssetRecordKind::Issuance(issuance) = &response.issuance_asset.kind else {
        return None;
    };
    let ownership = match &response.ownership_asset.kind {
        AssetRecordKind::Ownership(ownership) => Some(ownership),
        _ => None,
    };
    let mut entry = issuance_fields(issuance);
    entry["possessor"] = json!(identity_of(&possession.public_key));
    if let Some(ownership) = ownership {
        entry["owner"] = json!(identity_of(&ownership.public_key));
        entry["ownership"] = json!({
            "owner": identity_of(&ownership.public_key),
            "shares": ownership.shares,
            "managing_contract_index": ownership.managing_contract_index,
            "ownership_index": ownership.issuance_index,
        });
    }
    entry["shares"] = json!(possession.shares);
    entry["universe_index"] = json!(response.universe_index);
    entry["tick"] = json!(response.tick);
    entry["managing_contract_index"] = json!(possession.managing_contract_index);
    entry["possession_index"] = json!(possession.ownership_index);
    Some(entry)
}

fn issued_list(responses: &[RespondIssuedAssets]) -> Vec<Value> {
    responses.iter().filter_map(issued_entry).collect()
}

fn owned_list(responses: &[RespondOwnedAssets]) -> Vec<Value> {
    responses.iter().filter_map(owned_entry).collect()
}

fn possessed_list(responses: &[RespondPossessedAssets]) -> Vec<Value> {
    responses.iter().filter_map(possessed_entry).collect()
}

/// Merges repeated identities, sorts by shares descending then identity, and
/// truncates to `limit`. The second value reports whether rows were dropped.
fn rank_holders(
    entries: impl IntoIterator<Item = (String, i64)>,
    limit: u32,
) -> (Vec<Holder>, bool) {
    let mut totals: HashMap<String, i64> = HashMap::new();
    for (identity, shares) in entries {
        // Peer records are untrusted, so repeated ownership rows must not be
        // able to overflow the accumulator and panic the request.
        let total = totals.entry(identity).or_insert(0);
        *total = total.saturating_add(shares);
    }
    let mut holders: Vec<Holder> = totals
        .into_iter()
        .map(|(identity, shares)| Holder { identity, shares })
        .collect();
    holders.sort_by(|a, b| {
        b.shares
            .cmp(&a.shares)
            .then_with(|| a.identity.cmp(&b.identity))
    });
    let truncated = holders.len() > limit as usize;
    holders.truncate(limit as usize);
    (holders, truncated)
}

fn ownership_holder(record: &AssetRecord) -> Option<(String, i64)> {
    match &record.kind {
        AssetRecordKind::Ownership(ownership) => {
            Some((identity_of(&ownership.public_key), ownership.shares))
        }
        _ => None,
    }
}

async fn wallet(
    State(state): State<AppState>,
    Path(identity): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let (identity, key) = parse_identity(&identity)?;
    if state.peers.is_empty() {
        return Err(ApiError::unavailable(
            "no QUBIC_PEERS configured for peer-backed routes",
        ));
    }
    let cache_key = wallet_cache_key(&identity);
    if let Some((mut value, age)) = state.live_cache.peek(&cache_key) {
        let peer_tick = value
            .get("meta")
            .and_then(|meta| meta.get("peer_tick"))
            .and_then(Value::as_u64);
        let behind = peer_tick.is_some_and(|tick| {
            state
                .dirty_entity_tick(&identity)
                .is_some_and(|dirty| tick < u64::from(dirty))
        });
        if !behind {
            let stale = age >= WALLET_FRESH;
            let refreshing = stale && spawn_wallet_refresh(state.clone(), identity.clone(), key);
            decorate_wallet_cache(&mut value, stale, age.as_secs(), refreshing);
            return Ok((
                [(
                    header::CACHE_CONTROL,
                    HeaderValue::from_static(CACHE_HEADER),
                )],
                Json(value),
            ));
        }
    }

    let (body, cacheable) = build_wallet(&state, &identity, key).await?;
    if cacheable {
        state.live_cache.put(cache_key, body.clone());
    }
    Ok((
        [(
            header::CACHE_CONTROL,
            HeaderValue::from_static(CACHE_HEADER),
        )],
        Json(body),
    ))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CachedAssets {
    issued: Vec<Value>,
    owned: Vec<Value>,
    possessed: Vec<Value>,
    observed_at: chrono::DateTime<chrono::Utc>,
}

impl CachedAssets {
    fn age_seconds(&self) -> u64 {
        (chrono::Utc::now() - self.observed_at).num_seconds().max(0) as u64
    }

    fn data(&self) -> Value {
        json!({
            "issued": self.issued,
            "owned": self.owned,
            "possessed": self.possessed,
        })
    }

    fn counts(&self) -> Value {
        json!({
            "issued": self.issued.len(),
            "owned": self.owned.len(),
            "possessed": self.possessed.len(),
        })
    }
}

struct AssetRead {
    assets: CachedAssets,
    stale: bool,
    refreshing: bool,
    warning: Option<String>,
    cacheable: bool,
}

pub(crate) fn wallet_cache_key(identity: &str) -> String {
    format!("wallet:{identity}")
}

fn assets_cache_key(identity: &str) -> String {
    format!("wallet-assets:{identity}")
}

fn decorate_wallet_cache(value: &mut Value, stale: bool, age_seconds: u64, refreshing: bool) {
    if let Some(meta) = value.get_mut("meta").and_then(Value::as_object_mut) {
        meta.insert("cached".into(), json!(true));
        meta.insert("stale".into(), json!(stale));
        meta.insert("age_seconds".into(), json!(age_seconds));
        meta.insert("refreshing".into(), json!(refreshing));
    }
}

pub(crate) async fn build_wallet(
    state: &AppState,
    identity: &str,
    key: PublicKey,
) -> Result<(Value, bool), ApiError> {
    let (local, entity, recent) = tokio::join!(
        state.store.identity(identity),
        crate::entity_cache::get_or_fetch(state, identity),
        state.store.transactions_for_identity(identity, 10, 0),
    );
    let local = local?;
    let entity = entity?;
    let recent = recent?;
    let warning = truncation_warning(&entity.entity);
    if warning.is_some() && local.is_none() {
        return Err(ApiError::not_found(format!(
            "identity {identity} is neither indexed nor in the peer spectrum"
        )));
    }

    let assets = load_assets(state, identity, key).await;
    let warning = warning
        .map(str::to_string)
        .or_else(|| assets.warning.clone());
    let mut body = json!({
        "data": {
            "identity": identity,
            "public_key": key.to_hex(),
            "entity": entity.entity.data(),
            "assets": assets.assets.data(),
            "recent": recent,
        },
        "meta": {
            "source": "peer+index",
            "peer_tick": entity.entity.tick,
            "entity_source": entity.meta.source,
            "entity_stale": entity.meta.stale,
            "entity_age_seconds": entity.meta.age_seconds,
            "assets_source": if assets.stale { "snapshot" } else { "peer" },
            "assets_stale": assets.stale,
            "assets_age_seconds": assets.assets.age_seconds(),
            "assets_refreshing": assets.refreshing,
            "cached": false,
            "stale": entity.meta.stale || assets.stale,
            "age_seconds": entity.meta.age_seconds.max(assets.assets.age_seconds()),
            "refreshing": entity.meta.refreshing || assets.refreshing,
            "warning": warning,
        }
    });
    if let Some(data) = body.get_mut("data") {
        state.labels.enrich_wallet(data, identity);
    }
    Ok((body, assets.cacheable))
}

fn spawn_wallet_refresh(state: AppState, identity: String, key: PublicKey) -> bool {
    let Some(guard) = state
        .refreshes
        .try_start(format!("wallet-refresh:{identity}"))
    else {
        return false;
    };
    tokio::spawn(async move {
        let _guard = guard;
        match build_wallet(&state, &identity, key).await {
            Ok((body, true)) => state.live_cache.put(wallet_cache_key(&identity), body),
            Ok((_, false)) => {}
            Err(error) => tracing::debug!(%identity, %error, "background wallet refresh failed"),
        }
    });
    true
}

async fn fetch_assets(state: &AppState, key: PublicKey) -> Result<CachedAssets, node::Error> {
    let (issued, owned, possessed) = tokio::join!(
        state.pool.issued_assets(key.0),
        state.pool.owned_assets(key.0),
        state.pool.possessed_assets(key.0),
    );
    Ok(CachedAssets {
        issued: issued_list(&issued?),
        owned: owned_list(&owned?),
        possessed: possessed_list(&possessed?),
        observed_at: chrono::Utc::now(),
    })
}

async fn load_assets(state: &AppState, identity: &str, key: PublicKey) -> AssetRead {
    let cache_key = assets_cache_key(identity);
    if let Some((value, age)) = state.live_cache.peek(&cache_key) {
        if let Ok(assets) = serde_json::from_value::<CachedAssets>(value) {
            if age < ASSET_FRESH {
                return AssetRead {
                    assets,
                    stale: false,
                    refreshing: false,
                    warning: None,
                    cacheable: true,
                };
            }
            let refreshing = spawn_assets_refresh(state.clone(), identity.to_string(), key);
            return AssetRead {
                assets,
                stale: true,
                refreshing,
                warning: None,
                cacheable: true,
            };
        }
    }

    match tokio::time::timeout(ASSET_FETCH_TIMEOUT, fetch_assets(state, key)).await {
        Ok(Ok(assets)) => {
            state.live_cache.put(
                cache_key,
                serde_json::to_value(&assets).unwrap_or(Value::Null),
            );
            AssetRead {
                assets,
                stale: false,
                refreshing: false,
                warning: None,
                cacheable: true,
            }
        }
        Ok(Err(error)) => AssetRead {
            assets: empty_assets(),
            stale: true,
            refreshing: false,
            warning: Some(format!("asset lookup failed: {error}")),
            cacheable: false,
        },
        Err(_) => AssetRead {
            assets: empty_assets(),
            stale: true,
            refreshing: false,
            warning: Some("asset lookup timed out".into()),
            cacheable: false,
        },
    }
}

fn empty_assets() -> CachedAssets {
    CachedAssets {
        issued: Vec::new(),
        owned: Vec::new(),
        possessed: Vec::new(),
        observed_at: chrono::Utc::now(),
    }
}

fn spawn_assets_refresh(state: AppState, identity: String, key: PublicKey) -> bool {
    let Some(guard) = state
        .refreshes
        .try_start(format!("wallet-assets-refresh:{identity}"))
    else {
        return false;
    };
    tokio::spawn(async move {
        let _guard = guard;
        match fetch_assets(&state, key).await {
            Ok(assets) => state.live_cache.put(
                assets_cache_key(&identity),
                serde_json::to_value(&assets).unwrap_or(Value::Null),
            ),
            Err(error) => tracing::debug!(%identity, %error, "background asset refresh failed"),
        }
    });
    true
}

async fn wallet_assets(
    State(state): State<AppState>,
    Path(identity): Path<String>,
    Query(params): Query<WalletAssetsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let (identity, key) = parse_identity(&identity)?;
    let kind = match params.kind.as_deref() {
        None | Some("") | Some("all") => "all",
        Some("issued") => "issued",
        Some("owned") => "owned",
        Some("possessed") => "possessed",
        Some(other) => {
            return Err(ApiError::bad_request(format!(
                "unknown asset kind {other:?}; expected issued, owned, possessed, or all"
            )));
        }
    };
    if state.peers.is_empty() {
        return Err(ApiError::unavailable(
            "no QUBIC_PEERS configured for peer-backed routes",
        ));
    }

    let assets = load_assets(&state, &identity, key).await;
    let issued = if matches!(kind, "all" | "issued") {
        assets.assets.issued.clone()
    } else {
        Vec::new()
    };
    let owned = if matches!(kind, "all" | "owned") {
        assets.assets.owned.clone()
    } else {
        Vec::new()
    };
    let possessed = if matches!(kind, "all" | "possessed") {
        assets.assets.possessed.clone()
    } else {
        Vec::new()
    };

    Ok((
        [(
            header::CACHE_CONTROL,
            HeaderValue::from_static(CACHE_HEADER),
        )],
        Json(json!({
            "data": {
                "identity": identity,
                "issued": issued,
                "owned": owned,
                "possessed": possessed,
            },
            "meta": {
                "source": if assets.stale { "snapshot" } else { "peer" },
                "kind": kind,
                "stale": assets.stale,
                "age_seconds": assets.assets.age_seconds(),
                "refreshing": assets.refreshing,
                "counts": {
                    "issued": issued.len(),
                    "owned": owned.len(),
                    "possessed": possessed.len(),
                },
                "warning": assets.warning,
            }
        })),
    ))
}

async fn wallet_snapshot(
    State(state): State<AppState>,
    Path(identity): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let (identity, key) = parse_identity(&identity)?;
    if state.peers.is_empty() {
        return Err(ApiError::unavailable(
            "no QUBIC_PEERS configured for peer-backed routes",
        ));
    }
    let (entity, assets, head) = tokio::join!(
        crate::entity_cache::get_or_fetch(&state, &identity),
        load_assets(&state, &identity, key),
        tokio::time::timeout(Duration::from_millis(750), state.pool.head()),
    );
    let entity = entity?;
    let warning = truncation_warning(&entity.entity)
        .map(str::to_string)
        .or_else(|| assets.warning.clone());
    let epoch = head.ok().and_then(Result::ok).map(|head| head.epoch);
    let peer_tick = entity.entity.tick;

    Ok((
        [(
            header::CACHE_CONTROL,
            HeaderValue::from_static(CACHE_HEADER),
        )],
        Json(json!({
            "data": {
                "identity": identity,
                "public_key": key.to_hex(),
                "peer_tick": peer_tick,
                "epoch": epoch,
                "entity": entity.entity.data(),
                "assets": assets.assets.data(),
                "counts": assets.assets.counts(),
            },
            "meta": {
                "source": entity.meta.source,
                "peer_tick": peer_tick,
                "epoch": epoch,
                "stale": entity.meta.stale || assets.stale,
                "age_seconds": entity.meta.age_seconds.max(assets.assets.age_seconds()),
                "refreshing": entity.meta.refreshing || assets.refreshing,
                "assets_source": if assets.stale { "snapshot" } else { "peer" },
                "warning": warning,
            }
        })),
    ))
}

async fn asset_holders(
    State(state): State<AppState>,
    Path((issuer, name)): Path<(String, String)>,
    Query(params): Query<HoldersQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let (issuer, _) = parse_identity(&issuer)?;
    let name = normalize_asset_name(&name)?;
    let limit = params.limit.unwrap_or(100).clamp(1, 1000);
    let (holders, truncated) = holders_for(&state, &issuer, &name, limit).await?;

    Ok((
        [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(json!({
            "data": holders,
            "meta": {
                "issuer": issuer,
                "name": name,
                "count": holders.len(),
                "limit": limit,
                "source": "peer",
                "truncated": truncated,
            }
        })),
    ))
}

/// Ownership holders for an asset, ranked by shares.
pub(crate) async fn holders_for(
    state: &AppState,
    issuer: &str,
    name: &str,
    limit: u32,
) -> Result<(Vec<Holder>, bool), ApiError> {
    let (_, issuer_key) = parse_identity(issuer)?;
    let asset_name = encode_asset_name(name)?;
    let request = RequestAssets::ByFilter {
        request_type: ASSET_REQUEST_OWNERSHIP,
        flags: ASSET_REQUEST_ANY_OWNER | ASSET_REQUEST_ANY_OWNERSHIP_MANAGING_CONTRACT,
        ownership_managing_contract: 0,
        possession_managing_contract: 0,
        issuer: issuer_key.0,
        asset_name,
        owner: [0u8; 32],
        possessor: [0u8; 32],
    };
    let mut peer = connect(state).await?;
    let records = peer
        .asset_records(&request)
        .await
        .map_err(|error| ApiError::unavailable(format!("asset holder lookup failed: {error}")))?;
    let entries = records
        .iter()
        .map(AssetRecordResponse::record)
        .filter_map(ownership_holder);
    Ok(rank_holders(entries, limit))
}

/// Ownership and possession transfers for one asset, newest first.
///
/// The local index records every decoded `asset_ownership_change` and
/// `asset_possession_change` log, so this works even when no peer is
/// configured and without rebuilding the full holder set.
async fn asset_transfers(
    State(state): State<AppState>,
    Path((issuer, name)): Path<(String, String)>,
    Query(params): Query<TransfersQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let (issuer, _) = parse_identity(&issuer)?;
    let name = normalize_asset_name(&name)?;
    let kind = match params.kind.as_deref() {
        None | Some("") | Some("all") => "all",
        Some("ownership") => "ownership",
        Some("possession") => "possession",
        Some(other) => {
            return Err(ApiError::bad_request(format!(
                "unknown transfer kind {other:?}; expected ownership, possession, or all"
            )));
        }
    };
    let page = params
        .page()
        .resolve(&format!("assets/{issuer}/{name}/transfers?kind={kind}"))?;
    let mut rows = state
        .store
        .asset_transfers(
            &issuer,
            &name,
            (kind != "all").then_some(kind),
            page.fetch_limit(),
            page.offset,
        )
        .await?;
    let has_more = rows.len() > page.limit as usize;
    rows.truncate(page.limit as usize);
    Ok((
        [(
            header::CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=5, stale-while-revalidate=10"),
        )],
        Json(json!({
            "data": rows,
            "meta": {
                "issuer": issuer,
                "name": name,
                "kind": kind,
                "source": "index",
                "limit": page.limit,
                "offset": page.offset,
                "count": rows.len(),
                "has_more": has_more,
                "next_cursor": has_more.then(|| crate::dto::encode_cursor(
                    &format!("assets/{issuer}/{name}/transfers?kind={kind}"),
                    page.offset + rows.len() as u64,
                )),
            }
        })),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use qubic::messages::{
        ASSET_NAME_LEN, ASSET_UNIT_LEN, AssetOwnershipRecord, AssetPossessionRecord,
    };

    const ISSUER: [u8; 32] = [1u8; 32];
    const OWNER: [u8; 32] = [2u8; 32];
    const POSSESSOR: [u8; 32] = [3u8; 32];

    fn name_bytes(name: &str) -> [u8; ASSET_NAME_LEN] {
        let mut bytes = [0u8; ASSET_NAME_LEN];
        bytes[..name.len()].copy_from_slice(name.as_bytes());
        bytes
    }

    fn issuance_record(name: &str) -> AssetRecord {
        AssetRecord {
            public_key: ISSUER,
            kind: AssetRecordKind::Issuance(AssetIssuanceRecord {
                public_key: ISSUER,
                name: name_bytes(name),
                decimals: 2,
                unit: [0u8; ASSET_UNIT_LEN],
            }),
        }
    }

    #[test]
    fn asset_name_encoding_matches_core() {
        assert_eq!(encode_asset_name("0").unwrap(), 0x30);
        assert_eq!(encode_asset_name("A").unwrap(), 0x41);
        assert_eq!(encode_asset_name("QX").unwrap(), 0x5851);
        assert_eq!(
            encode_asset_name("ABCDEFG").unwrap(),
            (0x41u64)
                | (0x42u64 << 8)
                | (0x43u64 << 16)
                | (0x44u64 << 24)
                | (0x45u64 << 32)
                | (0x46u64 << 40)
                | (0x47u64 << 48)
        );
        // The packed name is the ASCII bytes a peer decodes back into a name.
        let name = encode_asset_name("QCAP").unwrap().to_le_bytes();
        assert_eq!(qubic::messages::asset_name_string(&name), "QCAP");
        // Lower-case input is normalised before encoding.
        assert_eq!(encode_asset_name("qx").unwrap(), 0x5851);
    }

    #[test]
    fn asset_name_validation_rejects_bad_shapes() {
        assert!(encode_asset_name("").is_err());
        assert!(encode_asset_name("ABCDEFGH").is_err());
        assert!(encode_asset_name("AB-CD").is_err());
        assert!(encode_asset_name("A:B").is_err());
    }

    #[test]
    fn owned_entry_maps_owner_and_issuance() {
        let response = RespondOwnedAssets {
            asset: AssetRecord {
                public_key: OWNER,
                kind: AssetRecordKind::Ownership(AssetOwnershipRecord {
                    public_key: OWNER,
                    managing_contract_index: 7,
                    issuance_index: 9,
                    shares: 1_000,
                }),
            },
            issuance_asset: issuance_record("QX"),
            tick: 42,
            universe_index: 11,
            siblings: Vec::new(),
        };
        let entry = owned_entry(&response).expect("ownership response maps");
        assert_eq!(entry["owner"], json!(identity_of(&OWNER)));
        assert_eq!(entry["issuer"], json!(identity_of(&ISSUER)));
        assert_eq!(entry["name"], json!("QX"));
        assert_eq!(entry["decimals"], json!(2));
        assert_eq!(entry["shares"], json!(1_000));
        assert_eq!(entry["universe_index"], json!(11));
        assert_eq!(entry["tick"], json!(42));
        assert_eq!(entry["issuance"]["name"], json!("QX"));
    }

    #[test]
    fn possessed_entry_maps_ownership_and_issuance() {
        let response = RespondPossessedAssets {
            asset: AssetRecord {
                public_key: POSSESSOR,
                kind: AssetRecordKind::Possession(AssetPossessionRecord {
                    public_key: POSSESSOR,
                    managing_contract_index: 3,
                    ownership_index: 5,
                    shares: 250,
                }),
            },
            ownership_asset: AssetRecord {
                public_key: OWNER,
                kind: AssetRecordKind::Ownership(AssetOwnershipRecord {
                    public_key: OWNER,
                    managing_contract_index: 7,
                    issuance_index: 9,
                    shares: 1_000,
                }),
            },
            issuance_asset: issuance_record("QX"),
            tick: 42,
            universe_index: 11,
            siblings: Vec::new(),
        };
        let entry = possessed_entry(&response).expect("possession response maps");
        assert_eq!(entry["possessor"], json!(identity_of(&POSSESSOR)));
        assert_eq!(entry["owner"], json!(identity_of(&OWNER)));
        assert_eq!(entry["shares"], json!(250));
        assert_eq!(entry["ownership"]["shares"], json!(1_000));
        assert_eq!(entry["tick"], json!(42));
        assert_eq!(entry["universe_index"], json!(11));
    }

    #[test]
    fn issued_entry_rejects_non_issuance_records() {
        let response = RespondIssuedAssets {
            asset: AssetRecord {
                public_key: OWNER,
                kind: AssetRecordKind::Ownership(AssetOwnershipRecord {
                    public_key: OWNER,
                    managing_contract_index: 0,
                    issuance_index: 0,
                    shares: 1,
                }),
            },
            tick: 1,
            universe_index: 2,
            siblings: Vec::new(),
        };
        assert!(issued_entry(&response).is_none());
    }

    #[test]
    fn holders_sort_dedupe_and_limit() {
        let entries = vec![
            ("B".to_string(), 10),
            ("A".to_string(), 10),
            ("B".to_string(), 5),
            ("C".to_string(), 100),
            ("D".to_string(), -1),
        ];
        let (holders, truncated) = rank_holders(entries, 3);
        assert!(truncated);
        assert_eq!(
            holders,
            vec![
                Holder {
                    identity: "C".into(),
                    shares: 100
                },
                Holder {
                    identity: "B".into(),
                    shares: 15
                },
                Holder {
                    identity: "A".into(),
                    shares: 10
                },
            ]
        );

        let (holders, truncated) =
            rank_holders(vec![("B".to_string(), 10), ("A".to_string(), 10)], 100);
        assert!(!truncated);
        assert_eq!(holders[0].identity, "A");
        assert_eq!(holders[1].identity, "B");
    }

    #[test]
    fn holders_limit_one_reports_truncation() {
        let (holders, truncated) =
            rank_holders(vec![("A".to_string(), 2), ("B".to_string(), 1)], 1);
        assert_eq!(holders.len(), 1);
        assert_eq!(holders[0].identity, "A");
        assert!(truncated);
    }

    #[test]
    fn holders_saturate_instead_of_overflowing() {
        let (holders, _) = rank_holders(
            vec![
                ("A".to_string(), i64::MAX),
                ("A".to_string(), 1),
                ("B".to_string(), i64::MIN),
                ("B".to_string(), -1),
            ],
            10,
        );
        assert_eq!(holders[0].identity, "A");
        assert_eq!(holders[0].shares, i64::MAX);
        assert_eq!(holders[1].identity, "B");
        assert_eq!(holders[1].shares, i64::MIN);
    }
}
