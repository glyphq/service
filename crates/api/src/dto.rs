use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use store::EventRow;
use store::TransactionRow;

use crate::ApiError;

pub const DEFAULT_LIMIT: u32 = 50;
pub const MAX_LIMIT: u32 = 200;
const CURSOR_VERSION: u8 = 1;

/// Serializes a transaction with both the indexed tick and the explicit target
/// tick. Qubic executes a transaction in the tick named by its header, so the
/// two values match for indexed rows.
pub fn transaction_value(row: &TransactionRow) -> Value {
    let mut value = serde_json::to_value(row).unwrap_or(Value::Null);
    if let Some(object) = value.as_object_mut() {
        object.insert("target_tick".into(), json!(row.tick));
    }
    value
}

/// Query parameters shared by every collection.
///
/// `cursor` is the preferred form. `offset` remains accepted while clients
/// migrate; a cursor always wins when both are present.
#[derive(Debug, Default, Deserialize)]
pub struct PageParams {
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub offset: Option<u64>,
}

/// A resolved page request bound to one collection fingerprint.
pub struct Page {
    pub limit: u32,
    pub offset: u64,
    fingerprint: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct CursorPayload {
    v: u8,
    f: String,
    k: Vec<String>,
}

impl PageParams {
    /// Resolves limit and offset. `fingerprint` identifies the route and its
    /// normalized filters so a cursor cannot be replayed with a different
    /// query.
    pub fn resolve(&self, fingerprint: &str) -> Result<Page, ApiError> {
        let limit = self.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
        let offset = match &self.cursor {
            Some(token) => decode_cursor(token, fingerprint)?,
            None => self.offset.unwrap_or(0),
        };
        Ok(Page {
            limit,
            offset,
            fingerprint: fingerprint.to_owned(),
        })
    }
}

impl Page {
    /// Number of rows to request so `has_more` can be detected without a count
    /// query.
    pub fn fetch_limit(&self) -> u32 {
        self.limit.saturating_add(1)
    }

    pub fn meta(&self, returned: usize, has_more: bool) -> Value {
        json!({
            "limit": self.limit,
            "offset": self.offset,
            "count": returned,
            "next_offset": if has_more { Some(self.offset + returned as u64) } else { None },
            "next_cursor": if has_more {
                Some(encode_cursor(&self.fingerprint, self.offset + returned as u64))
            } else {
                None
            },
            "has_more": has_more,
        })
    }
}

/// Encodes an opaque, URL-safe cursor. The payload is not signed; it only
/// carries the collection fingerprint and the next offset.
pub fn encode_cursor(fingerprint: &str, offset: u64) -> String {
    let payload = CursorPayload {
        v: CURSOR_VERSION,
        f: fingerprint.to_owned(),
        k: vec![offset.to_string()],
    };
    let bytes = serde_json::to_vec(&payload).expect("cursor payload is serializable");
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Decodes a cursor and rejects version, fingerprint, or shape mismatches.
pub fn decode_cursor(token: &str, fingerprint: &str) -> Result<u64, ApiError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|_| ApiError::invalid_cursor("cursor is not valid base64url"))?;
    let payload: CursorPayload = serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::invalid_cursor("cursor payload is malformed"))?;
    if payload.v != CURSOR_VERSION {
        return Err(ApiError::invalid_cursor(format!(
            "unsupported cursor version {}",
            payload.v
        )));
    }
    if payload.f != fingerprint {
        return Err(ApiError::invalid_cursor(
            "cursor does not belong to this collection or filter set",
        ));
    }
    let offset = payload
        .k
        .first()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| ApiError::invalid_cursor("cursor offset is missing"))?;
    Ok(offset)
}

/// Store rows keep the decoded event body as a JSON string so the schema stays
/// flat and ClickHouse-friendly. The public API promotes it back to an object.
pub fn event_value(row: EventRow) -> Value {
    let mut value = serde_json::to_value(row).expect("EventRow is serializable");
    if let Some(body) = value.get("body").and_then(Value::as_str) {
        if let Ok(mut parsed) = serde_json::from_str::<Value>(body) {
            normalize_event_body(&mut parsed);
            value["body"] = parsed;
        }
    }
    value
}

/// Older event rows were written with camelCase keys and string-encoded
/// amounts. Both are normalized on read so the public JSON keeps the documented
/// snake_case and integer conventions without a full re-index.
fn normalize_event_body(value: &mut Value) {
    match value {
        Value::Object(map) => {
            let entries = std::mem::take(map);
            for (key, mut value) in entries {
                normalize_event_body(&mut value);
                let key = snake_case_key(&key);
                map.insert(key.clone(), normalize_event_scalar(&key, value));
            }
        }
        Value::Array(items) => {
            for item in items {
                normalize_event_body(item);
            }
        }
        _ => {}
    }
}

fn snake_case_key(key: &str) -> String {
    let mut out = String::with_capacity(key.len() + 4);
    for (index, ch) in key.char_indices() {
        if ch.is_ascii_uppercase() {
            if index > 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

fn normalize_event_scalar(key: &str, value: Value) -> Value {
    if !matches!(key, "amount" | "shares") {
        return value;
    }
    let Value::String(text) = &value else {
        return value;
    };
    // Only convert canonical decimal integers; leave hex data and hashes alone.
    if !text.is_empty()
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'-')
        && let Ok(number) = text.parse::<i64>()
    {
        return json!(number);
    }
    value
}

pub fn resource_meta() -> Value {
    json!({})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trips() {
        let token = encode_cursor("ticks?epoch=240", 500);
        assert_eq!(decode_cursor(&token, "ticks?epoch=240").unwrap(), 500);
    }

    #[test]
    fn cursor_rejects_a_different_filter_set() {
        let token = encode_cursor("ticks?epoch=240", 500);
        let error = decode_cursor(&token, "ticks?epoch=241").unwrap_err();
        assert_eq!(error.code, "invalid_cursor");
    }

    #[test]
    fn cursor_rejects_garbage() {
        assert!(decode_cursor("not-a-cursor", "ticks").is_err());
    }
}
