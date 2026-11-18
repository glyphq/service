use serde::Deserialize;
use serde_json::{Value, json};
use store::EventRow;

pub const DEFAULT_LIMIT: u32 = 50;
pub const MAX_LIMIT: u32 = 200;

#[derive(Debug, Deserialize)]
pub struct PageParams {
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub offset: Option<u64>,
}

impl PageParams {
    pub fn limit(&self) -> u32 {
        self.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
    }

    pub fn offset(&self) -> u64 {
        self.offset.unwrap_or(0)
    }
}

pub fn page_meta(limit: u32, offset: u64, returned: usize, has_more: bool) -> Value {
    json!({
        "limit": limit,
        "offset": offset,
        "count": returned,
        "nextOffset": if has_more { Some(offset + returned as u64) } else { None },
        "hasMore": has_more,
    })
}

pub fn resource_meta() -> Value {
    json!({})
}

/// Store rows keep the decoded event body as a JSON string so the schema stays
/// flat and ClickHouse-friendly. The public API promotes it back to an object.
pub fn event_value(row: EventRow) -> Value {
    let mut value = serde_json::to_value(row).expect("EventRow is serializable");
    if let Some(body) = value.get("body").and_then(Value::as_str) {
        if let Ok(parsed) = serde_json::from_str::<Value>(body) {
            value["body"] = parsed;
        }
    }
    value
}
