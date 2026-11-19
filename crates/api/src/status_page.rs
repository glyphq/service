//! Server-rendered status dashboard.
//!
//! `GET /status` is intentionally dependency-free: no JavaScript, external
//! stylesheets, fonts, or client-side fetching. The HTML is rendered from a
//! small [`StatusView`] so escaping and formatting can be unit-tested without
//! a live store.

use axum::Router;
use axum::extract::State;
use axum::http::header;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use chrono::{DateTime, SecondsFormat, Utc};
use store::{IngestCursor, NetworkStats};

use crate::AppState;

pub fn router() -> Router<AppState> {
    Router::new().route("/status", get(page))
}

async fn page(State(state): State<AppState>) -> Response {
    let (stats, cursor) = tokio::join!(state.store.network_stats(), state.store.ingest_cursor());
    let view = StatusView::from_state(&state, stats, cursor, Utc::now());
    ([(header::CACHE_CONTROL, "no-store")], Html(render(&view))).into_response()
}

/// Everything the renderer needs. Built once per request, then rendered
/// without touching the clock or the store so rendering stays deterministic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusView {
    pub version: String,
    pub uptime_seconds: u64,
    pub head: Option<HeadView>,
    pub latest_timestamp: Option<String>,
    pub lag_seconds: Option<i64>,
    pub index_lag_ticks: Option<i64>,
    pub counts: IndexCounts,
    pub ingest: IngestView,
    pub peers_configured: usize,
    pub streaming_enabled: bool,
    pub broadcast_enabled: bool,
    pub archive_enabled: bool,
    pub storage_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadView {
    pub epoch: u16,
    pub tick: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexCounts {
    pub epochs: u64,
    pub ticks: u64,
    pub transactions: u64,
    pub events: u64,
    pub identities: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IngestView {
    pub last_indexed_tick: Option<u32>,
    pub epoch: Option<u16>,
    pub verification: Option<String>,
    pub updated_at: Option<String>,
}

impl Default for StatusView {
    fn default() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            uptime_seconds: 0,
            head: None,
            latest_timestamp: None,
            lag_seconds: None,
            index_lag_ticks: None,
            counts: IndexCounts::default(),
            ingest: IngestView::default(),
            peers_configured: 0,
            streaming_enabled: false,
            broadcast_enabled: false,
            archive_enabled: false,
            storage_error: None,
        }
    }
}

impl StatusView {
    fn from_state(
        state: &AppState,
        stats: store::Result<NetworkStats>,
        cursor: store::Result<IngestCursor>,
        now: DateTime<Utc>,
    ) -> Self {
        let mut view = Self {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            uptime_seconds: state.uptime_seconds(),
            peers_configured: state.peers.len(),
            streaming_enabled: state.hub.is_some(),
            broadcast_enabled: state.allow_broadcast,
            archive_enabled: state.archive.is_some(),
            ..Self::default()
        };

        let mut errors = Vec::new();
        let mut stats_ingest = None;

        match stats {
            Ok(stats) => {
                let head_timestamp = stats.head.as_ref().map(|head| head.timestamp);
                view.head = stats.head.map(|head| HeadView {
                    epoch: head.epoch,
                    tick: head.tick,
                });

                let latest_timestamp = stats.latest_timestamp.or(head_timestamp);
                view.latest_timestamp = latest_timestamp.map(format_timestamp);
                view.lag_seconds = latest_timestamp
                    .map(|timestamp| now.signed_duration_since(timestamp).num_seconds().max(0));

                view.counts = IndexCounts {
                    epochs: stats.epochs,
                    ticks: stats.ticks,
                    transactions: stats.transactions,
                    events: stats.events,
                    identities: stats.identities,
                };
                stats_ingest = Some(stats.ingest);
            }
            Err(error) => errors.push(format!("network stats unavailable: {error}")),
        }

        match cursor {
            Ok(cursor) => view.ingest = IngestView::from(cursor),
            Err(error) => {
                // A failed cursor read should not hide the cursor already
                // embedded in the network stats response.
                if let Some(fallback) = stats_ingest {
                    view.ingest = IngestView::from(fallback);
                }
                errors.push(format!("ingest cursor unavailable: {error}"));
            }
        }

        view.index_lag_ticks = match (view.head.as_ref(), view.ingest.last_indexed_tick) {
            (Some(head), Some(last_indexed)) => {
                Some(i64::from(head.tick) - i64::from(last_indexed))
            }
            _ => None,
        };
        view.storage_error = (!errors.is_empty()).then(|| errors.join("; "));
        view
    }
}

impl From<IngestCursor> for IngestView {
    fn from(cursor: IngestCursor) -> Self {
        Self {
            last_indexed_tick: cursor.last_indexed_tick,
            epoch: cursor.epoch,
            verification: cursor.verification,
            updated_at: cursor.updated_at.map(format_timestamp),
        }
    }
}

fn format_timestamp(timestamp: DateTime<Utc>) -> String {
    timestamp.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Formats an uptime or lag as days, hours, minutes, and seconds.
fn format_uptime(seconds: u64) -> String {
    let days = seconds / 86_400;
    let hours = (seconds % 86_400) / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let secs = seconds % 60;
    format!("{days}d {hours:02}h {minutes:02}m {secs:02}s")
}

fn format_lag(seconds: i64) -> String {
    let seconds = seconds.max(0) as u64;
    if seconds < 60 {
        format!("{seconds}s")
    } else {
        format_uptime(seconds)
    }
}

/// Escapes text before it is placed in HTML. Every dynamic value in the
/// renderer passes through this function.
fn escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(character),
        }
    }
    escaped
}

fn display_option(value: Option<&str>, missing: &str) -> String {
    match value {
        Some(value) => escape(value),
        None => format!("<span class=\"muted\">{}</span>", escape(missing)),
    }
}

fn number(value: u64) -> String {
    value.to_string()
}

fn badge(enabled: bool) -> String {
    if enabled {
        "<span class=\"badge\">enabled</span>".to_owned()
    } else {
        "<span class=\"badge off\">disabled</span>".to_owned()
    }
}

fn push_row(out: &mut String, label: &str, value_html: &str) {
    out.push_str("<tr><th scope=\"row\">");
    out.push_str(&escape(label));
    out.push_str("</th><td>");
    out.push_str(value_html);
    out.push_str("</td></tr>");
}

const STYLE: &str = r#":root{color-scheme:light dark;--bg:#f6f7f9;--fg:#18181b;--muted:#52525b;--panel:#fff;--border:#d4d4d8;--bad:#b91c1c;--good:#166534;--link:#1d4ed8}
@media (prefers-color-scheme:dark){:root{--bg:#0b0b0d;--fg:#f4f4f5;--muted:#a1a1aa;--panel:#161618;--border:#2e2e33;--bad:#fca5a5;--good:#86efac;--link:#93c5fd}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.5 ui-sans-serif,system-ui,-apple-system,"Segoe UI",sans-serif}
main{max-width:60rem;margin:0 auto;padding:1.25rem}
header{display:flex;flex-wrap:wrap;gap:.5rem 1rem;align-items:baseline;justify-content:space-between;margin-bottom:1rem}
h1{font-size:1.35rem;margin:0}
h2{font-size:1rem;margin:0 0 .5rem}
p{margin:.25rem 0}
.muted{color:var(--muted)}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(17rem,1fr));gap:1rem}
section{background:var(--panel);border:1px solid var(--border);border-radius:.6rem;padding:1rem;min-width:0}
table{width:100%;border-collapse:collapse}
th,td{text-align:left;vertical-align:top;padding:.35rem 0;border-bottom:1px solid var(--border)}
tr:last-child th,tr:last-child td{border-bottom:0}
th{font-weight:500;color:var(--muted);white-space:nowrap;padding-right:1rem}
td{overflow-wrap:anywhere}
.alert{border:1px solid var(--bad);border-left-width:.35rem;border-radius:.5rem;padding:.75rem 1rem;margin-bottom:1rem;background:rgba(185,28,28,.08)}
.alert strong{color:var(--bad)}
.badge{display:inline-block;border-radius:999px;padding:.05rem .5rem;font-size:.8rem;border:1px solid currentColor;color:var(--good)}
.badge.off{color:var(--muted)}
nav{display:flex;flex-wrap:wrap;gap:.5rem 1rem;margin-top:1rem}
a{color:var(--link)}
footer{margin-top:1rem;color:var(--muted);font-size:.85rem}
@media (max-width:30rem){th{display:block;padding-bottom:0}td{display:block;padding-top:0}tr{display:block;border-bottom:1px solid var(--border);padding:.35rem 0}tr:last-child{border-bottom:0}th,td{border-bottom:0}}"#;

fn render(view: &StatusView) -> String {
    let mut html = String::with_capacity(8_192);
    html.push_str("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">");
    html.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">");
    html.push_str("<meta http-equiv=\"refresh\" content=\"5\">");
    html.push_str("<link rel=\"icon\" href=\"/favicon.png\">");
    html.push_str("<title>Glyph API Status</title><style>");
    html.push_str(STYLE);
    html.push_str("</style></head><body><main>");

    html.push_str("<header><div><h1>Glyph API Status</h1>");
    html.push_str(
        "<p class=\"muted\">Server-rendered dashboard, refreshes every 5 seconds.</p></div>",
    );
    html.push_str("<span class=\"muted\">v");
    html.push_str(&escape(&view.version));
    html.push_str("</span></header>");

    if let Some(error) = &view.storage_error {
        html.push_str("<div class=\"alert\" role=\"alert\"><strong>Storage error</strong><p>");
        html.push_str(&escape(error));
        html.push_str("</p><p class=\"muted\">Counts below may be stale or zero until storage recovers.</p></div>");
    }

    html.push_str("<div class=\"grid\">");

    html.push_str("<section><h2>Overview</h2><table><tbody>");
    push_row(&mut html, "Version", &escape(&view.version));
    push_row(
        &mut html,
        "Uptime",
        &escape(&format_uptime(view.uptime_seconds)),
    );
    let epoch = view
        .head
        .as_ref()
        .map(|head| head.epoch.to_string())
        .unwrap_or_else(|| "<span class=\"muted\">no indexed head</span>".to_owned());
    push_row(&mut html, "Current epoch", &epoch);
    let tick = view
        .head
        .as_ref()
        .map(|head| head.tick.to_string())
        .unwrap_or_else(|| "<span class=\"muted\">no indexed head</span>".to_owned());
    push_row(&mut html, "Current tick", &tick);
    push_row(
        &mut html,
        "Latest timestamp",
        &display_option(view.latest_timestamp.as_deref(), "not reported"),
    );
    let lag = view
        .lag_seconds
        .map(format_lag)
        .unwrap_or_else(|| "not derivable".to_owned());
    push_row(
        &mut html,
        "Data lag",
        &display_option(Some(&lag), "not derivable"),
    );
    let index_lag = view
        .index_lag_ticks
        .map(|ticks| format!("{ticks} ticks"))
        .unwrap_or_else(|| "not derivable".to_owned());
    push_row(
        &mut html,
        "Index lag",
        &display_option(Some(&index_lag), "not derivable"),
    );
    html.push_str("</tbody></table></section>");

    html.push_str("<section><h2>Indexed data</h2><table><tbody>");
    push_row(&mut html, "Epochs", &number(view.counts.epochs));
    push_row(&mut html, "Ticks", &number(view.counts.ticks));
    push_row(&mut html, "Transactions", &number(view.counts.transactions));
    push_row(&mut html, "Events", &number(view.counts.events));
    push_row(&mut html, "Identities", &number(view.counts.identities));
    html.push_str("</tbody></table></section>");

    html.push_str("<section><h2>Ingest cursor</h2><table><tbody>");
    let last_indexed = view
        .ingest
        .last_indexed_tick
        .map(|tick| tick.to_string())
        .unwrap_or_else(|| "not reported".to_owned());
    push_row(
        &mut html,
        "Last indexed tick",
        &display_option(Some(&last_indexed), "not reported"),
    );
    let ingest_epoch = view
        .ingest
        .epoch
        .map(|epoch| epoch.to_string())
        .unwrap_or_else(|| "not reported".to_owned());
    push_row(
        &mut html,
        "Epoch",
        &display_option(Some(&ingest_epoch), "not reported"),
    );
    push_row(
        &mut html,
        "Verification",
        &display_option(view.ingest.verification.as_deref(), "not reported"),
    );
    push_row(
        &mut html,
        "Updated at",
        &display_option(view.ingest.updated_at.as_deref(), "not reported"),
    );
    html.push_str("</tbody></table></section>");

    html.push_str("<section><h2>Runtime</h2><table><tbody>");
    push_row(
        &mut html,
        "Peers configured",
        &number(view.peers_configured as u64),
    );
    push_row(&mut html, "Streaming", &badge(view.streaming_enabled));
    push_row(&mut html, "Broadcast", &badge(view.broadcast_enabled));
    push_row(&mut html, "Archive", &badge(view.archive_enabled));
    html.push_str("</tbody></table></section>");

    html.push_str("</div><nav aria-label=\"Related endpoints\">");
    html.push_str("<a href=\"/healthz\">GET /healthz</a>");
    html.push_str("<a href=\"/v1/status\">GET /v1/status</a>");
    html.push_str("<a href=\"/v1/network\">GET /v1/network</a>");
    html.push_str("<a href=\"/openapi.json\">GET /openapi.json</a>");
    html.push_str("<a href=\"/docs\">GET /docs</a>");
    html.push_str(
        "</nav><footer>Times are UTC. /healthz and the /v1 JSON routes remain machine-readable.</footer>",
    );
    html.push_str("</main></body></html>");
    html
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_dynamic_html() {
        let view = StatusView {
            version: "<script>alert(\"x\") & 'y'</script>".to_owned(),
            ingest: IngestView {
                verification: Some("<b>verified</b>".to_owned()),
                ..IngestView::default()
            },
            storage_error: Some("<boom & \"quoted\">".to_owned()),
            ..StatusView::default()
        };

        let html = render(&view);

        assert!(
            html.contains("&lt;script&gt;alert(&quot;x&quot;) &amp; &#39;y&#39;&lt;/script&gt;")
        );
        assert!(html.contains("&lt;b&gt;verified&lt;/b&gt;"));
        assert!(html.contains("&lt;boom &amp; &quot;quoted&quot;&gt;"));
        assert!(!html.contains("<script>alert"));
        assert!(!html.contains("<b>verified</b>"));
        assert!(!html.contains("<boom"));
    }

    #[test]
    fn formats_uptime_in_days_hours_minutes_seconds() {
        assert_eq!(format_uptime(0), "0d 00h 00m 00s");
        assert_eq!(format_uptime(61), "0d 00h 01m 01s");
        assert_eq!(format_uptime(90_061), "1d 01h 01m 01s");
        assert_eq!(format_uptime(900_000), "10d 10h 00m 00s");
    }

    #[test]
    fn renders_zero_counts_and_missing_head() {
        let view = StatusView::default();
        let html = render(&view);

        assert!(html.contains("no indexed head"));
        assert!(html.contains("<td>0</td>"));
        assert!(html.contains("not reported"));
        assert!(html.contains(env!("CARGO_PKG_VERSION")));
        assert!(html.contains("<meta http-equiv=\"refresh\" content=\"5\">"));
        assert!(html.contains("href=\"/v1/network\""));
        assert!(html.contains("href=\"/healthz\""));
        assert!(html.contains("href=\"/docs\""));
        assert!(html.contains("remain machine-readable"));
    }

    #[test]
    fn renders_storage_error_text() {
        let view = StatusView {
            storage_error: Some("network stats unavailable: connection refused".to_owned()),
            ..StatusView::default()
        };

        let html = render(&view);

        assert!(html.contains("<strong>Storage error</strong>"));
        assert!(html.contains("network stats unavailable: connection refused"));
        assert!(html.contains("role=\"alert\""));
    }
}
