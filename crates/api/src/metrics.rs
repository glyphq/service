//! In-process request and cache metrics.
//!
//! The public API is intentionally small: route latency, status, in-flight
//! requests, and cache hit/miss counts. ClickHouse query times remain available
//! in `system.query_log`, which is a better place for per-query detail than a
//! per-process histogram.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

const BUCKETS_MS: [u64; 11] = [1, 2, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500];

#[derive(Debug, Default)]
struct Histogram {
    /// Cumulative counts per upper bound, aligned with `BUCKETS_MS`.
    buckets: [u64; BUCKETS_MS.len()],
    count: u64,
    sum_ms: f64,
}

impl Histogram {
    fn observe(&mut self, elapsed: Duration) {
        let ms = elapsed.as_secs_f64() * 1_000.0;
        self.count = self.count.saturating_add(1);
        self.sum_ms += ms;
        for (index, bound) in BUCKETS_MS.iter().enumerate() {
            if ms <= *bound as f64 {
                self.buckets[index] = self.buckets[index].saturating_add(1);
            }
        }
    }

    fn render(&self, name: &str, labels: &str, output: &mut String) {
        for (index, bound) in BUCKETS_MS.iter().enumerate() {
            push_sample(
                output,
                &format!("{name}_bucket"),
                &format!("{labels},le=\"{bound}\""),
                self.buckets[index] as f64,
            );
        }
        // +Inf is cumulative for every observation, including slow ones that
        // fall outside the finite buckets.
        push_sample(
            output,
            &format!("{name}_bucket"),
            &format!("{labels},le=\"+Inf\""),
            self.count as f64,
        );
        push_sample(
            output,
            &format!("{name}_sum"),
            labels,
            self.sum_ms / 1_000.0,
        );
        push_sample(output, &format!("{name}_count"), labels, self.count as f64);
    }
}

fn push_sample(output: &mut String, name: &str, labels: &str, value: f64) {
    if labels.is_empty() {
        output.push_str(&format!("{name} {value}\n"));
    } else {
        output.push_str(&format!("{name}{{{labels}}} {value}\n"));
    }
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[derive(Debug, Default)]
pub struct Metrics {
    http_status: Mutex<BTreeMap<(String, String, u16), u64>>,
    http_latency: Mutex<BTreeMap<(String, String), Histogram>>,
    in_flight: AtomicI64,
}

impl Metrics {
    pub fn request_started(&self) {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
    }

    pub fn request_finished(&self, route: &str, method: &str, status: u16, elapsed: Duration) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
        let route = route.to_owned();
        let method = method.to_owned();
        if let Ok(mut statuses) = self.http_status.lock() {
            *statuses
                .entry((route.clone(), method.clone(), status))
                .or_default() += 1;
        }
        if let Ok(mut latencies) = self.http_latency.lock() {
            latencies
                .entry((route, method))
                .or_default()
                .observe(elapsed);
        }
    }

    /// Renders the current snapshot in Prometheus text exposition format.
    pub fn render(&self, cache_hits: u64, cache_misses: u64) -> String {
        let mut output = String::with_capacity(4_096);
        output.push_str(
            "# HELP glyph_http_requests_total HTTP requests by route, method, and status.\n",
        );
        output.push_str("# TYPE glyph_http_requests_total counter\n");
        if let Ok(statuses) = self.http_status.lock() {
            for ((route, method, status), count) in statuses.iter() {
                let labels = format!(
                    "route=\"{}\",method=\"{}\",status=\"{}\"",
                    escape_label(route),
                    escape_label(method),
                    status
                );
                push_sample(
                    &mut output,
                    "glyph_http_requests_total",
                    &labels,
                    *count as f64,
                );
            }
        }

        output.push_str("# HELP glyph_http_request_duration_seconds HTTP request latency.\n");
        output.push_str("# TYPE glyph_http_request_duration_seconds histogram\n");
        if let Ok(latencies) = self.http_latency.lock() {
            for ((route, method), histogram) in latencies.iter() {
                let labels = format!(
                    "route=\"{}\",method=\"{}\"",
                    escape_label(route),
                    escape_label(method)
                );
                histogram.render("glyph_http_request_duration_seconds", &labels, &mut output);
            }
        }

        output.push_str("# HELP glyph_http_requests_in_flight Requests currently being served.\n");
        output.push_str("# TYPE glyph_http_requests_in_flight gauge\n");
        push_sample(
            &mut output,
            "glyph_http_requests_in_flight",
            "",
            self.in_flight.load(Ordering::Relaxed) as f64,
        );

        output.push_str("# HELP glyph_cache_requests_total In-process live cache lookups.\n");
        output.push_str("# TYPE glyph_cache_requests_total counter\n");
        push_sample(
            &mut output,
            "glyph_cache_requests_total",
            "result=\"hit\"",
            cache_hits as f64,
        );
        push_sample(
            &mut output,
            "glyph_cache_requests_total",
            "result=\"miss\"",
            cache_misses as f64,
        );
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_cumulative_buckets_and_status_counts() {
        let metrics = Metrics::default();
        metrics.request_started();
        metrics.request_finished("/v1/transactions", "GET", 200, Duration::from_millis(12));
        let body = metrics.render(4, 1);
        assert!(body.contains(
            "glyph_http_requests_total{route=\"/v1/transactions\",method=\"GET\",status=\"200\"} 1"
        ));
        assert!(body.contains(
            "glyph_http_request_duration_seconds_bucket{route=\"/v1/transactions\",method=\"GET\",le=\"10\"} 0"
        ));
        assert!(body.contains(
            "glyph_http_request_duration_seconds_bucket{route=\"/v1/transactions\",method=\"GET\",le=\"25\"} 1"
        ));
        assert!(body.contains("glyph_cache_requests_total{result=\"hit\"} 4"));
        assert!(body.contains("glyph_http_requests_in_flight 0"));
    }
}
