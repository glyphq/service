//! Encrypted, expiring links for the transaction lifecycle SSE stream.
//!
//! A link is a self-contained AEAD token. The API does not store issued
//! tokens, so reconnect-after-disconnect remains cheap and a horizontally
//! scaled API can verify any token with the same deployment key.

use std::time::Duration;

use axum::http::{HeaderMap, Uri, header};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use chacha20poly1305::{ChaCha20Poly1305, Nonce};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::crypto::decode_key;

const TOKEN_PREFIX: &str = "gll1.";
const TOKEN_VERSION: u8 = 1;
const AUDIENCE: &str = "lifecycle-stream";
const ASSOCIATED_DATA: &[u8] = b"glyph.lifecycle.link.v1";
const DEFAULT_TTL_SECONDS: u64 = 900;
const MIN_TTL_SECONDS: u64 = 30;
const MAX_TTL_SECONDS: u64 = 3600;

/// Claims authenticated and encrypted inside one link token.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LinkClaims {
    pub version: u8,
    pub audience: String,
    pub hash: String,
    pub issued_at: i64,
    pub expires_at: i64,
}

/// Deployment configuration and token operations for lifecycle links.
#[derive(Clone)]
pub struct LifecycleLinks {
    key: [u8; 32],
    default_ttl_seconds: u64,
    public_base: Option<String>,
}

impl std::fmt::Debug for LifecycleLinks {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LifecycleLinks")
            .field("default_ttl_seconds", &self.default_ttl_seconds)
            .field("public_base", &self.public_base)
            .finish_non_exhaustive()
    }
}

impl LifecycleLinks {
    /// Reads `LIFECYCLE_LINK_KEY`, `LIFECYCLE_LINK_TTL_SECONDS`, and
    /// `API_PUBLIC_URL`. Returns `None` when the key is absent or malformed so
    /// the feature is explicitly unavailable instead of silently insecure.
    pub fn from_env() -> Option<Self> {
        let raw = std::env::var("LIFECYCLE_LINK_KEY").ok()?;
        let key = match decode_key(raw.trim()) {
            Some(key) => key,
            None => {
                tracing::warn!(
                    "LIFECYCLE_LINK_KEY must be 32 bytes as hex or base64; lifecycle links are disabled"
                );
                return None;
            }
        };
        let default_ttl_seconds = env_ttl();
        let public_base = std::env::var("API_PUBLIC_URL")
            .or_else(|_| std::env::var("LIFECYCLE_LINK_BASE_URL"))
            .ok()
            .and_then(|value| normalize_base(&value));
        Some(Self::new(key, default_ttl_seconds, public_base))
    }

    pub fn new(key: [u8; 32], default_ttl_seconds: u64, public_base: Option<String>) -> Self {
        Self {
            key,
            default_ttl_seconds: default_ttl_seconds.clamp(MIN_TTL_SECONDS, MAX_TTL_SECONDS),
            public_base: public_base.and_then(|value| normalize_base(&value)),
        }
    }

    pub fn default_ttl_seconds(&self) -> u64 {
        self.default_ttl_seconds
    }

    /// Validates an optional caller-supplied TTL.
    pub fn resolve_ttl(&self, requested: Option<u64>) -> Result<u64, String> {
        let ttl = requested.unwrap_or(self.default_ttl_seconds);
        if !(MIN_TTL_SECONDS..=MAX_TTL_SECONDS).contains(&ttl) {
            return Err(format!(
                "ttl_seconds must be between {MIN_TTL_SECONDS} and {MAX_TTL_SECONDS}"
            ));
        }
        Ok(ttl)
    }

    pub fn issue(&self, hash: &str, ttl_seconds: u64) -> Result<String, String> {
        let now = Utc::now().timestamp();
        let claims = LinkClaims {
            version: TOKEN_VERSION,
            audience: AUDIENCE.to_owned(),
            hash: hash.to_ascii_lowercase(),
            issued_at: now,
            expires_at: now.saturating_add(ttl_seconds as i64),
        };
        self.seal(&claims)
    }

    /// Returns the claims for a valid, unexpired token.
    pub fn open(&self, token: &str) -> Result<LinkClaims, String> {
        let encoded = token
            .trim()
            .strip_prefix(TOKEN_PREFIX)
            .ok_or_else(|| "unsupported lifecycle link token".to_owned())?;
        let payload = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| "lifecycle link token is not valid base64".to_owned())?;
        if payload.len() < 12 + 16 {
            return Err("lifecycle link token is truncated".into());
        }
        let (nonce, ciphertext) = payload.split_at(12);
        let cipher = ChaCha20Poly1305::new_from_slice(&self.key)
            .map_err(|_| "invalid lifecycle link key".to_owned())?;
        let plaintext = cipher
            .decrypt(
                Nonce::from_slice(nonce),
                Payload {
                    msg: ciphertext,
                    aad: ASSOCIATED_DATA,
                },
            )
            .map_err(|_| "lifecycle link token authentication failed".to_owned())?;
        let claims: LinkClaims = serde_json::from_slice(&plaintext)
            .map_err(|_| "lifecycle link claims are malformed".to_owned())?;
        if claims.version != TOKEN_VERSION || claims.audience != AUDIENCE {
            return Err("lifecycle link claims are not for this stream".into());
        }
        if claims.expires_at <= Utc::now().timestamp() {
            return Err("lifecycle link has expired".into());
        }
        qubic::decode_tx_hash(&claims.hash)
            .map_err(|_| "lifecycle link hash is invalid".to_owned())?;
        Ok(claims)
    }

    /// Absolute URL when the deployment is configured with a public base,
    /// otherwise an origin-derived absolute URL, otherwise a relative path.
    pub fn stream_url(&self, token: &str, headers: &HeaderMap, uri: &Uri) -> String {
        let path = format!("/v1/lifecycle/stream?token={token}");
        match self
            .public_base
            .clone()
            .or_else(|| request_origin(headers, uri))
        {
            Some(base) => format!("{base}{path}"),
            None => path,
        }
    }

    fn seal(&self, claims: &LinkClaims) -> Result<String, String> {
        let plaintext = serde_json::to_vec(claims)
            .map_err(|_| "lifecycle claims encoding failed".to_owned())?;
        let cipher = ChaCha20Poly1305::new_from_slice(&self.key)
            .map_err(|_| "invalid lifecycle link key".to_owned())?;
        let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng);
        let ciphertext = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: &plaintext,
                    aad: ASSOCIATED_DATA,
                },
            )
            .map_err(|_| "lifecycle link encryption failed".to_owned())?;
        let mut payload = nonce.to_vec();
        payload.extend_from_slice(&ciphertext);
        Ok(format!("{TOKEN_PREFIX}{}", URL_SAFE_NO_PAD.encode(payload)))
    }

    pub fn expires_in(&self, claims: &LinkClaims) -> Duration {
        let remaining = claims.expires_at.saturating_sub(Utc::now().timestamp());
        Duration::from_secs(u64::try_from(remaining).unwrap_or(0))
    }
}

fn env_ttl() -> u64 {
    std::env::var("LIFECYCLE_LINK_TTL_SECONDS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|value| value.clamp(MIN_TTL_SECONDS, MAX_TTL_SECONDS))
        .unwrap_or(DEFAULT_TTL_SECONDS)
}

fn normalize_base(value: &str) -> Option<String> {
    let base = value.trim().trim_end_matches('/');
    if base.is_empty() {
        return None;
    }
    let parsed = url::Url::parse(base).ok()?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return None;
    }
    Some(base.to_owned())
}

fn request_origin(headers: &HeaderMap, uri: &Uri) -> Option<String> {
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| matches!(*value, "http" | "https"))
        .or_else(|| uri.scheme_str())
        .unwrap_or("http");
    let host = headers
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .or_else(|| uri.authority().map(|authority| authority.as_str()))?;
    if host.is_empty()
        || host.contains('@')
        || host.contains('/')
        || host.contains('\\')
        || host.chars().any(char::is_whitespace)
    {
        return None;
    }
    let authority: axum::http::uri::Authority = host.parse().ok()?;
    Some(format!("{scheme}://{authority}"))
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderValue, Request};

    use super::*;

    const HASH: &str = "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK";

    fn links() -> LifecycleLinks {
        LifecycleLinks::new([7u8; 32], 600, Some("https://api.example".into()))
    }

    #[test]
    fn token_round_trips_and_keeps_the_hash_private() {
        let links = links();
        let token = links.issue(HASH, 120).expect("issue");
        assert!(
            !token
                .to_ascii_lowercase()
                .contains(&HASH.to_ascii_lowercase())
        );
        let claims = links.open(&token).expect("open");
        assert_eq!(claims.hash, HASH.to_ascii_lowercase());
        assert_eq!(claims.audience, AUDIENCE);
        assert!(links.expires_in(&claims) <= Duration::from_secs(120));
    }

    #[test]
    fn tampering_or_a_different_key_is_rejected() {
        let links = links();
        let token = links.issue(HASH, 120).expect("issue");
        let mut tampered = token.clone().into_bytes();
        let last = tampered.len() - 1;
        tampered[last] = if tampered[last] == b'A' { b'B' } else { b'A' };
        let tampered = String::from_utf8(tampered).expect("utf8");
        assert!(links.open(&tampered).is_err());

        let other = LifecycleLinks::new([9u8; 32], 600, None);
        assert!(other.open(&token).is_err());
    }

    #[test]
    fn expired_tokens_are_rejected() {
        let links = links();
        let now = Utc::now().timestamp();
        let claims = LinkClaims {
            version: TOKEN_VERSION,
            audience: AUDIENCE.into(),
            hash: HASH.to_ascii_lowercase(),
            issued_at: now - 60,
            expires_at: now - 1,
        };
        let token = links.seal(&claims).expect("seal");
        assert!(links.open(&token).is_err());
    }

    #[test]
    fn ttl_is_bounded() {
        let links = links();
        assert!(links.resolve_ttl(None).is_ok());
        assert!(links.resolve_ttl(Some(29)).is_err());
        assert!(links.resolve_ttl(Some(30)).is_ok());
        assert!(links.resolve_ttl(Some(3601)).is_err());
    }

    #[test]
    fn origin_uses_forwarded_proto_and_host() {
        let request = Request::builder()
            .uri("http://internal:8080/v1/transactions/x/lifecycle/link")
            .header("x-forwarded-proto", "https")
            .header(header::HOST, "api.example")
            .body(())
            .expect("request");
        let links = LifecycleLinks::new([7u8; 32], 600, None);
        let url = links.stream_url("token", request.headers(), request.uri());
        assert_eq!(url, "https://api.example/v1/lifecycle/stream?token=token");
    }

    #[test]
    fn origin_rejects_host_header_injection() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("evil@example.com"));
        assert_eq!(request_origin(&headers, &Uri::default()), None);
    }

    #[test]
    fn configured_base_wins_over_request_headers() {
        let links = links();
        let request = Request::builder()
            .uri("http://internal")
            .header(header::HOST, "attacker.example")
            .body(())
            .expect("request");
        let url = links.stream_url("token", request.headers(), request.uri());
        assert!(url.starts_with("https://api.example/"));
    }
}
