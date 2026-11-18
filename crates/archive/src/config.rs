//! S3/MinIO archive configuration.

use std::collections::BTreeMap;

use crate::key::{normalize_prefix, validate_key};
use crate::{Error, Result};

/// Default region when neither `S3_REGION` nor an AWS region variable is set.
pub const DEFAULT_REGION: &str = "us-east-1";

/// S3-compatible object store configuration.
///
/// This covers AWS S3 and MinIO. `endpoint` is optional: when it is absent the
/// region's AWS endpoint is used. Path-style addressing is the default, which is
/// what MinIO expects.
///
/// # Environment
///
/// [`Config::from_env`] reads:
///
/// | Variable | Meaning |
/// | --- | --- |
/// | `S3_BUCKET` | bucket name, required |
/// | `S3_BUCKET` or `MINIO_BUCKET` | bucket name, required |
/// | `S3_ENDPOINT`, `MINIO_ENDPOINT`, or `AWS_ENDPOINT_URL_S3`/`AWS_ENDPOINT_URL` | endpoint URL |
/// | `S3_ACCESS_KEY`/`S3_ACCESS_KEY_ID`, `MINIO_ACCESS_KEY`/`MINIO_ROOT_USER`, or `AWS_ACCESS_KEY_ID` | access key |
/// | `S3_SECRET_KEY`/`S3_SECRET_ACCESS_KEY`, `MINIO_SECRET_KEY`/`MINIO_ROOT_PASSWORD`, or `AWS_SECRET_ACCESS_KEY` | secret key |
/// | `S3_SESSION_TOKEN` or `AWS_SESSION_TOKEN` | optional session token |
/// | `S3_REGION`, `AWS_REGION`, or `AWS_DEFAULT_REGION` | region, default `us-east-1` |
/// | `S3_PREFIX` | optional key prefix inside the bucket |
/// | `S3_ALLOW_HTTP` | `1`/`true`/`yes`/`on` or `0`/`false`/`no`/`off` |
///
/// `allow_http` defaults to true when the endpoint uses the `http://` scheme and
/// false otherwise. Empty values are treated as unset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Bucket name.
    pub bucket: String,
    /// Optional endpoint URL, for MinIO or another S3-compatible service.
    pub endpoint: Option<String>,
    /// Access key id. Must be paired with `secret_access_key`.
    pub access_key_id: Option<String>,
    /// Secret access key. Must be paired with `access_key_id`.
    pub secret_access_key: Option<String>,
    /// Optional session token for temporary credentials.
    pub session_token: Option<String>,
    /// AWS region. Defaults to [`DEFAULT_REGION`].
    pub region: String,
    /// Optional key prefix inside the bucket.
    pub prefix: Option<String>,
    /// Allow plain HTTP endpoints.
    pub allow_http: bool,
}

impl Config {
    /// Builds a config with only a bucket set. Region defaults to
    /// [`DEFAULT_REGION`], endpoint and credentials are unset.
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            region: DEFAULT_REGION.to_owned(),
            prefix: None,
            allow_http: false,
        }
    }

    /// Reads configuration from the process environment.
    ///
    /// Returns `None` when `S3_BUCKET` is unset or empty. Any other missing
    /// value falls back to a documented default.
    pub fn from_env() -> Option<Self> {
        Self::from_pairs(std::env::vars())
    }

    /// Reads configuration from an arbitrary key/value source.
    ///
    /// Exposed so callers and tests can supply a deterministic environment
    /// without mutating process globals. The last value for a repeated key
    /// wins.
    pub fn from_pairs<I, K, V>(pairs: I) -> Option<Self>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let map: BTreeMap<String, String> = pairs
            .into_iter()
            .map(|(key, value)| (key.as_ref().to_owned(), value.as_ref().to_owned()))
            .collect();

        let bucket = non_empty(value(&map, &["S3_BUCKET", "MINIO_BUCKET"]))?;
        let endpoint = non_empty(value(
            &map,
            &[
                "S3_ENDPOINT",
                "MINIO_ENDPOINT",
                "AWS_ENDPOINT_URL_S3",
                "AWS_ENDPOINT_URL",
            ],
        ))
        .map(|endpoint| endpoint.trim_end_matches('/').to_owned())
        .filter(|endpoint| !endpoint.is_empty());
        let access_key_id = non_empty(value(
            &map,
            &[
                "S3_ACCESS_KEY",
                "S3_ACCESS_KEY_ID",
                "MINIO_ACCESS_KEY",
                "MINIO_ROOT_USER",
                "AWS_ACCESS_KEY_ID",
            ],
        ))
        .map(str::to_owned);
        let secret_access_key = non_empty(value(
            &map,
            &[
                "S3_SECRET_KEY",
                "S3_SECRET_ACCESS_KEY",
                "MINIO_SECRET_KEY",
                "MINIO_ROOT_PASSWORD",
                "AWS_SECRET_ACCESS_KEY",
            ],
        ))
        .map(str::to_owned);
        let session_token =
            non_empty(value(&map, &["S3_SESSION_TOKEN", "AWS_SESSION_TOKEN"])).map(str::to_owned);
        let region = non_empty(value(
            &map,
            &["S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"],
        ))
        .unwrap_or(DEFAULT_REGION)
        .to_owned();
        let prefix = normalize_prefix(non_empty(value(&map, &["S3_PREFIX"])));
        let allow_http = non_empty(value(&map, &["S3_ALLOW_HTTP"]))
            .and_then(parse_bool)
            .unwrap_or_else(|| {
                endpoint
                    .as_deref()
                    .is_some_and(|endpoint| endpoint.starts_with("http://"))
            });

        Some(Self {
            bucket: bucket.to_owned(),
            endpoint,
            access_key_id,
            secret_access_key,
            session_token,
            region,
            prefix,
            allow_http,
        })
    }

    /// Checks values that would otherwise fail later inside the S3 client.
    pub fn validate(&self) -> Result<()> {
        if self.bucket.trim().is_empty() {
            return Err(Error::Config("S3 bucket is empty".into()));
        }
        if self.bucket.contains('/') || self.bucket.chars().any(char::is_whitespace) {
            return Err(Error::Config(format!(
                "invalid S3 bucket {:?}",
                self.bucket
            )));
        }
        if self.region.trim().is_empty() {
            return Err(Error::Config("S3 region is empty".into()));
        }
        match (&self.access_key_id, &self.secret_access_key) {
            (Some(_), Some(_)) | (None, None) => {}
            _ => {
                return Err(Error::Config(
                    "S3 credentials require both an access key and a secret key".into(),
                ));
            }
        }
        if self.session_token.is_some() && self.access_key_id.is_none() {
            return Err(Error::Config(
                "S3 session token requires an access key and a secret key".into(),
            ));
        }
        if let Some(endpoint) = &self.endpoint {
            if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
                return Err(Error::Config(format!(
                    "S3 endpoint {endpoint:?} must start with http:// or https://"
                )));
            }
            if endpoint.starts_with("http://") && !self.allow_http {
                return Err(Error::Config(format!(
                    "S3 endpoint {endpoint:?} is plain HTTP but allow_http is false"
                )));
            }
        }
        if let Some(prefix) = &self.prefix {
            validate_key(prefix).map_err(|error| {
                Error::Config(format!("invalid S3 prefix {:?}: {error}", prefix))
            })?;
        }
        Ok(())
    }
}

fn value<'a>(map: &'a BTreeMap<String, String>, names: &[&str]) -> Option<&'a str> {
    names.iter().find_map(|name| {
        map.get(*name)
            .map(String::as_str)
            .filter(|value| !value.trim().is_empty())
    })
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(pairs: &[(&str, &str)]) -> Option<Config> {
        Config::from_pairs(pairs.iter().copied())
    }

    #[test]
    fn missing_bucket_returns_none() {
        assert!(parse(&[]).is_none());
        assert!(parse(&[("S3_ENDPOINT", "http://localhost:9000")]).is_none());
        assert!(parse(&[("S3_BUCKET", "   ")]).is_none());
    }

    #[test]
    fn minimal_minio_config() {
        let config = parse(&[
            ("S3_BUCKET", "glyph"),
            ("S3_ENDPOINT", "http://127.0.0.1:9000/"),
            ("S3_ACCESS_KEY", "minio"),
            ("S3_SECRET_KEY", "minio-secret"),
        ])
        .unwrap();
        assert_eq!(config.bucket, "glyph");
        assert_eq!(config.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert_eq!(config.access_key_id.as_deref(), Some("minio"));
        assert_eq!(config.secret_access_key.as_deref(), Some("minio-secret"));
        assert_eq!(config.region, DEFAULT_REGION);
        assert_eq!(config.prefix, None);
        assert!(config.allow_http);
        config.validate().unwrap();
    }

    #[test]
    fn minio_variable_aliases_and_session_token() {
        let config = parse(&[
            ("MINIO_BUCKET", "glyph"),
            ("MINIO_ENDPOINT", "http://127.0.0.1:9000"),
            ("MINIO_ACCESS_KEY", "minio"),
            ("MINIO_SECRET_KEY", "minio-secret"),
            ("S3_SESSION_TOKEN", "temporary"),
        ])
        .unwrap();
        assert_eq!(config.bucket, "glyph");
        assert_eq!(config.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert_eq!(config.access_key_id.as_deref(), Some("minio"));
        assert_eq!(config.secret_access_key.as_deref(), Some("minio-secret"));
        assert_eq!(config.session_token.as_deref(), Some("temporary"));
        assert!(config.allow_http);
        config.validate().unwrap();
    }

    #[test]
    fn full_config_and_aws_aliases() {
        let config = parse(&[
            ("S3_BUCKET", "glyph"),
            ("S3_ENDPOINT", "https://s3.example.test"),
            ("AWS_ACCESS_KEY_ID", "key"),
            ("AWS_SECRET_ACCESS_KEY", "secret"),
            ("AWS_REGION", "eu-central-1"),
            ("S3_PREFIX", "/archive/raw/"),
            ("S3_ALLOW_HTTP", "false"),
        ])
        .unwrap();
        assert_eq!(config.region, "eu-central-1");
        assert_eq!(config.prefix.as_deref(), Some("archive/raw"));
        assert!(!config.allow_http);
        config.validate().unwrap();
    }

    #[test]
    fn allow_http_defaults_from_endpoint_scheme() {
        assert!(
            parse(&[("S3_BUCKET", "b"), ("S3_ENDPOINT", "http://minio:9000")])
                .unwrap()
                .allow_http
        );
        assert!(
            !parse(&[("S3_BUCKET", "b"), ("S3_ENDPOINT", "https://s3.test")])
                .unwrap()
                .allow_http
        );
        assert!(!parse(&[("S3_BUCKET", "b")]).unwrap().allow_http);
    }

    #[test]
    fn validation_rejects_partial_credentials_and_plain_http() {
        let mut config = Config::new("bucket");
        config.access_key_id = Some("key".into());
        assert!(config.validate().is_err());

        let mut config = Config::new("bucket");
        config.endpoint = Some("http://minio:9000".into());
        config.allow_http = false;
        assert!(config.validate().is_err());

        let mut config = Config::new("bucket");
        config.prefix = Some("../escape".into());
        assert!(config.validate().is_err());

        let mut config = Config::new("bucket");
        config.session_token = Some("token".into());
        assert!(config.validate().is_err());
    }

    #[test]
    fn validation_rejects_bad_bucket_and_endpoint() {
        assert!(Config::new("").validate().is_err());
        assert!(Config::new("a/b").validate().is_err());
        let mut config = Config::new("bucket");
        config.endpoint = Some("minio:9000".into());
        assert!(config.validate().is_err());
    }

    #[test]
    fn bool_aliases_are_accepted() {
        assert_eq!(parse_bool("YES"), Some(true));
        assert_eq!(parse_bool("off"), Some(false));
        assert_eq!(parse_bool("maybe"), None);
    }
}
