//! NATS JetStream transport for verified tick batches.
//!
//! The fetcher publishes one compressed [`TickBatch`] per tick; the writer
//! consumes a durable work-queue consumer and acknowledges only after
//! ClickHouse has committed the batch. Message IDs are `epoch:tick`, so a
//! fetcher restart or a duplicate delivery is idempotent in JetStream and in
//! ClickHouse's versioned tables.

use std::time::Duration;

use async_nats::HeaderMap;
use async_nats::jetstream::Context;
use async_nats::jetstream::consumer::{AckPolicy, PullConsumer, pull};
use async_nats::jetstream::stream::{
    Config as StreamConfig, DiscardPolicy, RetentionPolicy, StorageType,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use store::TickBatch;

pub const DEFAULT_STREAM: &str = "glyph_ticks";
pub const DEFAULT_SUBJECT: &str = "glyph.ticks";
pub const DEFAULT_CONSUMER: &str = "glyph-writer";
pub const COMMITTED_SUBJECT: &str = "glyph.committed";
pub const DEFAULT_MAX_MESSAGES: i64 = 1_000_000;
pub const DEFAULT_MAX_BYTES: i64 = 8 * 1024 * 1024 * 1024;
pub const DEFAULT_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);

pub type Result<T> = anyhow::Result<T>;

#[derive(Debug, Clone)]
pub struct BusConfig {
    pub url: String,
    pub stream: String,
    pub subject: String,
    pub consumer: String,
    pub max_messages: i64,
    pub max_bytes: i64,
    pub max_age: Duration,
}

impl BusConfig {
    pub fn from_env() -> Option<Self> {
        let url = std::env::var("NATS_URL").ok()?;
        if url.trim().is_empty() {
            return None;
        }
        Some(Self {
            url,
            stream: std::env::var("NATS_STREAM").unwrap_or_else(|_| DEFAULT_STREAM.into()),
            subject: std::env::var("NATS_SUBJECT").unwrap_or_else(|_| DEFAULT_SUBJECT.into()),
            consumer: std::env::var("NATS_CONSUMER").unwrap_or_else(|_| DEFAULT_CONSUMER.into()),
            max_messages: env_i64("NATS_MAX_MESSAGES", DEFAULT_MAX_MESSAGES),
            max_bytes: env_i64("NATS_MAX_BYTES", DEFAULT_MAX_BYTES),
            max_age: Duration::from_secs(env_i64(
                "NATS_MAX_AGE_SECONDS",
                DEFAULT_MAX_AGE.as_secs() as i64,
            ) as u64),
        })
    }
}

fn env_i64(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

#[derive(Clone)]
pub struct Bus {
    config: BusConfig,
    client: async_nats::Client,
    jetstream: Context,
}

impl Bus {
    pub async fn connect(config: BusConfig) -> Result<Self> {
        let client = async_nats::connect(&config.url).await?;
        let jetstream = async_nats::jetstream::new(client.clone());
        Ok(Self {
            config,
            client,
            jetstream,
        })
    }

    pub fn client(&self) -> &async_nats::Client {
        &self.client
    }

    pub fn config(&self) -> &BusConfig {
        &self.config
    }

    pub async fn ensure_stream(&self) -> Result<()> {
        let config = StreamConfig {
            name: self.config.stream.clone(),
            subjects: vec![self.config.subject.clone()],
            retention: RetentionPolicy::WorkQueue,
            discard: DiscardPolicy::New,
            storage: StorageType::File,
            max_messages: self.config.max_messages,
            max_bytes: self.config.max_bytes,
            max_age: self.config.max_age,
            duplicate_window: self.config.max_age.min(Duration::from_secs(2 * 60 * 60)),
            num_replicas: 1,
            allow_direct: true,
            ..Default::default()
        };
        self.jetstream.get_or_create_stream(config).await?;
        Ok(())
    }

    /// Publishes one verified tick. The message ID makes retries idempotent.
    pub async fn publish_tick(&self, batch: &TickBatch) -> Result<()> {
        let payload = encode_tick(batch)?;
        let mut headers = HeaderMap::new();
        headers.insert("Nats-Msg-Id", format!("{}:{}", batch.epoch, batch.tick));
        let ack = self
            .jetstream
            .publish_with_headers(self.config.subject.clone(), headers, payload.into())
            .await?;
        ack.await?;
        Ok(())
    }

    pub async fn writer_consumer(&self) -> Result<Consumer> {
        let stream = self.jetstream.get_stream(&self.config.stream).await?;
        let consumer = stream
            .get_or_create_consumer(
                &self.config.consumer,
                pull::Config {
                    durable_name: Some(self.config.consumer.clone()),
                    name: Some(self.config.consumer.clone()),
                    ack_policy: AckPolicy::Explicit,
                    max_ack_pending: 1024,
                    ack_wait: Duration::from_secs(60),
                    max_deliver: 20,
                    filter_subject: self.config.subject.clone(),
                    ..Default::default()
                },
            )
            .await?;
        Ok(Consumer { inner: consumer })
    }

    /// Fire-and-forget notification used by the API for live cache
    /// invalidation and streams. This is core NATS, not JetStream: a slow API
    /// consumer must never hold back the writer.
    pub async fn publish_committed(&self, event: &TickEvent) -> Result<()> {
        let payload = serde_json::to_vec(event)?;
        self.client
            .publish(COMMITTED_SUBJECT, payload.into())
            .await?;
        Ok(())
    }
}

pub struct Consumer {
    inner: PullConsumer,
}

impl Consumer {
    pub async fn messages(&self) -> Result<Messages> {
        let messages = self.inner.messages().await?;
        Ok(Messages { inner: messages })
    }
}

pub struct Messages {
    inner: async_nats::jetstream::consumer::pull::Stream,
}

impl Messages {
    pub async fn next(&mut self) -> Option<Result<DecodedMessage>> {
        let message = self.inner.next().await?;
        Some(match message {
            Ok(message) => decode_message(message),
            Err(error) => Err(error.into()),
        })
    }
}

pub struct DecodedMessage {
    pub batch: TickBatch,
    message: async_nats::jetstream::Message,
}

impl DecodedMessage {
    pub async fn ack(self) -> Result<()> {
        self.message
            .ack()
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        Ok(())
    }

    pub async fn nak(self) -> Result<()> {
        self.message
            .ack_with(async_nats::jetstream::message::AckKind::Nak(None))
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        Ok(())
    }

    pub async fn in_progress(&self) -> Result<()> {
        self.message
            .ack_with(async_nats::jetstream::message::AckKind::Progress)
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        Ok(())
    }
}

fn encode_tick(batch: &TickBatch) -> Result<Vec<u8>> {
    let json = serde_json::to_vec(batch)?;
    Ok(zstd::encode_all(json.as_slice(), 3)?)
}

fn decode_message(message: async_nats::jetstream::Message) -> Result<DecodedMessage> {
    let json = zstd::decode_all(message.payload.as_ref())?;
    let batch = serde_json::from_slice::<TickBatch>(&json)?;
    Ok(DecodedMessage { batch, message })
}

/// A lightweight live event published after a batch is committed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickEvent {
    pub epoch: u16,
    pub tick: u32,
    pub transactions: u32,
    pub events: u32,
    pub verification: String,
}

impl TickEvent {
    pub fn from_batch(batch: &TickBatch) -> Self {
        Self {
            epoch: batch.epoch,
            tick: batch.tick,
            transactions: batch.transactions.len() as u32,
            events: batch.events.len() as u32,
            verification: batch.verification.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[tokio::test]
    #[ignore = "requires a local NATS JetStream server on NATS_URL"]
    async fn jetstream_round_trip() {
        let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
        let suffix = std::process::id();
        let config = BusConfig {
            url,
            stream: format!("glyph_ticks_test_{suffix}"),
            subject: format!("glyph.ticks.test.{suffix}"),
            consumer: format!("glyph-writer-test-{suffix}"),
            max_messages: 10_000,
            max_bytes: 64 * 1024 * 1024,
            max_age: Duration::from_secs(60),
        };
        let bus = Bus::connect(config).await.expect("connect");
        bus.ensure_stream().await.expect("stream");
        let batch = TickBatch::new(241, 92_274_000, Utc::now(), 7, "quorum");
        bus.publish_tick(&batch).await.expect("publish");

        let consumer = bus.writer_consumer().await.expect("consumer");
        let mut messages = consumer.messages().await.expect("messages");
        let message = tokio::time::timeout(Duration::from_secs(5), messages.next())
            .await
            .expect("message timeout")
            .expect("message")
            .expect("decode");
        assert_eq!(message.batch.tick, batch.tick);
        assert_eq!(message.batch.epoch, batch.epoch);
        message.ack().await.expect("ack");
    }
}
