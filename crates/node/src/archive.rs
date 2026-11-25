//! Fallback tick data from the public Qubic archive.
//!
//! Core peers stop serving `TickData` for some finalized ticks even though
//! their quorum votes and transactions remain. The public Query API keeps the
//! full tick data and exposes it over HTTPS, so the indexer can recover those
//! ticks instead of stalling or skipping them.

use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, Datelike, Timelike, Utc};
use qubic::messages::{MAX_CONTRACTS, TICK_DATA_SIZE, TX_PER_TICK, TickData};

use crate::{Error, Result};

const ARCHIVE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(crate) struct ArchiveClient {
    base_url: String,
    client: reqwest::Client,
}

impl ArchiveClient {
    pub(crate) fn new(base_url: String) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(ARCHIVE_TIMEOUT)
            .build()
            .map_err(|error| Error::Archive(format!("archive client: {error}")))?;
        Ok(Self { base_url, client })
    }

    pub(crate) async fn tick_data(&self, tick: u32) -> Result<TickData> {
        let url = format!("{}/getTickData", self.base_url.trim_end_matches('/'));
        let response = self
            .client
            .post(url)
            .json(&serde_json::json!({ "tickNumber": tick }))
            .send()
            .await
            .map_err(|error| Error::Archive(format!("request tick {tick}: {error}")))?;
        let response = response
            .error_for_status()
            .map_err(|error| Error::Archive(format!("request tick {tick}: {error}")))?;
        let body = response
            .json::<serde_json::Value>()
            .await
            .map_err(|error| Error::Archive(format!("decode tick {tick}: {error}")))?;
        parse_tick_data(&body, tick)
    }
}

fn parse_tick_data(body: &serde_json::Value, requested: u32) -> Result<TickData> {
    let data = body
        .get("tickData")
        .ok_or_else(|| Error::Archive(format!("tick {requested} has no tickData")))?;
    let tick = number(data, "tickNumber")?;
    if tick != u64::from(requested) {
        return Err(Error::Archive(format!(
            "archive returned tick {tick}, requested {requested}"
        )));
    }
    let epoch = number(data, "epoch")?;
    let computor_index = number(data, "computorIndex")?;
    let timestamp = match data.get("timestamp") {
        Some(serde_json::Value::String(value)) => value
            .parse::<i64>()
            .map_err(|error| Error::Archive(format!("timestamp: {error}")))?,
        Some(value) => value
            .as_i64()
            .ok_or_else(|| Error::Archive("timestamp is not an integer".into()))?,
        None => return Err(Error::Archive("timestamp is missing".into())),
    };
    let time = DateTime::<Utc>::from_timestamp_millis(timestamp)
        .ok_or_else(|| Error::Archive(format!("invalid timestamp {timestamp}")))?;

    let timelock = decode_base64(data, "timeLock", 32)?;
    let mut timelock_array = [0u8; 32];
    timelock_array.copy_from_slice(&timelock);

    let signature = decode_base64(data, "signature", 64)?;
    let mut signature_array = [0u8; 64];
    signature_array.copy_from_slice(&signature);

    let hashes = data
        .get("transactionHashes")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| Error::Archive("transactionHashes is missing".into()))?;
    if hashes.len() > TX_PER_TICK {
        return Err(Error::Archive(format!(
            "archive returned {} transaction hashes, maximum is {TX_PER_TICK}",
            hashes.len()
        )));
    }
    let mut transaction_digests = vec![[0u8; 32]; TX_PER_TICK];
    for (index, hash) in hashes.iter().enumerate() {
        let hash = hash
            .as_str()
            .ok_or_else(|| Error::Archive("transaction hash is not a string".into()))?;
        transaction_digests[index] = qubic::decode_tx_hash(hash)
            .map_err(|error| Error::Archive(format!("transaction hash: {error}")))?;
    }

    let fees = data
        .get("contractFees")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| Error::Archive("contractFees is missing".into()))?;
    if fees.len() > MAX_CONTRACTS {
        return Err(Error::Archive(format!(
            "archive returned {} contract fees, maximum is {MAX_CONTRACTS}",
            fees.len()
        )));
    }
    let mut contract_fees = vec![0i64; MAX_CONTRACTS];
    for (index, fee) in fees.iter().enumerate() {
        contract_fees[index] = fee
            .as_i64()
            .ok_or_else(|| Error::Archive("contract fee is not an integer".into()))?;
    }

    let tick_data = TickData {
        computor_index: u16::try_from(computor_index)
            .map_err(|_| Error::Archive("computor index is too large".into()))?,
        epoch: u16::try_from(epoch).map_err(|_| Error::Archive("epoch is too large".into()))?,
        tick: u32::try_from(tick).map_err(|_| Error::Archive("tick is too large".into()))?,
        millisecond: u16::try_from(time.timestamp_subsec_millis())
            .map_err(|_| Error::Archive("millisecond is invalid".into()))?,
        second: time.second() as u8,
        minute: time.minute() as u8,
        hour: time.hour() as u8,
        day: time.day() as u8,
        month: time.month() as u8,
        year: (time.year() - 2000) as u8,
        timelock: timelock_array,
        transaction_digests,
        contract_fees,
        signature: signature_array,
    };
    if tick_data.encode().len() != TICK_DATA_SIZE {
        return Err(Error::Archive(
            "reconstructed tick data has the wrong size".into(),
        ));
    }
    Ok(tick_data)
}

fn number(data: &serde_json::Value, key: &str) -> Result<u64> {
    data.get(key)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| Error::Archive(format!("{key} is missing or not an integer")))
}

fn decode_base64(data: &serde_json::Value, key: &str, size: usize) -> Result<Vec<u8>> {
    let value = data
        .get(key)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| Error::Archive(format!("{key} is missing")))?;
    let bytes = BASE64
        .decode(value)
        .map_err(|error| Error::Archive(format!("{key}: {error}")))?;
    if bytes.len() != size {
        return Err(Error::Archive(format!(
            "{key} has {} bytes, expected {size}",
            bytes.len()
        )));
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_archive_tick_data() {
        let body = serde_json::json!({
            "tickData": {
                "tickNumber": 92261883,
                "epoch": 241,
                "computorIndex": 51,
                "timestamp": "1795613299000",
                "timeLock": "deIsRJaH3SxxKoEetUEdekt1y/4Ka6+M4Tl+ILvxqjc=",
                "transactionHashes": [
                    "thipgipfkksrfcgfnffaqnkraxccjfewurolzxlfdhwwvojclwgtfmlfdeij"
                ],
                "contractFees": [7],
                "signature": "QT4Y0Q7fh7f6XwqJzXIkLk16hJYyWmGW6B+ki3Ht6jG8zJMVy0EReuTKQ7CGw6g5FXmBM5Gpzso7U3aBVUQiAA=="
            }
        });
        let tick_data = parse_tick_data(&body, 92261883).unwrap();
        assert_eq!(tick_data.tick, 92261883);
        assert_eq!(tick_data.epoch, 241);
        assert_eq!(tick_data.computor_index, 51);
        assert_eq!(tick_data.non_empty_transactions().count(), 1);
        assert_eq!(tick_data.contract_fees[0], 7);
        assert_eq!(tick_data.encode().len(), TICK_DATA_SIZE);
    }

    #[tokio::test]
    #[ignore = "connects to a live Qubic peer and the public archive"]
    async fn archive_matches_live_votes_for_several_ticks() {
        use std::net::SocketAddr;

        use qubic::messages::COMPUTOR_COUNT;

        use crate::{Peer, PeerConfig};

        let mut peer = Peer::connect(PeerConfig::new(
            "198.27.75.81:21841".parse::<SocketAddr>().unwrap(),
        ))
        .await
        .expect("peer");
        let client =
            ArchiveClient::new("https://rpc.qubic.org/query/v1".into()).expect("archive client");
        for tick in [92_261_500, 92_261_883, 92_262_000, 92_262_500] {
            let votes = peer
                .votes(tick, &[false; COMPUTOR_COUNT])
                .await
                .expect("votes");
            let digest = votes
                .iter()
                .find(|vote| !vote.is_empty_vote())
                .map(|vote| vote.transaction_digest)
                .expect("non-empty vote");
            let tick_data = client.tick_data(tick).await.expect("archive tick");
            assert_eq!(tick_data.digest(), digest, "tick {tick}");
        }
    }
}
