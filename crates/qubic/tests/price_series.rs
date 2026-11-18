//! Golden test for a live `RespondPriceSeries` packet.
//!
//! The fixture was captured from a Qubic core peer on 2026-11-18 after sending
//! `RequestPriceSeries { from_hour: 40300, count: 20 }`. It keeps the 8-byte
//! header, so this also checks framing and packet-size handling.

use qubic::messages::{
    PRICE_POINT_FLAG_FILLED, PRICE_SERIES_MAX_POINTS, PRICE_SERIES_SOURCE_BOOTSTRAP,
    RESPOND_PRICE_SERIES_HEADER_SIZE, RESPOND_PRICE_SERIES_POINT_SIZE, RequestPriceSeries,
    RespondPriceSeries, RespondPriceSeriesPoint,
};
use qubic::wire::{Header, MessageType, Packet};

const FIXTURE: &[u8] = include_bytes!("../../../fixtures/price_series.bin");

#[test]
fn request_price_series_layout() {
    let request = RequestPriceSeries {
        from_hour: 40_300,
        count: 20,
    };
    let bytes = request.encode();
    assert_eq!(bytes.len(), 8);
    assert_eq!(&bytes[0..4], &40_300u32.to_le_bytes());
    assert_eq!(&bytes[4..8], &20u32.to_le_bytes());
    assert_eq!(RequestPriceSeries::decode(&bytes).unwrap(), request);
    assert!(RequestPriceSeries::decode(&bytes[..4]).is_err());
}

#[test]
fn respond_price_series_point_layout() {
    let point = RespondPriceSeriesPoint {
        hour: 0x0102_0304,
        append_tick: 0x0506_0708,
        flags: PRICE_POINT_FLAG_FILLED,
        price_e8: -6_411_509_000_000,
    };
    let bytes = point.encode();
    assert_eq!(bytes.len(), RESPOND_PRICE_SERIES_POINT_SIZE);
    assert_eq!(&bytes[0..4], &0x0102_0304u32.to_le_bytes());
    assert_eq!(&bytes[4..8], &0x0506_0708u32.to_le_bytes());
    assert_eq!(&bytes[8..12], &1u32.to_le_bytes());
    assert_eq!(&bytes[12..20], &(-6_411_509_000_000i64).to_le_bytes());
    assert_eq!(RespondPriceSeriesPoint::decode(&bytes).unwrap(), point);
}

#[test]
fn golden_price_series_decodes_and_reencodes() {
    let header = Header::decode(FIXTURE).expect("fixture header decodes");
    assert_eq!(header.kind, MessageType::RespondPriceSeries);
    assert_eq!(header.size as usize, FIXTURE.len());

    let packet = Packet::decode(FIXTURE).expect("fixture packet decodes");
    assert_eq!(packet.kind, MessageType::RespondPriceSeries);
    assert_eq!(packet.payload.len(), FIXTURE.len() - 8);

    let series = RespondPriceSeries::decode(&packet.payload).expect("price series payload decodes");
    assert_eq!(series.tick, 91_919_063);
    assert_eq!(series.epoch, 240);
    assert!(series.synced);
    assert!(!series.bound);
    assert_eq!(series.source, PRICE_SERIES_SOURCE_BOOTSTRAP);
    assert_eq!(series.base_hour, 40_316);
    assert_eq!(series.next_hour, 40_325);
    assert_eq!(series.count, 20);
    assert_eq!(series.points.len(), series.count as usize);
    assert!(series.points.len() <= PRICE_SERIES_MAX_POINTS);
    assert_eq!(
        packet.payload.len(),
        RESPOND_PRICE_SERIES_HEADER_SIZE + series.points.len() * RESPOND_PRICE_SERIES_POINT_SIZE
    );

    let first = series.points.first().expect("fixture has points");
    assert_eq!(first.hour, 40_300);
    assert_eq!(first.append_tick, 0);
    assert_eq!(first.flags & PRICE_POINT_FLAG_FILLED, 0);
    assert_eq!(first.price_e8, 6_411_509_000_000);

    let last = series.points.last().expect("fixture has points");
    assert_eq!(last.hour, 40_319);
    assert_eq!(last.append_tick, 91_910_040);
    assert!(last.price_e8 > 0);

    // The captured payload must re-encode byte-for-byte, including count and
    // point order.
    assert_eq!(series.encode(), packet.payload);
}
