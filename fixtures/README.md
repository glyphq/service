# Qubic peer fixtures

Golden packets captured directly from Qubic core peers on 2026-11-18 via the
normal peer TCP protocol on port `21841`. No hosted RPC endpoint was used.

## Capture procedure

1. Open TCP to a peer on port `21841`.
2. Read the initial unsolicited `EXCHANGE_PUBLIC_PEERS` frame (type 0).
3. Send an `EXCHANGE_PUBLIC_PEERS` frame with four zero IPv4 addresses.
4. Send the relevant request with a non-zero `dejavu`.
5. Save the response frames verbatim, including the 8-byte header.

Packets for this set were taken from `157.180.10.49` (head tick `91916246`,
epoch `240`), except `computors.bin` and `logs.bin` from `109.123.229.239`.
`system_info.bin` came from an earlier capture against the same core network.

## Files

| File | Request | Response type | Frame size | Notes |
| --- | --- | --- | --- | --- |
| `exchange_public_peers.bin` | server-initiated | `EXCHANGE_PUBLIC_PEERS` (0) | 24 | Four IPv4 addresses |
| `current_tick_info.bin` | `REQUEST_CURRENT_TICK_INFO` (27) | `RESPOND_CURRENT_TICK_INFO` (28) | 24 | tick 91916246 |
| `system_info.bin` | `REQUEST_SYSTEM_INFO` (46) | `RESPOND_SYSTEM_INFO` (47) | 136 | packed 128-byte payload |
| `computors.bin` | `REQUEST_COMPUTORS` (11) | `BROADCAST_COMPUTORS` (2) | 21706 | epoch 240 |
| `tick_data.bin` | `REQUEST_TICK_DATA` (16) | `BROADCAST_FUTURE_TICK_DATA` (8) | 139384 | 4096 digests |
| `quorum_tick.bin` | `REQUEST_QUORUM_TICK` (14) | `BROADCAST_TICK` (3) | 360 | one vote |
| `tick_transactions.bin` | `REQUEST_TICK_TRANSACTIONS` (29) | `BROADCAST_TRANSACTION` (24) | 200 | input type 12 |
| `entity.bin` | `REQUEST_ENTITY` (31) | `RESPOND_ENTITY` (32) | 848 | zero public key |
| `assets_issuance.bin` | `REQUEST_ASSETS` (52) | `RESPOND_ASSETS` (53) | 832 | issuance + 24 siblings |
| `contract_ipo.bin` | `REQUEST_CONTRACT_IPO` (33) | `RESPOND_CONTRACT_IPO` (34) | 27056 | contract 1 |
| `contract_function_empty.bin` | `REQUEST_CONTRACT_FUNCTION` (42) | `RESPOND_CONTRACT_FUNCTION` (43) | 8 | empty output |
| `logs.bin` | `REQUEST_LOG` (44) | `RESPOND_LOG` (45) | 1186 | 11 records at tick 91910000 |
| `issued_assets.bin` | `REQUEST_ISSUED_ASSETS` (36) | `RESPOND_ISSUED_ASSETS` (37) | 832 | zero public key, one of 31 records |
| `price_series.bin` | `REQUEST_PRICE_SERIES` (78) | `RESPOND_PRICE_SERIES` (79) | 532 | 20 hourly points from hour 40300 |

The fixtures are unmodified protocol frames. `crates/qubic/tests/wire_vectors.rs`
contains golden tests that decode each payload, round-trip it through the Rust
codec, and re-encode `logs.bin` byte-for-byte. The price series frame is covered
by `crates/qubic/tests/price_series.rs`, which also checks the variable
`124 + count * 20` payload size.
