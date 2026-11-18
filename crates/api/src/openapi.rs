use serde_json::{Value, json};

fn limit_param() -> Value {
    json!({
        "name": "limit",
        "in": "query",
        "required": false,
        "schema": { "type": "integer", "minimum": 1, "maximum": 200, "default": 50 }
    })
}

fn cursor_param() -> Value {
    json!({
        "name": "cursor",
        "in": "query",
        "required": false,
        "description": "Opaque cursor from meta.nextCursor. Bound to the route and filters.",
        "schema": { "type": "string" }
    })
}

fn offset_param() -> Value {
    json!({
        "name": "offset",
        "in": "query",
        "required": false,
        "deprecated": true,
        "description": "Legacy offset pagination. Prefer cursor.",
        "schema": { "type": "integer", "minimum": 0, "default": 0 }
    })
}

fn path_param(name: &str, description: &str) -> Value {
    json!({
        "name": name,
        "in": "path",
        "required": true,
        "description": description,
        "schema": { "type": "string" }
    })
}

pub fn spec() -> Value {
    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Glyph Qubic API",
            "version": "0.1.0",
            "description": "Resource-oriented read API over a from-scratch Qubic index fed by direct peer connections. Newest-first collections use opaque cursor pagination."
        },
        "servers": [{ "url": "/" }],
        "tags": [
            { "name": "status" },
            { "name": "epochs" },
            { "name": "ticks" },
            { "name": "transactions" },
            { "name": "events" },
            { "name": "identities" },
            { "name": "assets" },
            { "name": "analytics" },
            { "name": "stream" },
            { "name": "network" },
            { "name": "contracts" },
            { "name": "archive" },
            { "name": "search" }
        ],
        "paths": {
            "/healthz": {
                "get": {
                    "tags": ["status"],
                    "summary": "Liveness probe",
                    "responses": { "200": { "description": "Process is alive" } }
                }
            },
            "/v1/status": {
                "get": {
                    "tags": ["status"],
                    "summary": "Indexer head, ingest cursor, and counts",
                    "responses": { "200": { "description": "Current status" } }
                }
            },
            "/v1/network/stats": {
                "get": {
                    "tags": ["status"],
                    "summary": "Index-wide counts and progress",
                    "responses": {
                        "200": { "description": "Network stats", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "$ref": "#/components/schemas/NetworkStats" } } } } } },
                        "500": { "description": "Storage failure" }
                    }
                }
            },
            "/v1/epochs": {
                "get": {
                    "tags": ["epochs"],
                    "summary": "Epoch summaries, newest first",
                    "parameters": [limit_param(), cursor_param(), offset_param()],
                    "responses": { "200": { "description": "Paginated epochs" } }
                }
            },
            "/v1/epochs/{epoch}": {
                "get": {
                    "tags": ["epochs"],
                    "summary": "One epoch summary",
                    "parameters": [path_param("epoch", "Epoch number")],
                    "responses": { "200": { "description": "Epoch" }, "404": { "description": "Epoch not indexed" } }
                }
            },
            "/v1/ticks": {
                "get": {
                    "tags": ["ticks"],
                    "summary": "Ticks, newest first",
                    "parameters": [
                        { "name": "epoch", "in": "query", "schema": { "type": "integer", "minimum": 0 } },
                        limit_param(), cursor_param(), offset_param()
                    ],
                    "responses": { "200": { "description": "Paginated ticks" } }
                }
            },
            "/v1/ticks/{tick}": {
                "get": {
                    "tags": ["ticks"],
                    "summary": "Tick by number",
                    "parameters": [path_param("tick", "Global tick number")],
                    "responses": {
                        "200": { "description": "Tick" },
                        "404": { "description": "Tick not indexed" }
                    }
                }
            },
            "/v1/ticks/{tick}/transactions": {
                "get": {
                    "tags": ["ticks", "transactions"],
                    "summary": "Transactions in a tick, slot order",
                    "parameters": [path_param("tick", "Global tick number"), limit_param(), cursor_param(), offset_param()],
                    "responses": {
                        "200": { "description": "Paginated transactions" },
                        "404": { "description": "Tick not indexed" }
                    }
                }
            },
            "/v1/ticks/{tick}/events": {
                "get": {
                    "tags": ["ticks", "events"],
                    "summary": "Events in a tick, log id order",
                    "parameters": [path_param("tick", "Global tick number"), limit_param(), cursor_param(), offset_param()],
                    "responses": {
                        "200": { "description": "Paginated events" },
                        "404": { "description": "Tick not indexed" }
                    }
                }
            },
            "/v1/transactions": {
                "get": {
                    "tags": ["transactions"],
                    "summary": "Transactions, newest tick first",
                    "parameters": [
                        { "name": "epoch", "in": "query", "schema": { "type": "integer", "minimum": 0 } },
                        { "name": "tick", "in": "query", "schema": { "type": "integer", "minimum": 0 } },
                        { "name": "identity", "in": "query", "schema": { "type": "string" } },
                        limit_param(), cursor_param(), offset_param()
                    ],
                    "responses": { "200": { "description": "Paginated transactions" }, "400": { "description": "Invalid identity or cursor" } }
                },
                "post": {
                    "tags": ["transactions"],
                    "summary": "Broadcast a signed transaction to a direct peer",
                    "description": "Requires ALLOW_BROADCAST=true. The transaction is decoded and locally verified with SchnorrQ before it is sent with dejavu=0.",
                    "requestBody": {
                        "required": true,
                        "content": { "application/json": { "schema": { "$ref": "#/components/schemas/BroadcastRequest" } } }
                    },
                    "responses": {
                        "200": { "description": "Peer accepted the transaction" },
                        "400": { "description": "Invalid transaction or broadcast rejected" },
                        "403": { "description": "Broadcast disabled" },
                        "503": { "description": "No peer available" }
                    }
                }
            },
            "/v1/transactions/{hash}": {
                "get": {
                    "tags": ["transactions"],
                    "summary": "Transaction by lowercase base-26 hash",
                    "parameters": [path_param("hash", "60-character transaction hash")],
                    "responses": {
                        "200": { "description": "Transaction" },
                        "400": { "description": "Invalid hash" },
                        "404": { "description": "Transaction not indexed" }
                    }
                }
            },
            "/v1/events": {
                "get": {
                    "tags": ["events"],
                    "summary": "Typed events, newest tick first",
                    "parameters": [
                        { "name": "epoch", "in": "query", "schema": { "type": "integer", "minimum": 0 } },
                        { "name": "tick", "in": "query", "schema": { "type": "integer", "minimum": 0 } },
                        { "name": "type", "in": "query", "schema": { "type": "string" } },
                        limit_param(), cursor_param(), offset_param()
                    ],
                    "responses": { "200": { "description": "Paginated events" }, "400": { "description": "Invalid cursor" } }
                }
            },
            "/v1/identities": {
                "get": {
                    "tags": ["identities"],
                    "summary": "Identities by most recent activity",
                    "parameters": [limit_param(), cursor_param(), offset_param()],
                    "responses": { "200": { "description": "Paginated identities" } }
                }
            },
            "/v1/identities/{identity}": {
                "get": {
                    "tags": ["identities"],
                    "summary": "Identity summary and observed QU flow",
                    "parameters": [path_param("identity", "Uppercase base-26 identity")],
                    "responses": {
                        "200": { "description": "Identity" },
                        "400": { "description": "Invalid identity" },
                        "404": { "description": "Identity not indexed" }
                    }
                }
            },
            "/v1/identities/{identity}/transactions": {
                "get": {
                    "tags": ["identities", "transactions"],
                    "summary": "Transactions sent or received by an identity",
                    "parameters": [path_param("identity", "Uppercase base-26 identity"), limit_param(), cursor_param(), offset_param()],
                    "responses": {
                        "200": { "description": "Paginated transactions" },
                        "400": { "description": "Invalid identity or cursor" }
                    }
                }
            },
            "/v1/assets": {
                "get": {
                    "tags": ["assets"],
                    "summary": "Assets derived from issuance events, by issuer and name",
                    "parameters": [
                        { "name": "issuer", "in": "query", "schema": { "type": "string" } },
                        limit_param(), cursor_param(), offset_param()
                    ],
                    "responses": { "200": { "description": "Paginated assets" }, "400": { "description": "Invalid issuer or cursor" } }
                }
            },
            "/v1/assets/{issuer}/{name}": {
                "get": {
                    "tags": ["assets"],
                    "summary": "One asset by issuer and name",
                    "parameters": [path_param("issuer", "Issuer identity"), path_param("name", "Asset name")],
                    "responses": {
                        "200": { "description": "Asset" },
                        "400": { "description": "Invalid issuer" },
                        "404": { "description": "Asset not indexed" }
                    }
                }
            },
            "/v1/search": {
                "get": {
                    "tags": ["search"],
                    "summary": "Resolve a tick, transaction hash, or identity",
                    "parameters": [{ "name": "q", "in": "query", "required": true, "schema": { "type": "string" } }],
                    "responses": { "200": { "description": "Typed matches" }, "400": { "description": "Missing query" } }
                }
            },
            "/v1/transactions/batch": {
                "post": {
                    "tags": ["transactions"],
                    "summary": "Look up many transaction hashes in one request",
                    "description": "At most 100 unique hashes. Unknown hashes are reported in meta.missing instead of failing the request.",
                    "requestBody": {
                        "required": true,
                        "content": { "application/json": { "schema": { "$ref": "#/components/schemas/BatchRequest" } } }
                    },
                    "responses": {
                        "200": { "description": "Transactions found" },
                        "400": { "description": "Too many hashes or invalid input" }
                    }
                }
            },
            "/v1/analytics/activity": {
                "get": {
                    "tags": ["analytics"],
                    "summary": "Transaction and event counts per time bucket",
                    "parameters": [
                        { "name": "interval", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 86400, "default": 3600 }, "description": "Bucket width in seconds" },
                        { "name": "limit", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 500, "default": 24 } }
                    ],
                    "responses": { "200": { "description": "Activity buckets, newest first" } }
                }
            },
            "/v1/analytics/top-identities": {
                "get": {
                    "tags": ["analytics"],
                    "summary": "Identities with the most observed activity",
                    "parameters": [{ "name": "limit", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 200, "default": 25 } }],
                    "responses": { "200": { "description": "Ranked identities" } }
                }
            },
            "/v1/analytics/event-types": {
                "get": {
                    "tags": ["analytics"],
                    "summary": "Event counts grouped by decoded type",
                    "parameters": [{ "name": "limit", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 200, "default": 50 } }],
                    "responses": { "200": { "description": "Event type counts" } }
                }
            },
            "/v1/stream": {
                "get": {
                    "tags": ["stream"],
                    "summary": "Server-sent events for ticks, transactions, and events",
                    "description": "Polls the local index head only; never opens a peer connection. Sends keep-alive comments every 15 seconds and lag events when a subscriber falls behind.",
                    "parameters": [
                        { "name": "types", "in": "query", "schema": { "type": "string" }, "description": "Comma-separated subset of tick,transaction,event. The poll interval is set on the server with STREAM_POLL_MS." }
                    ],
                    "responses": {
                        "200": { "description": "text/event-stream", "content": { "text/event-stream": { "schema": { "type": "string" } } } },
                        "503": { "description": "Streaming is not enabled" }
                    }
                }
            },
            "/v1/stream/ticks": {
                "get": {
                    "tags": ["stream"],
                    "summary": "Tick events only",
                    "responses": { "200": { "description": "text/event-stream", "content": { "text/event-stream": { "schema": { "type": "string" } } } }, "503": { "description": "Streaming is not enabled" } }
                }
            },
            "/v1/stream/transactions": {
                "get": {
                    "tags": ["stream"],
                    "summary": "Transaction events only",
                    "responses": { "200": { "description": "text/event-stream", "content": { "text/event-stream": { "schema": { "type": "string" } } } }, "503": { "description": "Streaming is not enabled" } }
                }
            },
            "/v1/stream/events": {
                "get": {
                    "tags": ["stream"],
                    "summary": "Log event events only",
                    "responses": { "200": { "description": "text/event-stream", "content": { "text/event-stream": { "schema": { "type": "string" } } } }, "503": { "description": "Streaming is not enabled" } }
                }
            },
            "/v1/ws": {
                "get": {
                    "tags": ["stream"],
                    "summary": "WebSocket feed with the same event stream",
                    "responses": { "200": { "description": "WebSocket upgrade" }, "503": { "description": "Streaming is not enabled" } }
                }
            },
            "/v1/identities/{identity}/entity": {
                "get": {
                    "tags": ["identities", "network"],
                    "summary": "Entity record from a direct peer",
                    "parameters": [path_param("identity", "Uppercase base-26 identity")],
                    "responses": {
                        "200": { "description": "Entity record from the peer" },
                        "400": { "description": "Invalid identity" },
                        "404": { "description": "Peer has no entity record" },
                        "503": { "description": "No peer available" }
                    }
                }
            },
            "/v1/transactions/{hash}/network": {
                "get": {
                    "tags": ["transactions", "network"],
                    "summary": "Transaction as reported by a direct peer",
                    "parameters": [path_param("hash", "60-character transaction hash")],
                    "responses": {
                        "200": { "description": "Peer transaction record" },
                        "400": { "description": "Invalid hash" },
                        "404": { "description": "Peer has no transaction" },
                        "503": { "description": "No peer available" }
                    }
                }
            },
            "/v1/ticks/{tick}/tx-status": {
                "get": {
                    "tags": ["ticks", "network"],
                    "summary": "Transaction confirmation status from a direct peer",
                    "parameters": [path_param("tick", "Global tick number")],
                    "responses": {
                        "200": { "description": "Peer transaction status" },
                        "503": { "description": "No peer available" }
                    }
                }
            },
            "/v1/ticks/{tick}/raw": {
                "get": {
                    "tags": ["ticks", "archive"],
                    "summary": "Archived raw QBB1 tick bundle or one section",
                    "description": "Requires S3_BUCKET or ARCHIVE_DIR to be configured for the API. Without section, returns the zstd-compressed QBB1 container.",
                    "parameters": [
                        path_param("tick", "Global tick number"),
                        { "name": "section", "in": "query", "schema": { "type": "string", "enum": ["tick", "tick_data", "votes", "transactions", "log_ranges", "logs", "computors"] } }
                    ],
                    "responses": {
                        "200": { "description": "application/octet-stream", "content": { "application/octet-stream": { "schema": { "type": "string", "format": "binary" } } } },
                        "400": { "description": "Unknown section" },
                        "404": { "description": "Tick not indexed or bundle not archived" },
                        "503": { "description": "Archive not configured or unreadable" }
                    }
                }
            },
            "/v1/contracts/{contract_index}/query": {
                "post": {
                    "tags": ["contracts", "network"],
                    "summary": "Call a read-only contract function on a direct peer",
                    "parameters": [path_param("contract_index", "Contract index")],
                    "requestBody": {
                        "required": false,
                        "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ContractQueryRequest" } } }
                    },
                    "responses": {
                        "200": { "description": "Hex-encoded contract output" },
                        "400": { "description": "Invalid hex or input exceeds MAX_INPUT_SIZE" },
                        "503": { "description": "No peer available or contract call failed" }
                    }
                }
            }
        },
        "components": {
            "schemas": {
                "Problem": {
                    "type": "object",
                    "required": ["type", "title", "status", "code", "detail"],
                    "properties": {
                        "type": { "type": "string", "default": "about:blank" },
                        "title": { "type": "string" },
                        "status": { "type": "integer" },
                        "code": { "type": "string", "enum": ["bad_request", "invalid_identity", "invalid_transaction_hash", "invalid_cursor", "not_found", "forbidden", "unavailable", "internal_error", "rate_limited"] },
                        "detail": { "type": "string" }
                    }
                },
                "BatchRequest": {
                    "type": "object",
                    "required": ["hashes"],
                    "properties": {
                        "hashes": { "type": "array", "maxItems": 100, "items": { "type": "string" } }
                    }
                },
                "BroadcastRequest": {
                    "type": "object",
                    "required": ["raw"],
                    "properties": {
                        "raw": { "type": "string", "description": "Signed transaction bytes as hex" }
                    }
                },
                "ContractQueryRequest": {
                    "type": "object",
                    "properties": {
                        "inputType": { "type": "integer", "minimum": 0, "default": 0 },
                        "input": { "type": "string", "description": "Hex-encoded input, at most MAX_INPUT_SIZE bytes" }
                    }
                },
                "PageMeta": {
                    "type": "object",
                    "properties": {
                        "limit": { "type": "integer" },
                        "offset": { "type": "integer" },
                        "count": { "type": "integer" },
                        "nextOffset": { "type": ["integer", "null"] },
                        "nextCursor": { "type": ["string", "null"] },
                        "hasMore": { "type": "boolean" }
                    }
                },
                "Tick": {
                    "type": "object",
                    "properties": {
                        "epoch": { "type": "integer" }, "tick": { "type": "integer" },
                        "timestamp": { "type": "string", "format": "date-time" },
                        "computor_index": { "type": "integer" },
                        "transaction_count": { "type": "integer" },
                        "event_count": { "type": "integer" },
                        "verification": { "type": "string", "enum": ["quorum", "data", "state"] }
                    }
                },
                "Transaction": {
                    "type": "object",
                    "properties": {
                        "epoch": { "type": "integer" }, "tick": { "type": "integer" },
                        "tx_index": { "type": "integer" }, "hash": { "type": "string" },
                        "source": { "type": "string" }, "destination": { "type": "string" },
                        "amount": { "type": "integer", "format": "int64" },
                        "input_type": { "type": "integer" }, "input_data": { "type": "string" },
                        "status": { "type": "string" }, "timestamp": { "type": "string", "format": "date-time" }
                    }
                },
                "Event": {
                    "type": "object",
                    "properties": {
                        "epoch": { "type": "integer" }, "tick": { "type": "integer" },
                        "log_id": { "type": "integer", "format": "int64" },
                        "log_type": { "type": "integer" }, "type_name": { "type": "string" },
                        "tx_hash": { "type": "string" }, "tx_index": { "type": "integer" },
                        "body": { "type": "object" },
                        "log_digest": { "type": "integer", "format": "int64" },
                        "timestamp": { "type": "string", "format": "date-time" }
                    }
                },
                "Identity": {
                    "type": "object",
                    "properties": {
                        "identity": { "type": "string" }, "public_key": { "type": "string" },
                        "balance": { "type": "integer", "format": "int64" },
                        "incoming_amount": { "type": "integer", "format": "int64" },
                        "outgoing_amount": { "type": "integer", "format": "int64" },
                        "transfer_count": { "type": "integer", "format": "int64" },
                        "first_seen_tick": { "type": "integer" }, "last_seen_tick": { "type": "integer" }
                    }
                },
                "Epoch": {
                    "type": "object",
                    "properties": {
                        "epoch": { "type": "integer" }, "first_tick": { "type": "integer" },
                        "last_tick": { "type": "integer" },
                        "first_timestamp": { "type": "string", "format": "date-time" },
                        "last_timestamp": { "type": "string", "format": "date-time" },
                        "tick_count": { "type": "integer" },
                        "transaction_count": { "type": "integer" },
                        "event_count": { "type": "integer" }, "closed": { "type": "boolean" }
                    }
                },
                "Asset": {
                    "type": "object",
                    "properties": {
                        "issuer": { "type": "string" }, "name": { "type": "string" },
                        "decimals": { "type": "integer" }, "unit": { "type": "string" },
                        "managing_contract": { "type": "integer", "format": "int64" },
                        "shares": { "type": "integer", "format": "int64" },
                        "issued_tick": { "type": "integer" }, "log_id": { "type": "integer", "format": "int64" },
                        "timestamp": { "type": "string", "format": "date-time" }
                    }
                },
                "NetworkStats": {
                    "type": "object",
                    "properties": {
                        "head": { "oneOf": [{ "$ref": "#/components/schemas/Tick" }, { "type": "null" }] },
                        "epochs": { "type": "integer" }, "ticks": { "type": "integer" },
                        "transactions": { "type": "integer" }, "events": { "type": "integer" },
                        "identities": { "type": "integer" },
                        "latest_timestamp": { "type": ["string", "null"], "format": "date-time" },
                        "ingest": { "type": "object" }
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_is_openapi_31_and_covers_routes() {
        let spec = spec();
        assert_eq!(spec["openapi"], "3.1.0");
        let paths = spec["paths"].as_object().expect("paths object");
        for path in [
            "/healthz",
            "/v1/status",
            "/v1/network/stats",
            "/v1/epochs",
            "/v1/epochs/{epoch}",
            "/v1/ticks",
            "/v1/ticks/{tick}",
            "/v1/ticks/{tick}/transactions",
            "/v1/ticks/{tick}/events",
            "/v1/transactions",
            "/v1/transactions/{hash}",
            "/v1/events",
            "/v1/identities",
            "/v1/identities/{identity}",
            "/v1/identities/{identity}/transactions",
            "/v1/assets",
            "/v1/assets/{issuer}/{name}",
            "/v1/transactions/batch",
            "/v1/analytics/activity",
            "/v1/analytics/top-identities",
            "/v1/analytics/event-types",
            "/v1/stream",
            "/v1/stream/ticks",
            "/v1/stream/transactions",
            "/v1/stream/events",
            "/v1/ws",
            "/v1/identities/{identity}/entity",
            "/v1/transactions/{hash}/network",
            "/v1/ticks/{tick}/tx-status",
            "/v1/ticks/{tick}/raw",
            "/v1/contracts/{contract_index}/query",
            "/v1/search",
        ] {
            assert!(paths.contains_key(path), "missing {path}");
        }
    }
}
