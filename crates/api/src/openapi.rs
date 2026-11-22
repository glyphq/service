use serde_json::{Value, json};

fn limit_param() -> Value {
    json!({
        "name": "limit",
        "in": "query",
        "required": false,
        "description": "Page size.",
        "schema": { "type": "integer", "minimum": 1, "maximum": 200, "default": 50 }
    })
}

fn cursor_param() -> Value {
    json!({
        "name": "cursor",
        "in": "query",
        "required": false,
        "description": "Cursor from `meta.next_cursor`.",
        "schema": { "type": "string" }
    })
}

fn offset_param() -> Value {
    json!({
        "name": "offset",
        "in": "query",
        "required": false,
        "deprecated": true,
        "description": "Legacy offset.",
        "schema": { "type": "integer", "minimum": 0, "default": 0 }
    })
}

fn tick_param() -> Value {
    json!({
        "name": "tick",
        "in": "path",
        "required": true,
        "description": "Tick number.",
        "schema": { "type": "integer", "minimum": 0 }
    })
}

fn epoch_param() -> Value {
    json!({
        "name": "epoch",
        "in": "path",
        "required": true,
        "description": "Epoch number.",
        "schema": { "type": "integer", "minimum": 0 }
    })
}

fn identity_param() -> Value {
    json!({
        "name": "identity",
        "in": "path",
        "required": true,
        "description": "Uppercase base-26 identity.",
        "schema": { "type": "string" }
    })
}

fn transaction_hash_param() -> Value {
    json!({
        "name": "hash",
        "in": "path",
        "required": true,
        "description": "Lowercase base-26 transaction hash.",
        "schema": { "type": "string" }
    })
}

fn issuer_param() -> Value {
    json!({
        "name": "issuer",
        "in": "path",
        "required": true,
        "description": "Issuer identity.",
        "schema": { "type": "string" }
    })
}

fn asset_name_param() -> Value {
    json!({
        "name": "name",
        "in": "path",
        "required": true,
        "description": "Asset name, 1-7 uppercase alphanumeric characters.",
        "schema": { "type": "string", "minLength": 1, "maxLength": 7, "pattern": "^[A-Z0-9]+$" }
    })
}

fn contract_index_param() -> Value {
    json!({
        "name": "contract_index",
        "in": "path",
        "required": true,
        "description": "Contract index.",
        "schema": { "type": "integer", "minimum": 0 }
    })
}

fn data_envelope(schema: &str) -> Value {
    json!({
        "type": "object",
        "required": ["data"],
        "properties": {
            "data": { "$ref": format!("#/components/schemas/{schema}") },
            "meta": { "type": "object" }
        }
    })
}

fn info() -> Value {
    json!({
        "title": "Glyph API",
        "version": "0.4.0",
        "summary": "Verified Qubic chain data.",
        "x-logo": { "url": "/logo.png", "altText": "Glyph" },
        "description": "Glyph indexes Qubic directly from peers and verifies quorum evidence.\n\n- `/v1` reads persisted, verified history.\n- `POST /v1/transactions` submits signed bytes and tracks them internally.\n- Signed webhooks report terminal transaction state; a broadcast is not confirmation.\n- `/v1/network` reads live peer state such as system info, revenue, and oracles.\n- `/v1/live` exposes raw peer operations.\n- JSON responses use `data` and `meta`; errors use RFC 9457 problem documents.\n\nAmounts are integer QU. Identities are uppercase base-26; hashes are lowercase base-26."
    })
}

fn tags() -> Value {
    json!([
        { "name": "Status", "description": "Health and ingest progress." },
        { "name": "Epochs", "description": "Epochs and computors." },
        { "name": "Ticks", "description": "Tick data." },
        { "name": "Transactions", "description": "Indexed lookup, effects, and lifecycle." },
        { "name": "Events", "description": "Decoded contract logs." },
        { "name": "Identities", "description": "History, flow, counterparties." },
        { "name": "Wallets", "description": "Balances and asset holdings." },
        { "name": "Assets", "description": "Issuances, transfers, holders." },
        { "name": "Prices", "description": "QUBIC/USDT series." },
        { "name": "Market", "description": "External market reference." },
        { "name": "Analytics", "description": "Network aggregates." },
        { "name": "Broadcast", "description": "Submit signed transactions and validate them locally." },
        { "name": "Webhooks", "description": "Signed delivery for queued, broadcast, confirmed, expired, and failed events." },
        { "name": "Network", "description": "Index counts, peer health, and system info." },
        { "name": "Revenue", "description": "Live computor revenue scores and estimates." },
        { "name": "Oracles", "description": "Live oracle queries, subscriptions, and statistics." },
        { "name": "Stream", "description": "SSE and WebSocket feeds." },
        { "name": "Live", "description": "Direct peer reads." },
        { "name": "Archive", "description": "Raw tick bundles." },
        { "name": "Search", "description": "Cross-entity lookup." }
    ])
}

fn tag_groups() -> Value {
    json!([
        { "name": "Overview", "tags": ["Status", "Search"] },
        { "name": "Chain Data", "tags": ["Epochs", "Ticks", "Transactions", "Events", "Archive"] },
        { "name": "Accounts & Assets", "tags": ["Identities", "Wallets", "Assets"] },
        { "name": "Broadcast & Delivery", "tags": ["Broadcast", "Webhooks"] },
        { "name": "Network", "tags": ["Network", "Revenue", "Oracles"] },
        { "name": "Markets & Analytics", "tags": ["Prices", "Market", "Analytics"] },
        { "name": "Live & Stream", "tags": ["Live", "Stream"] }
    ])
}

pub fn spec() -> Value {
    json!({
        "openapi": "3.1.0",
        "info": info(),
        "servers": [{ "url": "/" }],
        "tags": tags(),
        "x-tagGroups": tag_groups(),
        "paths": paths(),
        "components": { "schemas": schemas() }
    })
}

fn paths() -> Value {
    json!({
        // Operations
        "/healthz": {
                "get": {
                    "operationId": "getHealth",
                    "tags": ["Status"],
                    "summary": "Health check",
                    "description": "Process liveness.",
                    "responses": { "200": { "description": "Process is alive" } }
                }
            },
            "/v1/status": {
                "get": {
                    "operationId": "getStatus",
                    "tags": ["Status"],
                    "summary": "Get indexer status",
                    "description": "Head, ingest cursor, throughput, lag, and backfill coverage.",
                    "responses": {
                        "200": {
                            "description": "Current status",
                            "content": { "application/json": { "schema": {
                                "type": "object",
                                "properties": {
                                    "data": { "$ref": "#/components/schemas/Status" },
                                    "meta": { "type": "object" }
                                }
                            } } }
                        }
                    }
                }
            },
            "/status": {
                "get": {
                    "operationId": "getStatusPage",
                    "tags": ["Status"],
                    "summary": "Operations dashboard",
                    "description": "HTML status page.",
                    "responses": { "200": { "description": "HTML dashboard", "content": { "text/html": { "schema": { "type": "string" } } } } }
                }
            },
            "/v1/network": {
                "get": {
                    "operationId": "getNetworkOverview",
                    "tags": ["Network"],
                    "summary": "Get network overview",
                    "description": "Indexed counts, live system info, peer health, and sync lag.",
                    "responses": {
                        "200": { "description": "Network overview", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "type": "object" }, "meta": { "type": "object" } } } } } },
                        "500": { "description": "Storage failure" }
                    }
                }
            },
            "/v1/network/index": {
                "get": {
                    "operationId": "getNetworkIndex",
                    "tags": ["Network"],
                    "summary": "Get indexed counts",
                    "description": "Entity counts and indexed tick range.",
                    "responses": {
                        "200": { "description": "Network stats", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "$ref": "#/components/schemas/NetworkStats" } } } } } },
                        "500": { "description": "Storage failure" }
                    }
                }
            },
            "/v1/network/system": {
                "get": {
                    "operationId": "getNetworkSystem",
                    "tags": ["Network"],
                    "summary": "Get live system info",
                    "description": "Peer system info with epoch start, entity counts, and thresholds.",
                    "responses": {
                        "200": { "description": "System info", "content": { "application/json": { "schema": data_envelope("NetworkSystem") } } },
                        "503": { "description": "No peer answered" }
                    }
                }
            },
            "/v1/network/peers": {
                "get": {
                    "operationId": "listNetworkPeers",
                    "tags": ["Network"],
                    "summary": "List peer health",
                    "description": "Score, latency, failures, and quarantine per configured peer.",
                    "responses": { "200": { "description": "Peer health", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "type": "array", "items": { "type": "object" } }, "meta": { "type": "object" } } } } } } }
                }
            },
            "/v1/network/revenue": {
                "get": {
                    "operationId": "listNetworkRevenue",
                    "tags": ["Revenue"],
                    "summary": "List live revenue scores",
                    "description": "Approximate per-computor scores and estimated rewards for the current epoch.",
                    "parameters": [
                        { "name": "limit", "in": "query", "required": false, "description": "Rows to return.", "schema": { "type": "integer", "minimum": 1, "maximum": 676, "default": 676 } },
                        { "name": "offset", "in": "query", "required": false, "description": "Rows to skip.", "schema": { "type": "integer", "minimum": 0, "default": 0 } },
                        { "name": "sort", "in": "query", "required": false, "description": "reward, tx_score, oracle_score, doge_score, or index.", "schema": { "type": "string", "enum": ["reward", "tx_score", "oracle_score", "doge_score", "index"] } },
                        { "name": "identity", "in": "query", "required": false, "description": "Filter to one computor identity.", "schema": { "type": "string" } }
                    ],
                    "responses": { "200": { "description": "Revenue rows", "content": { "application/json": { "schema": { "type": "object" } } } }, "400": { "description": "Invalid identity or sort" }, "503": { "description": "No peer answered" } }
                }
            },
            "/v1/network/revenue/{identity}": {
                "get": {
                    "operationId": "getNetworkRevenueIdentity",
                    "tags": ["Revenue"],
                    "summary": "Get one computor revenue",
                    "description": "Live scores for one computor identity.",
                    "parameters": [identity_param()],
                    "responses": { "200": { "description": "Computor revenue", "content": { "application/json": { "schema": data_envelope("RevenueComputor") } } }, "404": { "description": "Not a computor this epoch" } }
                }
            },
            "/v1/network/oracles": {
                "get": {
                    "operationId": "getOracleOverview",
                    "tags": ["Oracles"],
                    "summary": "Get oracle overview",
                    "description": "Query statistics, pending queries, and active subscriptions.",
                    "responses": { "200": { "description": "Oracle overview", "content": { "application/json": { "schema": { "type": "object" } } } }, "503": { "description": "No peer answered" } }
                }
            },
            "/v1/network/oracles/stats": {
                "get": {
                    "operationId": "getOracleStats",
                    "tags": ["Oracles"],
                    "summary": "Get oracle statistics",
                    "description": "Counters and average durations for one peer's oracle engine.",
                    "responses": { "200": { "description": "Oracle statistics", "content": { "application/json": { "schema": { "type": "object" } } } }, "503": { "description": "No peer answered" } }
                }
            },
            "/v1/network/oracles/queries": {
                "get": {
                    "operationId": "listOracleQueries",
                    "tags": ["Oracles"],
                    "summary": "List oracle queries",
                    "description": "Pending queries, or queries of one tick by type.",
                    "parameters": [
                        { "name": "status", "in": "query", "required": false, "description": "pending, all, user, contract, or subscription.", "schema": { "type": "string", "default": "pending" } },
                        { "name": "tick", "in": "query", "required": false, "description": "Tick for non-pending selectors. Defaults to the indexed head.", "schema": { "type": "integer" } },
                        { "name": "limit", "in": "query", "required": false, "description": "Queries to materialize.", "schema": { "type": "integer", "minimum": 1, "maximum": 100, "default": 25 } },
                        { "name": "offset", "in": "query", "required": false, "description": "Queries to skip.", "schema": { "type": "integer", "minimum": 0, "default": 0 } }
                    ],
                    "responses": { "200": { "description": "Oracle queries", "content": { "application/json": { "schema": { "type": "object" } } } }, "400": { "description": "Unknown status" }, "503": { "description": "No peer answered" } }
                }
            },
            "/v1/network/oracles/queries/{id}": {
                "get": {
                    "operationId": "getOracleQuery",
                    "tags": ["Oracles"],
                    "summary": "Get oracle query",
                    "description": "Metadata, raw query and reply, and decoded price data.",
                    "parameters": [{ "name": "id", "in": "path", "required": true, "description": "Oracle query ID.", "schema": { "type": "integer" } }],
                    "responses": { "200": { "description": "Oracle query", "content": { "application/json": { "schema": { "type": "object" } } } }, "503": { "description": "No peer answered" } }
                }
            },
            "/v1/network/oracles/subscriptions": {
                "get": {
                    "operationId": "listOracleSubscriptions",
                    "tags": ["Oracles"],
                    "summary": "List oracle subscriptions",
                    "description": "Active subscriptions, optionally filtered to one contract.",
                    "parameters": [
                        { "name": "contract", "in": "query", "required": false, "description": "Contract index.", "schema": { "type": "integer" } },
                        { "name": "limit", "in": "query", "required": false, "description": "Subscriptions to materialize.", "schema": { "type": "integer", "minimum": 1, "maximum": 100, "default": 25 } }
                    ],
                    "responses": { "200": { "description": "Oracle subscriptions", "content": { "application/json": { "schema": { "type": "object" } } } }, "400": { "description": "Invalid contract" }, "503": { "description": "No peer answered" } }
                }
            },
            "/v1/network/oracles/subscriptions/{id}": {
                "get": {
                    "operationId": "getOracleSubscription",
                    "tags": ["Oracles"],
                    "summary": "Get oracle subscription",
                    "description": "Subscription info, initial query, and contract subscribers.",
                    "parameters": [{ "name": "id", "in": "path", "required": true, "description": "Oracle subscription ID.", "schema": { "type": "integer" } }],
                    "responses": { "200": { "description": "Oracle subscription", "content": { "application/json": { "schema": { "type": "object" } } } }, "503": { "description": "No peer answered" } }
                }
            },
            "/v1/network/oracles/revenue": {
                "get": {
                    "operationId": "getOracleRevenue",
                    "tags": ["Oracles"],
                    "summary": "Get oracle revenue points",
                    "description": "676 oracle revenue points for the current epoch.",
                    "responses": { "200": { "description": "Oracle revenue", "content": { "application/json": { "schema": { "type": "object" } } } }, "503": { "description": "No peer answered" } }
                }
            },
            "/v1/transactions/inclusion-risk": {
                "get": {
                    "operationId": "getInclusionRisk",
                    "tags": ["Broadcast"],
                    "summary": "Estimate inclusion risk",
                    "description": "Heuristic from recent tick fill, target-tick distance, and balance.",
                    "parameters": [
                        { "name": "source", "in": "query", "required": false, "description": "Source identity.", "schema": { "type": "string" } },
                        { "name": "target_tick", "in": "query", "required": false, "description": "Target tick. Defaults to current plus one.", "schema": { "type": "integer" } },
                        { "name": "amount", "in": "query", "required": false, "description": "Transfer amount.", "schema": { "type": "integer", "minimum": 0 } }
                    ],
                    "responses": { "200": { "description": "Risk estimate", "content": { "application/json": { "schema": { "type": "object" } } } }, "400": { "description": "Invalid identity or amount" }, "503": { "description": "Current tick unavailable" } }
                }
            },
            "/v1/transactions/{hash}/lifecycle": {
                "get": {
                    "operationId": "getTransactionLifecycle",
                    "tags": ["Transactions"],
                    "summary": "Get transaction lifecycle",
                    "description": "Indexed, tracked, and peer state with events, effects, and money-flow.",
                    "parameters": [transaction_hash_param()],
                    "responses": { "200": { "description": "Transaction lifecycle", "content": { "application/json": { "schema": { "type": "object" } } } }, "400": { "description": "Invalid hash" }, "404": { "description": "Unknown transaction" } }
                }
            },
            "/v1/ticks/{tick}/capacity": {
                "get": {
                    "operationId": "getTickCapacity",
                    "tags": ["Ticks"],
                    "summary": "Get tick capacity",
                    "description": "Fill ratio, volume, top senders, and optional money-flow counts.",
                    "parameters": [tick_param()],
                    "responses": { "200": { "description": "Tick capacity", "content": { "application/json": { "schema": { "type": "object" } } } }, "404": { "description": "Tick not indexed" } }
                }
            },
            // Chain data
            "/v1/epochs": {
                "get": {
                    "operationId": "listEpochs",
                    "tags": ["Epochs"],
                    "summary": "List epochs",
                    "description": "Newest first.",
                    "parameters": [limit_param(), cursor_param(), offset_param()],
                    "responses": { "200": { "description": "Paginated epochs, newest first", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/EpochPage" } } } } }
                }
            },
            "/v1/epochs/{epoch}": {
                "get": {
                    "operationId": "getEpoch",
                    "tags": ["Epochs"],
                    "summary": "Get epoch",
                    "description": "One epoch.",
                    "parameters": [epoch_param()],
                    "responses": { "200": { "description": "Epoch summary", "content": { "application/json": { "schema": data_envelope("Epoch") } } }, "404": { "description": "Epoch not indexed" } }
                }
            },
            "/v1/epochs/{epoch}/computors": {
                "get": {
                    "operationId": "listEpochComputors",
                    "tags": ["Epochs"],
                    "summary": "List computors",
                    "description": "Computor set ordered by index.",
                    "parameters": [epoch_param()],
                    "responses": {
                        "200": {
                            "description": "Computor list",
                            "content": { "application/json": { "schema": {
                                "type": "object",
                                "properties": {
                                    "data": { "type": "array", "items": { "$ref": "#/components/schemas/Computor" } },
                                    "meta": { "type": "object" }
                                }
                            } } }
                        },
                        "404": { "description": "No computors indexed for the epoch" }
                    }
                }
            },
            "/v1/ticks": {
                "get": {
                    "operationId": "listTicks",
                    "tags": ["Ticks"],
                    "summary": "List ticks",
                    "description": "Newest first, filterable by epoch.",
                    "parameters": [
                        { "name": "epoch", "in": "query", "schema": { "type": "integer", "minimum": 0 } },
                        limit_param(), cursor_param(), offset_param()
                    ],
                    "responses": { "200": { "description": "Paginated ticks, newest first", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/TickPage" } } } } }
                }
            },
            "/v1/ticks/{tick}": {
                "get": {
                    "operationId": "getTick",
                    "tags": ["Ticks"],
                    "summary": "Get tick",
                    "description": "One tick header.",
                    "parameters": [tick_param()],
                    "responses": {
                        "200": { "description": "Tick header", "content": { "application/json": { "schema": data_envelope("Tick") } } },
                        "404": { "description": "Tick not indexed" }
                    }
                }
            },
            "/v1/ticks/{tick}/evidence": {
                "get": {
                    "operationId": "getTickQuorumEvidence",
                    "tags": ["Ticks"],
                    "summary": "Get quorum evidence",
                    "description": "Digests and vote counts behind the stored tick.",
                    "parameters": [tick_param()],
                    "responses": {
                        "200": {
                            "description": "Quorum evidence",
                            "content": { "application/json": { "schema": {
                                "type": "object",
                                "properties": { "data": { "$ref": "#/components/schemas/QuorumEvidence" } }
                            } } }
                        },
                        "404": { "description": "No evidence indexed for the tick" }
                    }
                }
            },
            "/v1/ticks/{tick}/transactions": {
                "get": {
                    "operationId": "listTickTransactions",
                    "tags": ["Ticks"],
                    "summary": "List tick transactions",
                    "description": "Execution-slot order.",
                    "parameters": [tick_param(), limit_param(), cursor_param(), offset_param()],
                    "responses": {
                        "200": { "description": "Paginated transactions", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/TransactionPage" } } } },
                        "404": { "description": "Tick not indexed" }
                    }
                }
            },
            "/v1/ticks/{tick}/events": {
                "get": {
                    "operationId": "listTickEvents",
                    "tags": ["Ticks"],
                    "summary": "List tick events",
                    "description": "Log-id order.",
                    "parameters": [tick_param(), limit_param(), cursor_param(), offset_param()],
                    "responses": {
                        "200": { "description": "Paginated events", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/EventPage" } } } },
                        "404": { "description": "Tick not indexed" }
                    }
                }
            },
            "/v1/transactions": {
                "get": {
                    "operationId": "listTransactions",
                    "tags": ["Transactions"],
                    "summary": "List transactions",
                    "description": "Newest first, filterable.",
                    "parameters": [
                        { "name": "epoch", "in": "query", "schema": { "type": "integer", "minimum": 0 } },
                        { "name": "tick", "in": "query", "schema": { "type": "integer", "minimum": 0 } },
                        { "name": "identity", "in": "query", "schema": { "type": "string" } },
                        limit_param(), cursor_param(), offset_param()
                    ],
                    "responses": { "200": { "description": "Paginated transactions", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/TransactionPage" } } } }, "400": { "description": "Invalid identity or cursor" } }
                },
                "post": {
                    "operationId": "broadcastTransaction",
                    "tags": ["Broadcast"],
                    "summary": "Broadcast transaction",
                    "description": "Tracks internally and serves terminal webhook events. Set `direct: true` for one peer fan-out. Requires `ALLOW_BROADCAST=true`. A broadcast is not confirmation.",
                    "requestBody": {
                        "required": true,
                        "content": { "application/json": { "schema": { "$ref": "#/components/schemas/BroadcastRequest" } } }
                    },
                    "responses": {
                        "200": { "description": "Dry run report; no peer connection was opened", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/BroadcastResult" } } } },
                        "202": { "description": "Transaction accepted for tracking, or written to at least one peer socket with direct: true", "content": { "application/json": { "schema": {
                            "oneOf": [
                                { "$ref": "#/components/schemas/BroadcastResult" },
                                { "type": "object", "required": ["data"], "properties": { "data": { "$ref": "#/components/schemas/TrackedTransaction" }, "meta": { "type": "object" } } }
                            ]
                        } } } },
                        "400": { "description": "Transaction failed local validation" },
                        "403": { "description": "Broadcast disabled" },
                        "429": { "description": "Tracking capacity is full; code tracking_full" },
                        "503": { "description": "Tracking is disabled, or no peer accepted a direct broadcast" }
                    }
                }
            },
            "/v1/transactions/{hash}": {
                "get": {
                    "operationId": "getTransaction",
                    "tags": ["Transactions"],
                    "summary": "Get transaction",
                    "description": "By hash.",
                    "parameters": [transaction_hash_param()],
                    "responses": {
                        "200": { "description": "Transaction", "content": { "application/json": { "schema": data_envelope("Transaction") } } },
                        "400": { "description": "Invalid hash" },
                        "404": { "description": "Transaction not indexed" }
                    }
                }
            },
            "/v1/transactions/{hash}/effects": {
                "get": {
                    "operationId": "getTransactionEffects",
                    "tags": ["Transactions"],
                    "summary": "Get transaction effects",
                    "description": "Transaction, tick, events, typed effects, and summary.",
                    "parameters": [transaction_hash_param()],
                    "responses": {
                        "200": { "description": "Transaction effects", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "$ref": "#/components/schemas/TransactionEffects" } } } } } },
                        "400": { "description": "Invalid transaction hash" },
                        "404": { "description": "Transaction not indexed" }
                    }
                }
            },
            "/v1/events": {
                "get": {
                    "operationId": "listEvents",
                    "tags": ["Events"],
                    "summary": "List events",
                    "description": "Newest first, filterable by type.",
                    "parameters": [
                        { "name": "epoch", "in": "query", "schema": { "type": "integer", "minimum": 0 } },
                        { "name": "tick", "in": "query", "schema": { "type": "integer", "minimum": 0 } },
                        { "name": "type", "in": "query", "schema": { "type": "string" } },
                        limit_param(), cursor_param(), offset_param()
                    ],
                    "responses": { "200": { "description": "Paginated events", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/EventPage" } } } }, "400": { "description": "Invalid cursor" } }
                }
            },
            // Accounts and assets
            "/v1/identities": {
                "get": {
                    "operationId": "listIdentities",
                    "tags": ["Identities"],
                    "summary": "List identities",
                    "description": "Most recently active first.",
                    "parameters": [limit_param(), cursor_param(), offset_param()],
                    "responses": { "200": { "description": "Paginated identities, most recently active first", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/IdentityPage" } } } } }
                }
            },
            "/v1/identities/{identity}": {
                "get": {
                    "operationId": "getIdentity",
                    "tags": ["Identities"],
                    "summary": "Get identity",
                    "description": "Observed QU flow.",
                    "parameters": [identity_param()],
                    "responses": {
                        "200": { "description": "Identity summary", "content": { "application/json": { "schema": data_envelope("Identity") } } },
                        "400": { "description": "Invalid identity" },
                        "404": { "description": "Identity not indexed" }
                    }
                }
            },
            "/v1/identities/{identity}/transactions": {
                "get": {
                    "operationId": "listIdentityTransactions",
                    "tags": ["Identities"],
                    "summary": "List identity transactions",
                    "description": "Newest first.",
                    "parameters": [identity_param(), limit_param(), cursor_param(), offset_param()],
                    "responses": {
                        "200": { "description": "Paginated transactions", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/TransactionPage" } } } },
                        "400": { "description": "Invalid identity or cursor" }
                    }
                }
            },
            "/v1/identities/{identity}/activity": {
                "get": {
                    "operationId": "getIdentityActivity",
                    "tags": ["Identities"],
                    "summary": "Get identity activity",
                    "description": "Transactions and events merged newest first.",
                    "parameters": [identity_param(), limit_param(), cursor_param(), offset_param()],
                    "responses": {
                        "200": { "description": "Paginated activity rows with kind transaction or event", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/IdentityActivityPage" } } } },
                        "400": { "description": "Invalid identity or cursor" }
                    }
                }
            },
            "/v1/identities/{identity}/counterparties": {
                "get": {
                    "operationId": "listIdentityCounterparties",
                    "tags": ["Identities"],
                    "summary": "List counterparties",
                    "description": "Top transfers by volume, excluding self-transfers.",
                    "parameters": [
                        identity_param(),
                        { "name": "direction", "in": "query", "schema": { "type": "string", "enum": ["all", "incoming", "outgoing"], "default": "all" } },
                        { "name": "limit", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 100, "default": 20 } }
                    ],
                    "responses": {
                        "200": { "description": "Ranked counterparties", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "type": "array", "items": { "$ref": "#/components/schemas/Counterparty" } } } } } } },
                        "400": { "description": "Invalid identity or direction" }
                    }
                }
            },
            "/v1/identities/{identity}/flow": {
                "get": {
                    "operationId": "getIdentityFlow",
                    "tags": ["Identities"],
                    "summary": "Get identity flow",
                    "description": "Zero-filled incoming and outgoing QU buckets.",
                    "parameters": [
                        identity_param(),
                        { "name": "interval", "in": "query", "schema": { "type": "integer", "minimum": 60, "maximum": 86400, "default": 3600 }, "description": "Bucket width in seconds" },
                        { "name": "limit", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 168 }, "description": "Number of buckets, newest first" }
                    ],
                    "responses": {
                        "200": { "description": "Flow buckets", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "type": "array", "items": { "$ref": "#/components/schemas/FlowBucket" } } } } } } },
                        "400": { "description": "Invalid identity or interval" },
                        "404": { "description": "The index has no ticks yet" }
                    }
                }
            },
            "/v1/identities/{identity}/events": {
                "get": {
                    "operationId": "listIdentityEvents",
                    "tags": ["Identities"],
                    "summary": "List identity events",
                    "description": "Newest first.",
                    "parameters": [identity_param(), limit_param(), cursor_param(), offset_param()],
                    "responses": {
                        "200": { "description": "Paginated event rows", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/EventPage" } } } },
                        "400": { "description": "Invalid identity or cursor" }
                    }
                }
            },
            "/v1/assets": {
                "get": {
                    "operationId": "listAssets",
                    "tags": ["Assets"],
                    "summary": "List assets",
                    "description": "Newest issuance first.",
                    "parameters": [
                        { "name": "issuer", "in": "query", "schema": { "type": "string" } },
                        limit_param(), cursor_param(), offset_param()
                    ],
                    "responses": { "200": { "description": "Paginated assets", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/AssetPage" } } } }, "400": { "description": "Invalid issuer or cursor" } }
                }
            },
            "/v1/assets/{issuer}/{name}": {
                "get": {
                    "operationId": "getAsset",
                    "tags": ["Assets"],
                    "summary": "Get asset",
                    "description": "By issuer and name.",
                    "parameters": [issuer_param(), asset_name_param()],
                    "responses": {
                        "200": { "description": "Asset metadata", "content": { "application/json": { "schema": data_envelope("Asset") } } },
                        "400": { "description": "Invalid issuer" },
                        "404": { "description": "Asset not indexed" }
                    }
                }
            },
            "/v1/assets/{issuer}/{name}/transfers": {
                "get": {
                    "operationId": "listAssetTransfers",
                    "tags": ["Assets"],
                    "summary": "List asset transfers",
                    "description": "Ownership and possession changes.",
                    "parameters": [
                        issuer_param(),
                        asset_name_param(),
                        { "name": "kind", "in": "query", "schema": { "type": "string", "enum": ["all", "ownership", "possession"], "default": "all" } },
                        limit_param(), cursor_param(), offset_param()
                    ],
                    "responses": {
                        "200": { "description": "Paginated transfer events", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/AssetTransferPage" } } } },
                        "400": { "description": "Invalid issuer, asset name, kind, or cursor" }
                    }
                }
            },
            "/v1/wallets/{identity}": {
                "get": {
                    "operationId": "getWallet",
                    "tags": ["Wallets"],
                    "summary": "Get wallet",
                    "description": "Indexed identity, recent transactions, and peer asset lists.",
                    "parameters": [identity_param()],
                    "responses": {
                        "200": { "description": "Wallet overview", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "$ref": "#/components/schemas/WalletOverview" }, "meta": { "type": "object" } } } } } },
                        "400": { "description": "Invalid identity" },
                        "404": { "description": "Identity is neither indexed nor in the peer spectrum" },
                        "503": { "description": "No peer available or peer lookup failed" }
                    }
                }
            },
            "/v1/wallets/{identity}/assets": {
                "get": {
                    "operationId": "listWalletAssets",
                    "tags": ["Wallets"],
                    "summary": "List wallet assets",
                    "description": "Issued, owned, and possessed.",
                    "parameters": [
                        identity_param(),
                        { "name": "kind", "in": "query", "required": false, "schema": { "type": "string", "enum": ["issued", "owned", "possessed", "all"], "default": "all" } }
                    ],
                    "responses": {
                        "200": { "description": "Typed asset lists", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "$ref": "#/components/schemas/WalletAssets" }, "meta": { "type": "object" } } } } } },
                        "400": { "description": "Invalid identity or asset kind" },
                        "503": { "description": "No peer available or peer lookup failed" }
                    }
                }
            },
            "/v1/live/wallets/{identity}": {
                "get": {
                    "operationId": "getLiveWallet",
                    "tags": ["Live"],
                    "summary": "Get live wallet",
                    "description": "Point-in-time peer snapshot; not cached.",
                    "parameters": [identity_param()],
                    "responses": {
                        "200": { "description": "Wallet snapshot", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "$ref": "#/components/schemas/WalletSnapshot" }, "meta": { "type": "object" } } } } } },
                        "400": { "description": "Invalid identity" },
                        "503": { "description": "No peer available or peer lookup failed" }
                    }
                }
            },
            "/v1/assets/{issuer}/{name}/holders": {
                "get": {
                    "operationId": "listAssetHolders",
                    "tags": ["Assets"],
                    "summary": "List asset holders",
                    "description": "Peer ownership snapshot, sorted by shares.",
                    "parameters": [
                        issuer_param(),
                        asset_name_param(),
                        { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 100 } }
                    ],
                    "responses": {
                        "200": { "description": "Holder snapshot; an empty list is valid", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "type": "array", "items": { "$ref": "#/components/schemas/AssetHolder" } }, "meta": { "type": "object" } } } } } },
                        "400": { "description": "Invalid issuer, asset name, or limit" },
                        "503": { "description": "No peer available or asset lookup failed" }
                    }
                }
            },
            // Lookup and analytics
            "/v1/search": {
                "get": {
                    "operationId": "search",
                    "tags": ["Search"],
                    "summary": "Search",
                    "description": "Resolve ticks, hashes, identities, and assets.",
                    "parameters": [{ "name": "q", "in": "query", "required": true, "schema": { "type": "string" } }],
                    "responses": { "200": { "description": "Typed matches", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/SearchResults" } } } }, "400": { "description": "Missing query" } }
                }
            },
            "/v1/transactions/batch": {
                "post": {
                    "operationId": "batchTransactions",
                    "tags": ["Transactions"],
                    "summary": "Batch transaction lookup",
                    "description": "Up to 100 hashes.",
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
                    "operationId": "getActivity",
                    "tags": ["Analytics"],
                    "summary": "Get activity",
                    "description": "Bucketed counts.",
                    "parameters": [
                        { "name": "interval", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 86400, "default": 3600 }, "description": "Bucket width in seconds" },
                        { "name": "limit", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 500, "default": 24 } }
                    ],
                    "responses": { "200": { "description": "Activity buckets, newest first", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ActivityBucketPage" } } } } }
                }
            },
            "/v1/analytics/top-identities": {
                "get": {
                    "operationId": "listTopIdentities",
                    "tags": ["Analytics"],
                    "summary": "List top identities",
                    "description": "By transfer count.",
                    "parameters": [{ "name": "limit", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 200, "default": 25 } }],
                    "responses": { "200": { "description": "Ranked identities", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/IdentityPage" } } } } }
                }
            },
            "/v1/analytics/event-types": {
                "get": {
                    "operationId": "listEventTypes",
                    "tags": ["Analytics"],
                    "summary": "Get event types",
                    "description": "Counts by type.",
                    "parameters": [{ "name": "limit", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 200, "default": 50 } }],
                    "responses": { "200": { "description": "Event type counts, highest first", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/EventTypePage" } } } } }
                }
            },
            // Streaming
            "/v1/stream": {
                "get": {
                    "operationId": "streamAll",
                    "tags": ["Stream"],
                    "summary": "Stream all events",
                    "description": "SSE feed for ticks, transactions, events, prices, and market.",
                    "parameters": [
                        { "name": "types", "in": "query", "schema": { "type": "string" }, "description": "Comma-separated subset of tick,transaction,event,price,market. The poll interval is set on the server with STREAM_POLL_MS." },
                        { "name": "identity", "in": "query", "schema": { "type": "string" }, "description": "Only emit transactions and events that mention this identity. Tick, price, and market events are suppressed while this filter is set." },
                        { "name": "asset", "in": "query", "schema": { "type": "string" }, "description": "Only emit asset events for {issuer}/{name}." }
                    ],
                    "responses": {
                        "200": { "description": "text/event-stream", "content": { "text/event-stream": { "schema": { "type": "string" } } } },
                        "503": { "description": "Streaming is not enabled" }
                    }
                }
            },
            "/v1/stream/ticks": {
                "get": {
                    "operationId": "streamTicks",
                    "tags": ["Stream"],
                    "summary": "Stream ticks",
                    "description": "Tick headers as indexed.",
                    "responses": { "200": { "description": "text/event-stream", "content": { "text/event-stream": { "schema": { "type": "string" } } } }, "503": { "description": "Streaming is not enabled" } }
                }
            },
            "/v1/stream/transactions": {
                "get": {
                    "operationId": "streamTransactions",
                    "tags": ["Stream"],
                    "summary": "Stream transactions",
                    "description": "Transaction rows as indexed.",
                    "responses": { "200": { "description": "text/event-stream", "content": { "text/event-stream": { "schema": { "type": "string" } } } }, "503": { "description": "Streaming is not enabled" } }
                }
            },
            "/v1/stream/events": {
                "get": {
                    "operationId": "streamEvents",
                    "tags": ["Stream"],
                    "summary": "Stream events",
                    "description": "Decoded events as indexed.",
                    "responses": { "200": { "description": "text/event-stream", "content": { "text/event-stream": { "schema": { "type": "string" } } } }, "503": { "description": "Streaming is not enabled" } }
                }
            },
            "/v1/stream/prices": {
                "get": {
                    "operationId": "streamPrices",
                    "tags": ["Stream"],
                    "summary": "Stream prices",
                    "description": "Price points as stored.",
                    "responses": { "200": { "description": "text/event-stream" }, "503": { "description": "Streaming is not enabled" } }
                }
            },
            "/v1/stream/market": {
                "get": {
                    "operationId": "streamMarket",
                    "tags": ["Stream"],
                    "summary": "Stream market",
                    "description": "Market snapshots as refreshed.",
                    "responses": { "200": { "description": "text/event-stream" }, "503": { "description": "Streaming is not enabled" } }
                }
            },
            "/v1/ws": {
                "get": {
                    "operationId": "streamWebSocket",
                    "tags": ["Stream"],
                    "summary": "WebSocket stream",
                    "description": "Same events over WebSocket.",
                    "responses": { "200": { "description": "WebSocket upgrade" }, "503": { "description": "Streaming is not enabled" } }
                }
            },
            // Live peer routes
            "/v1/live/identities/{identity}": {
                "get": {
                    "operationId": "getLiveIdentity",
                    "tags": ["Live"],
                    "summary": "Get live identity",
                    "description": "Peer spectrum record, cached for up to two seconds.",
                    "parameters": [identity_param()],
                    "responses": {
                        "200": { "description": "Live identity record from the peer", "content": { "application/json": { "schema": data_envelope("LiveIdentity") } } },
                        "400": { "description": "Invalid identity" },
                        "404": { "description": "Peer has no entity record" },
                        "503": { "description": "No peer available" }
                    }
                }
            },
            "/v1/live/transactions/{hash}": {
                "get": {
                    "operationId": "getLiveTransaction",
                    "tags": ["Live"],
                    "summary": "Get live transaction",
                    "description": "Peer transaction lookup by hash.",
                    "parameters": [transaction_hash_param()],
                    "responses": {
                        "200": { "description": "Transaction as reported by the peer", "content": { "application/json": { "schema": data_envelope("Transaction") } } },
                        "400": { "description": "Invalid hash" },
                        "404": { "description": "Peer has no transaction" },
                        "503": { "description": "No peer available" }
                    }
                }
            },
            "/v1/live/transactions/{hash}/status": {
                "get": {
                    "operationId": "getLiveTransactionStatus",
                    "tags": ["Live"],
                    "summary": "Get live transaction status",
                    "description": "Peer confirmation and money flow. `money_flew` may be null.",
                    "parameters": [transaction_hash_param()],
                    "responses": {
                        "200": { "description": "Peer transaction status", "content": { "application/json": { "schema": data_envelope("TransactionNetworkStatus") } } },
                        "400": { "description": "Invalid hash" },
                        "404": { "description": "Peer has no confirmed transaction" },
                        "503": { "description": "No peer available or transaction lookup failed" }
                    }
                }
            },
            "/v1/transactions/validate": {
                "post": {
                    "operationId": "validateTransaction",
                    "tags": ["Broadcast"],
                    "summary": "Validate transaction",
                    "description": "Local decode, size, amount, and signature checks.",
                    "requestBody": {
                        "required": true,
                        "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ValidateRequest" } } }
                    },
                    "responses": {
                        "200": { "description": "Validation report; data.valid is false when a check fails", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/TransactionValidation" } } } },
                        "400": { "description": "Raw bytes are not hex or do not decode as a transaction" }
                    }
                }
            },
            "/v1/transactions/{hash}/status": {
                "get": {
                    "operationId": "getTransactionStatus",
                    "tags": ["Transactions"],
                    "summary": "Get transaction status",
                    "description": "Legacy status view. Use transaction lifecycle for events, effects, tracking, and timeline.",
                    "deprecated": true,
                    "parameters": [transaction_hash_param()],
                    "responses": {
                        "200": { "description": "Indexed and/or peer transaction status" },
                        "400": { "description": "Invalid hash" },
                        "404": { "description": "Unknown to both index and peer" },
                        "503": { "description": "No peer and no indexed row" }
                    }
                }
            },
            "/v1/live/ticks/{tick}/transactions": {
                "get": {
                    "operationId": "listLiveTickTransactions",
                    "tags": ["Live"],
                    "summary": "List live tick transactions",
                    "description": "Peer-side status for one tick.",
                    "parameters": [tick_param()],
                    "responses": {
                        "200": { "description": "Peer transaction status", "content": { "application/json": { "schema": data_envelope("TickTransactionStatus") } } },
                        "503": { "description": "No peer available" }
                    }
                }
            },
            "/v1/ticks/{tick}/raw": {
                "get": {
                    "operationId": "downloadTickBundle",
                    "tags": ["Archive"],
                    "summary": "Download raw bundle",
                    "description": "Zstd QBB1 bundle or one section. Votes require `ARCHIVE_VOTES=true`; computors use the epoch sidecar.",
                    "parameters": [
                        tick_param(),
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
            "/v1/live/contracts/{contract_index}/query": {
                "post": {
                    "operationId": "queryContract",
                    "tags": ["Live"],
                    "summary": "Query contract",
                    "description": "Raw contract function call against a peer.",
                    "parameters": [contract_index_param()],
                    "requestBody": {
                        "required": false,
                        "content": { "application/json": { "schema": { "$ref": "#/components/schemas/ContractQueryRequest" } } }
                    },
                    "responses": {
                        "200": { "description": "Hex-encoded contract output", "content": { "application/json": { "schema": data_envelope("ContractQueryResult") } } },
                        "400": { "description": "Invalid hex or input exceeds MAX_INPUT_SIZE" },
                        "503": { "description": "No peer available or contract call failed" }
                    }
                }
            },
            // Broadcast and webhooks
            "/v1/webhooks": {
                "get": {
                    "operationId": "listWebhooks",
                    "tags": ["Webhooks"],
                    "summary": "List webhooks",
                    "description": "Active registrations.",
                    "responses": {
                        "200": {
                            "description": "Webhook registrations",
                            "content": { "application/json": { "schema": {
                                "type": "object",
                                "required": ["data"],
                                "properties": {
                                    "data": { "type": "array", "items": { "$ref": "#/components/schemas/Webhook" } },
                                    "meta": { "type": "object" }
                                }
                            } } }
                        }
                    }
                },
                "post": {
                    "operationId": "registerWebhook",
                    "tags": ["Webhooks"],
                    "summary": "Register webhook",
                    "description": "Signed at-least-once delivery for tracked transaction events. Secrets and full URLs are never returned.",
                    "requestBody": {
                        "required": true,
                        "content": { "application/json": { "schema": { "$ref": "#/components/schemas/WebhookRegistration" } } }
                    },
                    "responses": {
                        "201": { "description": "Webhook registered", "content": { "application/json": { "schema": data_envelope("Webhook") } } },
                        "400": { "description": "Invalid URL, secret, event, or filter, including a private target without WEBHOOK_ALLOW_PRIVATE" }
                    }
                }
            },
            "/v1/webhooks/{id}": {
                "delete": {
                    "operationId": "deleteWebhook",
                    "tags": ["Webhooks"],
                    "summary": "Deactivate webhook",
                    "description": "Stops further deliveries.",
                    "parameters": [{ "name": "id", "in": "path", "required": true, "schema": { "type": "string" } }],
                    "responses": {
                        "200": { "description": "Webhook deactivated", "content": { "application/json": { "schema": data_envelope("Webhook") } } },
                        "404": { "description": "Webhook not found" }
                    }
                }
            },
            "/v1/webhooks/{id}/deliveries": {
                "get": {
                    "operationId": "listWebhookDeliveries",
                    "tags": ["Webhooks"],
                    "summary": "List deliveries",
                    "description": "Delivery attempts, newest first.",
                    "parameters": [
                        { "name": "id", "in": "path", "required": true, "schema": { "type": "string" } },
                        { "name": "limit", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 500, "default": 50 } }
                    ],
                    "responses": {
                        "200": {
                            "description": "Deliveries",
                            "content": { "application/json": { "schema": {
                                "type": "object",
                                "required": ["data"],
                                "properties": {
                                    "data": { "type": "array", "items": { "$ref": "#/components/schemas/WebhookDelivery" } },
                                    "meta": { "type": "object" }
                                }
                            } } }
                        },
                        "404": { "description": "Webhook not found" }
                    }
                }
            },
            // Prices and market
            "/v1/prices": {
                "get": {
                    "operationId": "listPrices",
                    "tags": ["Prices"],
                    "summary": "List prices",
                    "description": "Hourly QUBIC/USDT points, oldest first.",
                    "parameters": [
                        { "name": "from_hour", "in": "query", "schema": { "type": "integer", "minimum": 0 }, "description": "First core price hour, inclusive" },
                        { "name": "limit", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 168 } }
                    ],
                    "responses": {
                        "200": { "description": "Price points, ascending", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/PricePointPage" } } } },
                        "500": { "description": "Storage failure" }
                    }
                }
            },
            "/v1/prices/history": {
                "get": {
                    "operationId": "getPriceHistory",
                    "tags": ["Prices"],
                    "summary": "Get price history",
                    "description": "Trailing window.",
                    "parameters": [{ "name": "hours", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 24 } }],
                    "responses": { "200": { "description": "Ascending price window", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/PricePointPage" } } } } }
                }
            },
            "/v1/prices/latest": {
                "get": {
                    "operationId": "getLatestPrice",
                    "tags": ["Prices"],
                    "summary": "Get latest price",
                    "description": "Newest stored point.",
                    "responses": {
                        "200": { "description": "Latest price", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "$ref": "#/components/schemas/PricePoint" } } } } } },
                        "404": { "description": "No price data yet" }
                    }
                }
            },
            "/v1/prices/snapshot": {
                "get": {
                    "operationId": "getPriceSnapshot",
                    "tags": ["Prices"],
                    "summary": "Get price snapshot",
                    "description": "Metadata and latest point.",
                    "responses": {
                        "200": { "description": "Price snapshot; data is null before the first poll", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "oneOf": [{ "$ref": "#/components/schemas/PriceSnapshot" }, { "type": "null" }] }, "meta": { "type": "object" } } } } } },
                        "500": { "description": "Storage failure" }
                    }
                }
            },
            "/v1/market": {
                "get": {
                    "operationId": "getMarket",
                    "tags": ["Market"],
                    "summary": "Get market snapshot",
                    "description": "Cached external reference.",
                    "responses": {
                        "200": { "description": "Market snapshot", "content": { "application/json": { "schema": data_envelope("MarketSnapshot") } } },
                        "503": { "description": "No market snapshot has been fetched yet" }
                    }
                }
            }
    })
}

fn schemas() -> Value {
    json!({
        // Errors and request bodies
        "Problem": {
                    "type": "object",
                    "required": ["type", "title", "status", "code", "detail"],
                    "properties": {
                        "type": { "type": "string", "default": "about:blank" },
                        "title": { "type": "string" },
                        "status": { "type": "integer" },
                        "code": { "type": "string", "enum": ["bad_request", "invalid_identity", "invalid_transaction_hash", "invalid_cursor", "not_found", "conflict", "tracking_full", "forbidden", "unavailable", "internal_error", "rate_limited"] },
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
                        "raw": { "type": "string", "description": "Signed transaction hex." },
                        "dry_run": { "type": "boolean", "default": false, "description": "Check without broadcasting." },
                        "peers": { "type": "integer", "minimum": 1, "maximum": 8, "description": "Peer fan-out." },
                        "direct": { "type": "boolean", "default": false, "description": "Bypass internal tracking." },
                        "webhook_url": { "type": "string", "description": "Optional webhook for this transaction." },
                        "webhook_secret": { "type": "string", "description": "Webhook signing secret." }
                    }
                },
                "TrackedTransaction": {
                    "type": "object",
                    "required": ["hash", "source", "destination", "amount", "tick", "status", "attempts", "created_at", "updated_at"],
                    "properties": {
                        "hash": { "type": "string", "description": "Transaction hash." },
                        "raw": { "type": "string", "description": "Signed transaction hex." },
                        "source": { "type": "string", "description": "Source identity." },
                        "destination": { "type": "string", "description": "Destination identity." },
                        "amount": { "type": "integer", "format": "int64" },
                        "tick": { "type": "integer", "description": "Target tick." },
                        "target_tick": { "type": "integer", "description": "Scheduled execution tick." },
                        "peers": { "type": "integer", "description": "Requested peers." },
                        "status": { "type": "string", "enum": ["queued", "broadcast", "confirmed", "expired", "failed"] },
                        "attempts": { "type": "integer", "description": "Attempts so far." },
                        "broadcast_peers": { "type": "integer", "description": "Peers that accepted the packet." },
                        "broadcast_at": { "type": ["string", "null"], "format": "date-time" },
                        "confirmed_at": { "type": ["string", "null"], "format": "date-time" },
                        "confirmed_tick": { "type": ["integer", "null"], "description": "Indexed confirmation tick." },
                        "error": { "type": ["string", "null"], "description": "Last error." },
                        "created_at": { "type": "string", "format": "date-time" },
                        "updated_at": { "type": "string", "format": "date-time" }
                    }
                },
                "WebhookRegistration": {
                    "type": "object",
                    "required": ["url", "secret"],
                    "properties": {
                        "url": { "type": "string", "format": "uri", "description": "Delivery URL." },
                        "secret": { "type": "string", "minLength": 16, "description": "HMAC signing key." },
                        "events": {
                            "type": "array",
                            "description": "Events to deliver. Empty means all.",
                            "items": { "type": "string", "enum": ["transaction.queued", "transaction.broadcast", "transaction.confirmed", "transaction.expired", "transaction.failed"] }
                        },
                        "transaction_hashes": { "type": "array", "description": "Hashes to match. Empty means all.", "items": { "type": "string" } },
                        "identity": { "type": "string", "description": "Source identity to match. Empty means all." }
                    }
                },
                "Webhook": {
                    "type": "object",
                    "required": ["id", "url_hint", "active", "created_at"],
                    "properties": {
                        "id": { "type": "string" },
                        "url_hint": { "type": "string", "description": "Scheme and host only; the path and query are redacted." },
                        "events": { "type": "array", "items": { "type": "string" } },
                        "transaction_hashes": { "type": "array", "items": { "type": "string" } },
                        "identity": { "type": "string", "description": "Empty matches all identities." },
                        "active": { "type": "boolean" },
                        "created_at": { "type": "string", "format": "date-time" },
                        "updated_at": { "type": "string", "format": "date-time" }
                    }
                },
                "WebhookDelivery": {
                    "type": "object",
                    "required": ["id", "webhook_id", "event", "attempt", "status", "created_at"],
                    "properties": {
                        "id": { "type": "string" },
                        "webhook_id": { "type": "string" },
                        "event": { "type": "string", "enum": ["transaction.queued", "transaction.broadcast", "transaction.confirmed", "transaction.expired", "transaction.failed"] },
                        "transaction_hash": { "type": "string" },
                        "status": { "type": "string", "enum": ["pending", "delivered", "failed"] },
                        "attempt": { "type": "integer", "description": "Completed attempts." },
                        "next_attempt_at": { "type": ["string", "null"], "format": "date-time" },
                        "response_status": { "type": ["integer", "null"], "description": "Receiver HTTP status." },
                        "error": { "type": ["string", "null"] },
                        "payload": { "type": "string", "description": "Exact JSON body." },
                        "created_at": { "type": "string", "format": "date-time" }
                    }
                },
                "BroadcastResult": {
                    "type": "object",
                    "required": ["data"],
                    "properties": {
                        "data": {
                            "type": "object",
                            "properties": {
                                "valid": { "type": "boolean", "description": "Dry run only." },
                                "dry_run": { "type": "boolean", "description": "Dry run only." },
                                "target_peers": { "type": "integer", "description": "Dry run only." },
                                "sent": { "type": "integer", "description": "Peers that accepted the packet." },
                                "targeted": { "type": "integer", "description": "Peers attempted." },
                                "transaction": {
                                    "type": "object",
                                    "properties": {
                                        "hash": { "type": "string" },
                                        "source": { "type": "string" },
                                        "destination": { "type": "string" },
                                        "amount": { "type": "integer", "format": "int64" },
                                        "tick": { "type": "integer" },
                                        "input_type": { "type": "integer" },
                                        "input_data": { "type": "string" },
                                        "input_bytes": { "type": "integer" },
                                        "total_bytes": { "type": "integer" }
                                    }
                                },
                                "checks": { "type": "array", "items": { "$ref": "#/components/schemas/TransactionValidationCheck" } },
                                "peers": {
                                    "type": "array",
                                    "description": "One entry per peer.",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "peer": { "type": "string" },
                                            "sent": { "type": "boolean" },
                                            "latency_ms": { "type": "integer" },
                                            "error": { "type": ["string", "null"] }
                                        }
                                    }
                                },
                                "warnings": { "type": "array", "items": { "type": "string" } }
                            }
                        },
                        "meta": { "type": "object" }
                    }
                },
                "ValidateRequest": {
                    "type": "object",
                    "required": ["raw"],
                    "properties": {
                        "raw": { "type": "string", "description": "Signed transaction hex." }
                    }
                },
                "TransactionValidationCheck": {
                    "type": "object",
                    "required": ["name", "passed"],
                    "properties": {
                        "name": { "type": "string", "enum": ["amount", "input_size", "encoding", "signature"] },
                        "passed": { "type": "boolean" },
                        "detail": { "type": "string" }
                    }
                },
                "TransactionValidation": {
                    "type": "object",
                    "required": ["data"],
                    "properties": {
                        "data": {
                            "type": "object",
                            "required": ["valid", "hash", "checks"],
                            "properties": {
                                "valid": { "type": "boolean" },
                                "hash": { "type": "string" },
                                "source": { "type": "string" },
                                "destination": { "type": "string" },
                                "amount": { "type": "integer", "format": "int64" },
                                "tick": { "type": "integer" },
                                "input_type": { "type": "integer" },
                                "input_data": { "type": "string" },
                                "input_bytes": { "type": "integer" },
                                "total_bytes": { "type": "integer" },
                                "checks": { "type": "array", "items": { "$ref": "#/components/schemas/TransactionValidationCheck" } }
                            }
                        },
                        "meta": { "type": "object" }
                    }
                },
                "TransactionNetworkStatus": {
                    "type": "object",
                    "required": ["hash", "confirmed", "execution"],
                    "properties": {
                        "hash": { "type": "string" },
                        "tick": { "type": "integer" },
                        "source": { "type": "string" },
                        "destination": { "type": "string" },
                        "amount": { "type": "integer", "format": "int64" },
                        "input_type": { "type": "integer" },
                        "input_data": { "type": "string" },
                        "confirmed": { "type": "boolean" },
                        "money_flew": { "type": ["boolean", "null"] },
                        "execution": { "type": "string", "enum": ["transferred", "no_effect", "unknown"] },
                        "current_tick": { "type": ["integer", "null"] },
                        "confirmations": { "type": ["integer", "null"] }
                    }
                },
                "ContractQueryRequest": {
                    "type": "object",
                    "properties": {
                        "input_type": { "type": "integer", "minimum": 0, "default": 0 },
                        "input": { "type": "string", "description": "Hex-encoded input, at most MAX_INPUT_SIZE bytes" }
                    }
                },
        // Pagination and list envelopes
        "PageMeta": {
                    "type": "object",
                    "properties": {
                        "limit": { "type": "integer" },
                        "offset": { "type": "integer" },
                        "count": { "type": "integer" },
                        "next_offset": { "type": ["integer", "null"] },
                        "next_cursor": { "type": ["string", "null"] },
                        "has_more": { "type": "boolean" }
                    }
                },
                "TickPage": {
                    "type": "object",
                    "required": ["data", "meta"],
                    "properties": {
                        "data": { "type": "array", "items": { "$ref": "#/components/schemas/Tick" } },
                        "meta": { "$ref": "#/components/schemas/PageMeta" }
                    }
                },
                "EpochPage": {
                    "type": "object",
                    "required": ["data", "meta"],
                    "properties": {
                        "data": { "type": "array", "items": { "$ref": "#/components/schemas/Epoch" } },
                        "meta": { "$ref": "#/components/schemas/PageMeta" }
                    }
                },
                "TransactionPage": {
                    "type": "object",
                    "required": ["data", "meta"],
                    "properties": {
                        "data": { "type": "array", "items": { "$ref": "#/components/schemas/Transaction" } },
                        "meta": { "$ref": "#/components/schemas/PageMeta" }
                    }
                },
                "EventPage": {
                    "type": "object",
                    "required": ["data", "meta"],
                    "properties": {
                        "data": { "type": "array", "items": { "$ref": "#/components/schemas/Event" } },
                        "meta": { "$ref": "#/components/schemas/PageMeta" }
                    }
                },
                "IdentityPage": {
                    "type": "object",
                    "required": ["data", "meta"],
                    "properties": {
                        "data": { "type": "array", "items": { "$ref": "#/components/schemas/Identity" } },
                        "meta": { "$ref": "#/components/schemas/PageMeta" }
                    }
                },
                "IdentityActivityItem": {
                    "type": "object",
                    "required": ["kind"],
                    "properties": {
                        "kind": { "type": "string", "enum": ["transaction", "event"] },
                        "tick": { "type": "integer" },
                        "timestamp": { "type": "string", "format": "date-time" },
                        "transaction": { "$ref": "#/components/schemas/Transaction" },
                        "event": { "$ref": "#/components/schemas/Event" }
                    }
                },
                "IdentityActivityPage": {
                    "type": "object",
                    "required": ["data", "meta"],
                    "properties": {
                        "data": { "type": "array", "items": { "$ref": "#/components/schemas/IdentityActivityItem" } },
                        "meta": { "$ref": "#/components/schemas/PageMeta" }
                    }
                },
                "ActivityBucket": {
                    "type": "object",
                    "required": ["bucket", "transactions", "events", "ticks"],
                    "properties": {
                        "bucket": { "type": "string", "format": "date-time" },
                        "transactions": { "type": "integer" },
                        "events": { "type": "integer" },
                        "ticks": { "type": "integer" },
                        "volume": { "type": "integer", "format": "int64" }
                    }
                },
                "ActivityBucketPage": {
                    "type": "object",
                    "required": ["data", "meta"],
                    "properties": {
                        "data": { "type": "array", "items": { "$ref": "#/components/schemas/ActivityBucket" } },
                        "meta": { "type": "object" }
                    }
                },
                "EventTypeCount": {
                    "type": "object",
                    "required": ["type", "count"],
                    "properties": {
                        "type": { "type": "string" },
                        "count": { "type": "integer", "format": "int64" }
                    }
                },
                "EventTypePage": {
                    "type": "object",
                    "required": ["data", "meta"],
                    "properties": {
                        "data": { "type": "array", "items": { "$ref": "#/components/schemas/EventTypeCount" } },
                        "meta": { "type": "object" }
                    }
                },
                "AssetPage": {
                    "type": "object",
                    "required": ["data", "meta"],
                    "properties": {
                        "data": { "type": "array", "items": { "$ref": "#/components/schemas/Asset" } },
                        "meta": { "$ref": "#/components/schemas/PageMeta" }
                    }
                },
                "AssetTransferPage": {
                    "type": "object",
                    "required": ["data", "meta"],
                    "properties": {
                        "data": { "type": "array", "items": { "$ref": "#/components/schemas/AssetTransfer" } },
                        "meta": { "$ref": "#/components/schemas/PageMeta" }
                    }
                },
                "PricePointPage": {
                    "type": "object",
                    "required": ["data", "meta"],
                    "properties": {
                        "data": { "type": "array", "items": { "$ref": "#/components/schemas/PricePoint" } },
                        "meta": { "type": "object" }
                    }
                },
                "SearchResult": {
                    "type": "object",
                    "required": ["type", "value"],
                    "properties": {
                        "type": { "type": "string", "enum": ["tick", "epoch", "transaction", "identity", "asset"] },
                        "value": { "type": "object" }
                    }
                },
                "SearchResults": {
                    "type": "object",
                    "required": ["data", "meta"],
                    "properties": {
                        "data": { "type": "array", "items": { "$ref": "#/components/schemas/SearchResult" } },
                        "meta": { "type": "object" }
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
                        "target_tick": { "type": "integer", "description": "Scheduled execution tick." },
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
        // Analytics resources
        "Counterparty": {
                    "type": "object",
                    "required": ["identity", "incoming_amount", "outgoing_amount", "net_amount", "transaction_count"],
                    "properties": {
                        "identity": { "type": "string" },
                        "incoming_amount": { "type": "integer", "format": "int64" },
                        "outgoing_amount": { "type": "integer", "format": "int64" },
                        "net_amount": { "type": "integer", "format": "int64" },
                        "transaction_count": { "type": "integer", "format": "int64" },
                        "first_tick": { "type": "integer" },
                        "last_tick": { "type": "integer" }
                    }
                },
                "FlowBucket": {
                    "type": "object",
                    "required": ["bucket", "incoming_amount", "outgoing_amount", "net_amount", "transaction_count"],
                    "properties": {
                        "bucket": { "type": "string", "format": "date-time" },
                        "incoming_amount": { "type": "integer", "format": "int64" },
                        "outgoing_amount": { "type": "integer", "format": "int64" },
                        "net_amount": { "type": "integer", "format": "int64" },
                        "transaction_count": { "type": "integer", "format": "int64" },
                        "counterparty_count": { "type": "integer", "format": "int64" }
                    }
                },
                "AssetTransfer": {
                    "type": "object",
                    "required": ["epoch", "tick", "log_id", "kind", "issuer", "name", "from", "to", "shares"],
                    "properties": {
                        "epoch": { "type": "integer" },
                        "tick": { "type": "integer" },
                        "log_id": { "type": "integer", "format": "int64" },
                        "kind": { "type": "string", "enum": ["ownership", "possession"] },
                        "hash": { "type": "string" },
                        "tx_index": { "type": "integer" },
                        "issuer": { "type": "string" },
                        "name": { "type": "string" },
                        "from": { "type": "string" },
                        "to": { "type": "string" },
                        "shares": { "type": "integer", "format": "int64" },
                        "managing_contract": { "type": "integer", "format": "int64" },
                        "timestamp": { "type": "string", "format": "date-time" }
                    }
                },
                "TransactionEffect": {
                    "type": "object",
                    "required": ["kind", "tick", "log_id"],
                    "properties": {
                        "kind": { "type": "string", "enum": ["qu_transfer", "asset_ownership_change", "asset_possession_change", "burning", "contract_error", "contract_warning", "contract_information", "contract_debug", "custom_message"] },
                        "tick": { "type": "integer" },
                        "log_id": { "type": "integer", "format": "int64" },
                        "tx_hash": { "type": "string" },
                        "from": { "type": "string" },
                        "to": { "type": "string" },
                        "amount": { "type": "integer", "format": "int64" },
                        "issuer": { "type": "string" },
                        "name": { "type": "string" },
                        "shares": { "type": "integer", "format": "int64" },
                        "managing_contract": { "type": "integer", "format": "int64" },
                        "contract_index": { "type": "integer" },
                        "message_type": { "type": "integer" },
                        "op_code": { "type": "string" },
                        "data": { "type": "string" }
                    }
                },
                "TransactionEffects": {
                    "type": "object",
                    "required": ["transaction", "events", "effects", "summary"],
                    "properties": {
                        "transaction": { "$ref": "#/components/schemas/Transaction" },
                        "tick": { "oneOf": [{ "$ref": "#/components/schemas/Tick" }, { "type": "null" }] },
                        "events": { "type": "array", "items": { "$ref": "#/components/schemas/Event" } },
                        "effects": { "type": "array", "items": { "$ref": "#/components/schemas/TransactionEffect" } },
                        "summary": {
                            "type": "object",
                            "properties": {
                                "event_count": { "type": "integer" },
                                "qu_transfer_count": { "type": "integer" },
                                "qu_transfer_amount": { "type": "integer", "format": "int64" },
                                "asset_ownership_change_count": { "type": "integer" },
                                "asset_possession_change_count": { "type": "integer" },
                                "burn_count": { "type": "integer" },
                                "contract_error_count": { "type": "integer" },
                                "custom_message_count": { "type": "integer" }
                            }
                        }
                    }
                },
        // Markets and prices
        "MarketSnapshot": {
                    "type": "object",
                    "required": ["source", "coin_id", "price_usd", "price_e8", "fetched_at"],
                    "properties": {
                        "source": { "type": "string" },
                        "coin_id": { "type": "string" },
                        "price_usd": { "type": "number" },
                        "price_e8": { "type": "integer", "format": "int64" },
                        "market_cap_usd": { "type": ["number", "null"] },
                        "volume_24h_usd": { "type": ["number", "null"] },
                        "change_24h_percent": { "type": ["number", "null"] },
                        "last_updated_at": { "type": ["string", "null"], "format": "date-time" },
                        "fetched_at": { "type": "string", "format": "date-time" },
                        "sources": { "type": "array", "items": { "type": "object" } }
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
                        "epoch": { "type": "integer" }, "initial_tick": { "type": "integer", "description": "Observed first tick of the epoch; falls back to the earliest indexed tick" },
                        "first_tick": { "type": "integer" },
                        "last_tick": { "type": "integer" },
                        "first_timestamp": { "type": "string", "format": "date-time" },
                        "last_timestamp": { "type": "string", "format": "date-time" },
                        "tick_count": { "type": "integer" },
                        "transaction_count": { "type": "integer" },
                        "event_count": { "type": "integer" }, "closed": { "type": "boolean" }
                    }
                },
                "Computor": {
                    "type": "object",
                    "required": ["epoch", "computor_index", "public_key", "identity"],
                    "properties": {
                        "epoch": { "type": "integer" },
                        "computor_index": { "type": "integer" },
                        "public_key": { "type": "string", "description": "0x-prefixed hex public key" },
                        "identity": { "type": "string", "description": "Canonical uppercase base-26 identity" }
                    }
                },
                "QuorumEvidence": {
                    "type": "object",
                    "required": ["epoch", "tick", "quorum_digest", "tick_data_digest", "verification", "timestamp"],
                    "properties": {
                        "epoch": { "type": "integer" },
                        "tick": { "type": "integer" },
                        "quorum_digest": { "type": "string", "description": "Lowercase hex K12 digest agreed by the quorum" },
                        "tick_data_digest": { "type": "string", "description": "Lowercase hex K12 digest of the persisted tick data" },
                        "required_votes": { "type": "integer" },
                        "aligned_votes": { "type": "integer" },
                        "fetched_votes": { "type": "integer" },
                        "unique_votes": { "type": "integer" },
                        "verified_votes": { "type": "integer" },
                        "verification": { "type": "string", "enum": ["quorum", "data", "state"] },
                        "warning_count": { "type": "integer" },
                        "warnings": { "type": "string", "description": "Serialized warning list" },
                        "timestamp": { "type": "string", "format": "date-time" }
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
                "WalletEntity": {
                    "type": "object",
                    "properties": {
                        "spectrum_index": { "type": "integer" },
                        "tick": { "type": "integer" },
                        "public_key": { "type": "string" },
                        "balance": { "type": "integer", "format": "int64" },
                        "incoming_amount": { "type": "integer", "format": "int64" },
                        "outgoing_amount": { "type": "integer", "format": "int64" },
                        "incoming_transfers": { "type": "integer" },
                        "outgoing_transfers": { "type": "integer" },
                        "latest_incoming_tick": { "type": "integer" },
                        "latest_outgoing_tick": { "type": "integer" }
                    }
                },
                "LiveIdentity": {
                    "type": "object",
                    "required": ["identity", "public_key", "spectrum_index", "tick"],
                    "properties": {
                        "identity": { "type": "string" },
                        "public_key": { "type": "string" },
                        "spectrum_index": { "type": "integer" },
                        "tick": { "type": "integer" },
                        "balance": { "type": "integer", "format": "int64" },
                        "incoming_amount": { "type": "integer", "format": "int64" },
                        "outgoing_amount": { "type": "integer", "format": "int64" },
                        "incoming_transfers": { "type": "integer" },
                        "outgoing_transfers": { "type": "integer" },
                        "latest_incoming_tick": { "type": "integer" },
                        "latest_outgoing_tick": { "type": "integer" }
                    }
                },
                "TickTransactionStatus": {
                    "type": "object",
                    "required": ["tick", "current_tick", "transactions"],
                    "properties": {
                        "tick": { "type": "integer" },
                        "current_tick": { "type": "integer" },
                        "transactions": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["hash", "money_flew"],
                                "properties": {
                                    "hash": { "type": "string" },
                                    "money_flew": { "type": "boolean" }
                                }
                            }
                        }
                    }
                },
                "ContractQueryResult": {
                    "type": "object",
                    "required": ["contract_index", "input_type", "output", "bytes"],
                    "properties": {
                        "contract_index": { "type": "integer" },
                        "input_type": { "type": "integer" },
                        "output": { "type": "string", "description": "Hex-encoded output bytes" },
                        "bytes": { "type": "integer" }
                    }
                },
                "IssuedAsset": {
                    "type": "object",
                    "required": ["issuer", "name", "universe_index"],
                    "properties": {
                        "issuer": { "type": "string" },
                        "name": { "type": "string" },
                        "decimals": { "type": "integer" },
                        "unit": { "type": "string" },
                        "universe_index": { "type": "integer" },
                        "tick": { "type": "integer" }
                    }
                },
                "OwnedAsset": {
                    "type": "object",
                    "required": ["owner", "issuer", "name", "shares"],
                    "properties": {
                        "owner": { "type": "string" },
                        "issuer": { "type": "string" },
                        "name": { "type": "string" },
                        "decimals": { "type": "integer" },
                        "unit": { "type": "string" },
                        "shares": { "type": "integer", "format": "int64" },
                        "universe_index": { "type": "integer" },
                        "tick": { "type": "integer" },
                        "managing_contract_index": { "type": "integer" },
                        "ownership_index": { "type": "integer" },
                        "issuance": { "type": "object" }
                    }
                },
                "PossessedAsset": {
                    "type": "object",
                    "required": ["possessor", "issuer", "name", "shares"],
                    "properties": {
                        "possessor": { "type": "string" },
                        "owner": { "type": "string" },
                        "issuer": { "type": "string" },
                        "name": { "type": "string" },
                        "decimals": { "type": "integer" },
                        "unit": { "type": "string" },
                        "shares": { "type": "integer", "format": "int64" },
                        "universe_index": { "type": "integer" },
                        "tick": { "type": "integer" },
                        "managing_contract_index": { "type": "integer" },
                        "possession_index": { "type": "integer" },
                        "ownership": { "type": "object" }
                    }
                },
                "WalletAssets": {
                    "type": "object",
                    "properties": {
                        "identity": { "type": "string" },
                        "issued": { "type": "array", "items": { "$ref": "#/components/schemas/IssuedAsset" } },
                        "owned": { "type": "array", "items": { "$ref": "#/components/schemas/OwnedAsset" } },
                        "possessed": { "type": "array", "items": { "$ref": "#/components/schemas/PossessedAsset" } }
                    }
                },
                "WalletOverview": {
                    "type": "object",
                    "required": ["identity", "assets"],
                    "properties": {
                        "identity": { "type": "string" },
                        "public_key": { "type": "string" },
                        "entity": { "$ref": "#/components/schemas/WalletEntity" },
                        "assets": { "$ref": "#/components/schemas/WalletAssets" },
                        "recent": { "type": "array", "items": { "$ref": "#/components/schemas/Transaction" } }
                    }
                },
                "WalletSnapshot": {
                    "type": "object",
                    "required": ["identity", "assets"],
                    "properties": {
                        "identity": { "type": "string" },
                        "public_key": { "type": "string" },
                        "peer_tick": { "type": "integer" },
                        "epoch": { "type": ["integer", "null"] },
                        "entity": { "$ref": "#/components/schemas/WalletEntity" },
                        "assets": { "$ref": "#/components/schemas/WalletAssets" },
                        "counts": { "type": "object" }
                    }
                },
                "AssetHolder": {
                    "type": "object",
                    "required": ["identity", "shares"],
                    "properties": {
                        "identity": { "type": "string" },
                        "shares": { "type": "integer", "format": "int64" }
                    }
                },
                "PricePoint": {
                    "type": "object",
                    "required": ["hour", "timestamp", "price_e8", "price_usdt"],
                    "properties": {
                        "hour": { "type": "integer", "description": "Hours since 2022-04-13T12:00:00Z" },
                        "timestamp": { "type": "string", "format": "date-time" },
                        "price_e8": { "type": "integer", "format": "int64", "description": "QUBIC/USDT price scaled by 1e8 (USDT per QUBIC)" },
                        "price_usdt": { "type": "number", "description": "USDT per QUBIC rounded to eight decimals" },
                        "onchain": { "type": "boolean", "description": "False for external exchange fallback rows; on-chain-only fields are omitted when false" },
                        "append_tick": { "type": "integer", "description": "On-chain rows only" },
                        "source_tick": { "type": "integer", "description": "On-chain rows only" },
                        "epoch": { "type": "integer", "description": "On-chain rows only" },
                        "flags": { "type": "integer" },
                        "filled": { "type": "boolean", "description": "True when the core filled a missing hour from the previous price; on-chain rows only" },
                        "stale": { "type": "boolean", "description": "True when the source marked the value stale" },
                        "source": { "type": "integer", "description": "0 Price oracle interface, 29 QPayhub fallback, 1000 external exchange consensus" },
                        "source_name": { "type": "string", "description": "price_oracle, qpayhub, external_market, or unknown" },
                        "synced": { "type": "boolean", "description": "On-chain rows only" },
                        "bound": { "type": "boolean", "description": "On-chain rows only" },
                        "series_digest": { "type": "string", "description": "Hex K12 digest of the series state; on-chain rows only" },
                        "observed_at": { "type": "string", "format": "date-time" }
                    }
                },
                "PriceSnapshot": {
                    "type": "object",
                    "required": ["points", "source"],
                    "properties": {
                        "latest": { "oneOf": [{ "$ref": "#/components/schemas/PricePoint" }, { "type": "null" }] },
                        "points": { "type": "integer", "description": "Distinct stored hours" },
                        "hour": { "type": ["integer", "null"] },
                        "timestamp": { "type": ["string", "null"], "format": "date-time" },
                        "price_e8": { "type": ["integer", "null"], "format": "int64" },
                        "price_usdt": { "type": ["number", "null"] },
                        "onchain": { "type": "boolean", "description": "False when the snapshot is the external exchange fallback" },
                        "synced": { "type": "boolean", "description": "On-chain snapshots only" },
                        "bound": { "type": "boolean", "description": "On-chain snapshots only" },
                        "stale": { "type": "boolean", "description": "True when the source marked the value stale" },
                        "source": { "type": "integer", "description": "0 Price oracle interface, 29 QPayhub fallback, 1000 external exchange consensus" },
                        "source_name": { "type": "string" },
                        "series_digest": { "type": "string", "description": "On-chain snapshots only" },
                        "epoch": { "type": "integer", "description": "On-chain snapshots only" },
                        "append_tick": { "type": "integer", "description": "On-chain snapshots only" },
                        "source_tick": { "type": "integer", "description": "On-chain snapshots only" },
                        "observed_at": { "type": "string", "format": "date-time" }
                    }
                },
        // Status and network
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
                },
                "NetworkSystem": {
                    "type": "object",
                    "properties": {
                        "version": { "type": "integer" },
                        "epoch": { "type": "integer" },
                        "tick": { "type": "integer" },
                        "initial_tick": { "type": "integer" },
                        "latest_created_tick": { "type": "integer" },
                        "epoch_age_ticks": { "type": "integer" },
                        "started_at": { "type": ["string", "null"], "format": "date-time", "description": "Earliest trustworthy indexed epoch timestamp, or null when no epoch tick is retained." },
                        "entity_count": { "type": "integer" },
                        "transaction_count": { "type": "integer" },
                        "total_spectrum_amount": { "type": "integer" },
                        "dust_threshold": { "type": "integer" },
                        "solution_threshold": { "type": "integer" },
                        "solution_additional_threshold": { "type": "integer" },
                        "target_tick_vote_signature": { "type": "integer" },
                        "computor_packet_signature": { "type": "integer" },
                        "random_mining_seed": { "type": "string" },
                        "reserves": { "type": "array", "items": { "type": "integer" } },
                        "indexed": { "type": ["object", "null"] }
                    }
                },
                "RevenueComputor": {
                    "type": "object",
                    "properties": {
                        "computor_index": { "type": "integer" },
                        "identity": { "type": ["string", "null"] },
                        "public_key": { "type": ["string", "null"] },
                        "tx_score": { "type": "integer" },
                        "oracle_score": { "type": "integer" },
                        "doge_score": { "type": "integer" },
                        "tx_factor": { "type": "integer" },
                        "oracle_factor": { "type": "integer" },
                        "estimated_reward": { "type": "integer" },
                        "share": { "type": "number" }
                    }
                },
                "Status": {
                    "type": "object",
                    "properties": {
                        "head": { "oneOf": [{ "$ref": "#/components/schemas/Tick" }, { "type": "null" }] },
                        "ingest": { "type": "object", "description": "Persisted ingest cursor: last_indexed_tick, epoch, verification, updated_at." },
                        "ingest_status": { "$ref": "#/components/schemas/IngestStatus" },
                        "network": { "$ref": "#/components/schemas/NetworkStatus" },
                        "backfill": { "$ref": "#/components/schemas/BackfillStatus" },
                        "stats": { "type": "object", "description": "Counts and ingest cursor returned by /v1/network." }
                    }
                },
                "IngestStatus": {
                    "type": "object",
                    "description": "Indexer throughput measured between status requests.",
                    "properties": {
                        "last_indexed_tick": { "type": ["integer", "null"] },
                        "indexed_ticks": { "type": ["integer", "null"], "description": "All ticks currently stored, across epochs." },
                        "ticks_per_second": { "type": ["number", "null"], "description": "Ticks persisted per second since the previous status request. Null until a second sample exists." },
                        "lag_ticks": { "type": ["integer", "null"], "description": "Network head minus last indexed tick." },
                        "eta_seconds": { "type": ["integer", "null"], "description": "Lag divided by ingest rate." }
                    }
                },
                "NetworkStatus": {
                    "type": "object",
                    "description": "Peer head sampled directly from a Qubic peer with a short timeout.",
                    "properties": {
                        "reachable": { "type": "boolean" },
                        "head": { "type": ["integer", "null"] },
                        "epoch": { "type": ["integer", "null"] },
                        "initial_tick": { "type": ["integer", "null"], "description": "First tick of the peer's epoch." },
                        "tick_duration": { "type": ["integer", "null"], "description": "Tick duration reported by the peer, in milliseconds when available." },
                        "ticks_per_second": { "type": ["number", "null"], "description": "Observed peer head advance since the previous status request." },
                        "error": { "type": ["string", "null"] }
                    }
                },
                "BackfillStatus": {
                    "type": "object",
                    "description": "Current-epoch coverage. Coverage compares ticks stored in the epoch with ticks elapsed so far.",
                    "properties": {
                        "available": { "type": "boolean", "description": "False when the peer head is unavailable." },
                        "complete": { "type": "boolean", "description": "True when the first indexed tick is at or before the epoch initial tick." },
                        "initial_tick": { "type": "integer" },
                        "first_indexed_tick": { "type": ["integer", "null"] },
                        "last_indexed_tick": { "type": ["integer", "null"] },
                        "missing_before": { "type": ["integer", "null"], "description": "Ticks between the epoch initial tick and the first indexed tick." },
                        "indexed_ticks": { "type": ["integer", "null"], "description": "Ticks stored inside the current epoch, bounded by the peer head." },
                        "epoch_ticks": { "type": ["integer", "null"], "description": "Ticks elapsed in the current epoch so far." },
                        "remaining_ticks": { "type": ["integer", "null"] },
                        "coverage": { "type": ["number", "null"], "description": "indexed_ticks divided by epoch_ticks, from 0 to 1." },
                        "coverage_percent": { "type": ["number", "null"] }
                    }
                }
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn spec_is_openapi_31_and_covers_routes() {
        let spec = spec();
        assert_eq!(spec["openapi"], "3.1.0");
        assert_eq!(spec["info"]["version"], "0.4.0");
        let paths = spec["paths"].as_object().expect("paths object");
        for path in [
            "/healthz",
            "/v1/status",
            "/status",
            "/v1/network",
            "/v1/network/index",
            "/v1/network/system",
            "/v1/network/peers",
            "/v1/network/revenue",
            "/v1/network/revenue/{identity}",
            "/v1/network/oracles",
            "/v1/network/oracles/stats",
            "/v1/network/oracles/queries",
            "/v1/network/oracles/queries/{id}",
            "/v1/network/oracles/subscriptions",
            "/v1/network/oracles/subscriptions/{id}",
            "/v1/network/oracles/revenue",
            "/v1/epochs",
            "/v1/epochs/{epoch}",
            "/v1/epochs/{epoch}/computors",
            "/v1/ticks",
            "/v1/ticks/{tick}",
            "/v1/ticks/{tick}/evidence",
            "/v1/ticks/{tick}/transactions",
            "/v1/ticks/{tick}/capacity",
            "/v1/ticks/{tick}/events",
            "/v1/transactions",
            "/v1/transactions/{hash}",
            "/v1/transactions/{hash}/effects",
            "/v1/transactions/inclusion-risk",
            "/v1/transactions/{hash}/lifecycle",
            "/v1/events",
            "/v1/identities",
            "/v1/identities/{identity}",
            "/v1/identities/{identity}/transactions",
            "/v1/identities/{identity}/activity",
            "/v1/identities/{identity}/counterparties",
            "/v1/identities/{identity}/flow",
            "/v1/identities/{identity}/events",
            "/v1/assets",
            "/v1/assets/{issuer}/{name}",
            "/v1/assets/{issuer}/{name}/transfers",
            "/v1/assets/{issuer}/{name}/holders",
            "/v1/wallets/{identity}",
            "/v1/wallets/{identity}/assets",
            "/v1/live/wallets/{identity}",
            "/v1/transactions/batch",
            "/v1/analytics/activity",
            "/v1/analytics/top-identities",
            "/v1/analytics/event-types",
            "/v1/stream",
            "/v1/stream/ticks",
            "/v1/stream/transactions",
            "/v1/stream/events",
            "/v1/stream/prices",
            "/v1/stream/market",
            "/v1/ws",
            "/v1/live/identities/{identity}",
            "/v1/live/transactions/{hash}",
            "/v1/live/transactions/{hash}/status",
            "/v1/transactions/{hash}/status",
            "/v1/transactions/validate",
            "/v1/live/ticks/{tick}/transactions",
            "/v1/ticks/{tick}/raw",
            "/v1/live/contracts/{contract_index}/query",
            "/v1/prices",
            "/v1/prices/history",
            "/v1/prices/latest",
            "/v1/prices/snapshot",
            "/v1/market",
            "/v1/search",
            "/v1/webhooks",
            "/v1/webhooks/{id}",
            "/v1/webhooks/{id}/deliveries",
        ] {
            assert!(paths.contains_key(path), "missing {path}");
        }

        for path in [
            "/v1/network/stats",
            "/v1/market/qubic",
            "/v1/prices/market",
            "/v1/identities/{identity}/entity",
            "/v1/transactions/{hash}/network",
            "/v1/transactions/{hash}/network/status",
            "/v1/ticks/{tick}/tx-status",
            "/v1/contracts/{contract_index}/query",
            "/v1/wallets/{identity}/snapshot",
            "/v1/mempool",
            "/v1/mempool/{hash}",
        ] {
            assert!(
                !paths.contains_key(path),
                "retired route still documented: {path}"
            );
        }

        let declared_tags: HashSet<&str> = spec["tags"]
            .as_array()
            .expect("tags array")
            .iter()
            .map(|tag| tag["name"].as_str().expect("tag name"))
            .collect();
        for tag in &declared_tags {
            assert!(
                tag.chars().next().is_some_and(char::is_uppercase),
                "tag names are displayed in docs and should be title case: {tag}"
            );
        }

        let mut operation_ids = HashSet::new();
        let mut used_tags = HashSet::new();
        for (path, item) in paths {
            for (method, operation) in item.as_object().expect("path item") {
                let operation_id = operation["operationId"]
                    .as_str()
                    .unwrap_or_else(|| panic!("{method} {path} needs an operationId"));
                assert!(
                    operation_ids.insert(operation_id),
                    "duplicate operationId {operation_id} at {method} {path}"
                );
                assert!(
                    operation_id
                        .chars()
                        .next()
                        .is_some_and(|c| c.is_ascii_lowercase())
                        && operation_id.chars().all(|c| c.is_ascii_alphanumeric()),
                    "{method} {path} has a non-camelCase operationId: {operation_id}"
                );
                let tags = operation["tags"].as_array().expect("operation tags");
                assert_eq!(tags.len(), 1, "{method} {path} should have one primary tag");
                let tag = tags[0].as_str().expect("tag name");
                assert!(
                    declared_tags.contains(tag),
                    "undeclared tag {tag} at {path}"
                );
                used_tags.insert(tag);
                assert!(
                    operation["summary"]
                        .as_str()
                        .is_some_and(|value| !value.is_empty()),
                    "{method} {path} needs a summary"
                );
                assert!(
                    operation["description"]
                        .as_str()
                        .is_some_and(|value| !value.is_empty()),
                    "{method} {path} needs a description"
                );
                let has_success = operation["responses"]
                    .as_object()
                    .expect("responses object")
                    .keys()
                    .any(|status| status.starts_with('2'));
                assert!(has_success, "{method} {path} needs a 2xx success response");
            }
        }
        assert_eq!(
            used_tags, declared_tags,
            "every declared tag should be used"
        );

        let schemas = spec["components"]["schemas"]
            .as_object()
            .expect("component schemas");
        let mut refs = Vec::new();
        collect_refs(&spec, &mut refs);
        for reference in refs {
            if let Some(name) = reference.strip_prefix("#/components/schemas/") {
                assert!(schemas.contains_key(name), "missing schema {name}");
            } else {
                panic!("unexpected external reference {reference}");
            }
        }
    }

    fn collect_refs(value: &Value, refs: &mut Vec<String>) {
        match value {
            Value::Object(map) => {
                if let Some(reference) = map.get("$ref").and_then(Value::as_str) {
                    refs.push(reference.to_string());
                }
                for child in map.values() {
                    collect_refs(child, refs);
                }
            }
            Value::Array(items) => {
                for item in items {
                    collect_refs(item, refs);
                }
            }
            _ => {}
        }
    }
}
