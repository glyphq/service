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
        "description": "Opaque cursor from meta.next_cursor. Bound to the route and filters.",
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
            "title": "Glyph API",
            "version": "0.1.0",
            "summary": "A direct-peer read API for the Qubic network.",
            "description": "# Glyph API\n\nGlyph indexes Qubic by connecting straight to peers and verifying quorum evidence. There is no hosted RPC in the path: every tick, transaction, and event served here came from peer messages that Glyph fetched, aligned, and persisted itself.\n\n## Reading the API\n\n- Every response wraps its payload in `data` and, where relevant, `meta`.\n- Newest-first collections accept `limit` and an opaque `cursor` from `meta.next_cursor`.\n- Tick, epoch, transaction, event, identity, wallet, asset, and price routes are read-only.\n- `/v1/status` reports live ingest and network progress; `/status` renders the same state as HTML.\n\n## Freshness\n\nNewest-first responses are cached briefly. `/v1/status` is never cached and includes the latest indexed tick, the current network head sampled from peers, indexer throughput, and current-epoch backfill coverage."
        },
        "servers": [{ "url": "/" }],
        "tags": [
            { "name": "status", "description": "Liveness, indexer progress, and the HTML operations dashboard." },
            { "name": "epochs", "description": "Epoch summaries and elected computor sets." },
            { "name": "ticks", "description": "Tick headers and their verified transaction and event contents." },
            { "name": "transactions", "description": "Transactions by hash, source, destination, input type, and tick." },
            { "name": "events", "description": "Contract event logs decoded into typed bodies and participants." },
            { "name": "identities", "description": "Identity history, balances, transfers, and contract interactions." },
            { "name": "wallets", "description": "Wallet-centric balances and issued or held assets." },
            { "name": "assets", "description": "Asset issuances, metadata, and holder sets." },
            { "name": "prices", "description": "Oracle price samples and hourly QUBIC/USDT history." },
            { "name": "analytics", "description": "Aggregated activity, volume, and network-level counts." },
            { "name": "stream", "description": "Server-sent event streams for new ticks and transactions." },
            { "name": "network", "description": "Peer-observed network state and index coverage." },
            { "name": "contracts", "description": "Contract call history and per-contract activity." },
            { "name": "archive", "description": "Raw verified tick bundles retained for audit and replay." },
            { "name": "search", "description": "Resolve hashes, ticks, epochs, assets, and identities in one request." }
        ],
        "x-tagGroups": [
            { "name": "Operations", "tags": ["status", "network"] },
            { "name": "Chain data", "tags": ["epochs", "ticks", "transactions", "events", "contracts", "archive"] },
            { "name": "Accounts and assets", "tags": ["identities", "wallets", "assets"] },
            { "name": "Market and analytics", "tags": ["prices", "analytics"] },
            { "name": "Live and lookup", "tags": ["stream", "search"] }
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
                    "summary": "Live indexer and network progress",
                    "description": "Returns the persisted index head, ingest cursor, sampled peer head, indexer throughput, lag and ETA, and current-epoch backfill coverage. Throughput is measured between status requests at least one second apart, so the first request after startup may omit rates.",
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
                    "tags": ["status"],
                    "summary": "Server-rendered operations dashboard",
                    "description": "Uptime, head, ingest cursor, counts, and runtime feature flags. Refreshes every 5 seconds and does not require JavaScript.",
                    "responses": { "200": { "description": "HTML dashboard", "content": { "text/html": { "schema": { "type": "string" } } } } }
                }
            },
            "/v1/network/stats": {
                "get": {
                    "tags": ["network"],
                    "summary": "Index-wide counts and coverage",
                    "description": "Entity counts and the indexed tick range. Use /v1/status for live peer throughput and backfill progress.",
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
            "/v1/epochs/{epoch}/computors": {
                "get": {
                    "tags": ["epochs"],
                    "summary": "Elected computor set for an epoch",
                    "description": "Ordered by computor_index. public_key is 0x-prefixed hex and identity is the canonical uppercase base-26 form.",
                    "parameters": [path_param("epoch", "Epoch number")],
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
            "/v1/ticks/{tick}/evidence": {
                "get": {
                    "tags": ["ticks"],
                    "summary": "Quorum evidence behind a stored tick",
                    "description": "Digests are lowercase hex. Vote counts describe how the winning quorum was assembled.",
                    "parameters": [path_param("tick", "Global tick number")],
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
                    "summary": "Broadcast a signed transaction to direct peers",
                    "description": "Requires ALLOW_BROADCAST=true. The transaction is decoded and locally checked for amount range, input size, byte length, and SchnorrQ signature before any socket is opened. Valid transactions are fanned out concurrently to up to `peers` peers (capped by BROADCAST_PEERS). Set `dry_run` to run the same checks without broadcasting.\n\nQubic does not acknowledge broadcasts. A `sent` peer means the packet reached the peer socket, not that the transaction will be included. Poll `meta.links.transaction`, or stream ticks, to observe inclusion.",
                    "requestBody": {
                        "required": true,
                        "content": { "application/json": { "schema": { "$ref": "#/components/schemas/BroadcastRequest" } } }
                    },
                    "responses": {
                        "200": { "description": "Dry run report; no peer connection was opened", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/BroadcastResult" } } } },
                        "202": { "description": "Transaction written to at least one peer socket", "content": { "application/json": { "schema": { "$ref": "#/components/schemas/BroadcastResult" } } } },
                        "400": { "description": "Transaction failed local validation" },
                        "403": { "description": "Broadcast disabled" },
                        "503": { "description": "No peer accepted the transaction" }
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
            "/v1/identities/{identity}/activity": {
                "get": {
                    "tags": ["identities", "transactions", "events"],
                    "summary": "Linear transaction and contract-effect history",
                    "description": "Merges direct transactions with decoded events where the identity participates, including contract payouts and asset movements.",
                    "parameters": [path_param("identity", "Uppercase base-26 identity"), limit_param(), cursor_param(), offset_param()],
                    "responses": {
                        "200": { "description": "Paginated activity rows with kind transaction or event" },
                        "400": { "description": "Invalid identity or cursor" }
                    }
                }
            },
            "/v1/identities/{identity}/events": {
                "get": {
                    "tags": ["identities", "events"],
                    "summary": "Events where the identity participates",
                    "parameters": [path_param("identity", "Uppercase base-26 identity"), limit_param(), cursor_param(), offset_param()],
                    "responses": {
                        "200": { "description": "Paginated event rows" },
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
            "/v1/wallets/{identity}": {
                "get": {
                    "tags": ["wallets", "identities", "network"],
                    "summary": "Wallet overview from the index and a direct peer",
                    "description": "Combines the indexed identity and its ten newest transactions with the peer spectrum entity and the issued, owned, and possessed asset lists. One peer connection serves the request.",
                    "parameters": [path_param("identity", "60-character identity")],
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
                    "tags": ["wallets", "assets", "network"],
                    "summary": "Issued, owned, and possessed assets for a wallet",
                    "parameters": [
                        path_param("identity", "60-character identity"),
                        { "name": "kind", "in": "query", "required": false, "schema": { "type": "string", "enum": ["issued", "owned", "possessed", "all"], "default": "all" } }
                    ],
                    "responses": {
                        "200": { "description": "Typed asset lists", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "$ref": "#/components/schemas/WalletAssets" }, "meta": { "type": "object" } } } } } },
                        "400": { "description": "Invalid identity or asset kind" },
                        "503": { "description": "No peer available or peer lookup failed" }
                    }
                }
            },
            "/v1/wallets/{identity}/snapshot": {
                "get": {
                    "tags": ["wallets", "network"],
                    "summary": "Point-in-time peer snapshot for a wallet",
                    "description": "Entity, asset lists, counts, and peer head from a single peer connection. Never cached.",
                    "parameters": [path_param("identity", "60-character identity")],
                    "responses": {
                        "200": { "description": "Wallet snapshot", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "$ref": "#/components/schemas/WalletSnapshot" }, "meta": { "type": "object" } } } } } },
                        "400": { "description": "Invalid identity" },
                        "503": { "description": "No peer available or peer lookup failed" }
                    }
                }
            },
            "/v1/assets/{issuer}/{name}/holders": {
                "get": {
                    "tags": ["assets", "network"],
                    "summary": "Ownership holders for an asset",
                    "description": "Queries the peer asset tree for ownership records of one issuer and asset name. Holders are deduplicated by identity, sorted by shares descending then identity, and truncated to limit.",
                    "parameters": [
                        path_param("issuer", "Issuer identity"),
                        path_param("name", "Asset name, 1-7 uppercase ASCII letters or digits"),
                        { "name": "limit", "in": "query", "required": false, "schema": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 100 } }
                    ],
                    "responses": {
                        "200": { "description": "Holder snapshot; an empty list is valid", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "type": "array", "items": { "$ref": "#/components/schemas/AssetHolder" } }, "meta": { "type": "object" } } } } } },
                        "400": { "description": "Invalid issuer, asset name, or limit" },
                        "503": { "description": "No peer available or asset lookup failed" }
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
            "/v1/transactions/{hash}/network/status": {
                "get": {
                    "tags": ["transactions", "network"],
                    "summary": "Peer confirmation and money-flow status for a transaction",
                    "description": "Looks up the confirmed transaction by hash, then asks the same peer for the tick tx-status add-on. When the peer does not run the add-on, the response is still 200 with money_flew null and meta.status_source unavailable.",
                    "parameters": [path_param("hash", "60-character transaction hash")],
                    "responses": {
                        "200": { "description": "Peer transaction status", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "$ref": "#/components/schemas/TransactionNetworkStatus" } } } } } },
                        "400": { "description": "Invalid hash" },
                        "404": { "description": "Peer has no confirmed transaction" },
                        "503": { "description": "No peer available or transaction lookup failed" }
                    }
                }
            },
            "/v1/transactions/validate": {
                "post": {
                    "tags": ["transactions"],
                    "summary": "Validate a signed transaction locally without broadcasting",
                    "description": "Decodes the signed transaction and reports amount-range, input-size, and SchnorrQ signature checks. No peer connection is opened and ALLOW_BROADCAST is not required.",
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
                    "tags": ["transactions", "network"],
                    "summary": "Combined indexed and peer transaction status",
                    "description": "Returns the indexed row when present and best-effort peer confirmation data. An indexed transaction still returns 200 when peers are unavailable; meta.peer_error explains the degradation.",
                    "parameters": [path_param("hash", "60-character transaction hash")],
                    "responses": {
                        "200": { "description": "Indexed and/or peer transaction status" },
                        "400": { "description": "Invalid hash" },
                        "404": { "description": "Unknown to both index and peer" },
                        "503": { "description": "No peer and no indexed row" }
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
            },
            "/v1/prices": {
                "get": {
                    "tags": ["prices"],
                    "summary": "Hourly QUBIC/USDT points, ascending",
                    "description": "Points recorded from the latest verified QUBIC/USDT Price oracle reply. The QPayhub GetQuUsdPrice read function is used only as a secondary source.",
                    "parameters": [
                        { "name": "from_hour", "in": "query", "schema": { "type": "integer", "minimum": 0 }, "description": "First core price hour, inclusive" },
                        { "name": "limit", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 168 } }
                    ],
                    "responses": {
                        "200": { "description": "Price points", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "type": "array", "items": { "$ref": "#/components/schemas/PricePoint" } }, "meta": { "type": "object" } } } } } },
                        "500": { "description": "Storage failure" }
                    }
                }
            },
            "/v1/prices/history": {
                "get": {
                    "tags": ["prices"],
                    "summary": "Trailing QUBIC/USDT price window",
                    "parameters": [{ "name": "hours", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 24 } }],
                    "responses": { "200": { "description": "Ascending price window" } }
                }
            },
            "/v1/prices/latest": {
                "get": {
                    "tags": ["prices"],
                    "summary": "Latest stored QUBIC/USDT price point",
                    "responses": {
                        "200": { "description": "Latest price", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "$ref": "#/components/schemas/PricePoint" } } } } } },
                        "404": { "description": "No price data yet" }
                    }
                }
            },
            "/v1/prices/snapshot": {
                "get": {
                    "tags": ["prices"],
                    "summary": "QUBIC/USDT price metadata and latest point",
                    "responses": {
                        "200": { "description": "Price snapshot; data is null before the first poll", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "oneOf": [{ "$ref": "#/components/schemas/PriceSnapshot" }, { "type": "null" }] }, "meta": { "type": "object" } } } } } },
                        "500": { "description": "Storage failure" }
                    }
                }
            },
            "/v1/wallets/{identity}": {
                "get": {
                    "tags": ["wallets", "identities", "network"],
                    "summary": "Wallet overview from a peer snapshot plus indexed history",
                    "parameters": [path_param("identity", "Uppercase base-26 identity")],
                    "responses": {
                        "200": { "description": "Entity, assets, and recent transactions" },
                        "400": { "description": "Invalid identity" },
                        "404": { "description": "Identity absent from both index and peer spectrum" },
                        "503": { "description": "No peer available" }
                    }
                }
            },
            "/v1/wallets/{identity}/assets": {
                "get": {
                    "tags": ["wallets", "assets", "network"],
                    "summary": "Issued, owned, and possessed asset snapshot",
                    "parameters": [
                        path_param("identity", "Uppercase base-26 identity"),
                        { "name": "kind", "in": "query", "schema": { "type": "string", "enum": ["issued", "owned", "possessed", "all"], "default": "all" } }
                    ],
                    "responses": { "200": { "description": "Asset lists" }, "400": { "description": "Invalid identity or kind" }, "503": { "description": "No peer available" } }
                }
            },
            "/v1/wallets/{identity}/snapshot": {
                "get": {
                    "tags": ["wallets", "network"],
                    "summary": "Point-in-time wallet and asset snapshot",
                    "parameters": [path_param("identity", "Uppercase base-26 identity")],
                    "responses": { "200": { "description": "Entity and asset snapshot" }, "400": { "description": "Invalid identity" }, "503": { "description": "No peer available" } }
                }
            },
            "/v1/assets/{issuer}/{name}/holders": {
                "get": {
                    "tags": ["assets", "wallets", "network"],
                    "summary": "Ownership holder snapshot for one asset",
                    "parameters": [
                        path_param("issuer", "Issuer identity"),
                        path_param("name", "Asset name"),
                        { "name": "limit", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 100 } }
                    ],
                    "responses": { "200": { "description": "Ranked holders" }, "400": { "description": "Invalid issuer or name" }, "503": { "description": "No peer available" } }
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
                        "raw": { "type": "string", "description": "Signed transaction bytes as hex, with or without a 0x prefix. `transaction`, `raw_transaction`, and `signed_transaction` are accepted aliases." },
                        "dry_run": { "type": "boolean", "default": false, "description": "Validate and describe the transaction without opening a peer connection." },
                        "peers": { "type": "integer", "minimum": 1, "maximum": 8, "description": "Peers to fan out to. Capped by the server's BROADCAST_PEERS setting. `max_peers` is an alias." }
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
                                "target_peers": { "type": "integer", "description": "Dry run only: peers that would be contacted." },
                                "sent": { "type": "integer", "description": "Broadcast only: peers whose socket accepted the packet." },
                                "targeted": { "type": "integer", "description": "Broadcast only: peers attempted." },
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
                                    "description": "Broadcast only: one entry per attempted peer.",
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
                        "raw": { "type": "string", "description": "Signed transaction bytes as hex, with or without a 0x prefix" }
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
                        "append_tick": { "type": "integer" },
                        "source_tick": { "type": "integer" },
                        "epoch": { "type": "integer" },
                        "flags": { "type": "integer" },
                        "filled": { "type": "boolean", "description": "True when the core filled a missing hour from the previous price" },
                        "stale": { "type": "boolean", "description": "True when the on-chain source marked the value stale" },
                        "source": { "type": "integer", "description": "0 Price oracle interface, 29 QPayhub fallback" },
                        "source_name": { "type": "string", "description": "price_oracle, qpayhub, or unknown" },
                        "synced": { "type": "boolean" },
                        "bound": { "type": "boolean" },
                        "series_digest": { "type": "string", "description": "Hex K12 digest of the series state" },
                        "observed_at": { "type": "string", "format": "date-time" }
                    }
                },
                "PriceSnapshot": {
                    "type": "object",
                    "required": ["points", "synced", "bound", "source"],
                    "properties": {
                        "latest": { "oneOf": [{ "$ref": "#/components/schemas/PricePoint" }, { "type": "null" }] },
                        "points": { "type": "integer", "description": "Distinct stored hours" },
                        "hour": { "type": ["integer", "null"] },
                        "timestamp": { "type": ["string", "null"], "format": "date-time" },
                        "price_e8": { "type": ["integer", "null"], "format": "int64" },
                        "price_usdt": { "type": ["number", "null"] },
                        "synced": { "type": "boolean" },
                        "bound": { "type": "boolean" },
                        "stale": { "type": "boolean", "description": "True when the on-chain source marked the value stale" },
                        "source": { "type": "integer", "description": "0 Price oracle interface, 29 QPayhub fallback" },
                        "source_name": { "type": "string" },
                        "series_digest": { "type": "string" },
                        "epoch": { "type": "integer" },
                        "append_tick": { "type": "integer" },
                        "source_tick": { "type": "integer" },
                        "observed_at": { "type": "string", "format": "date-time" }
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
                },
                "Status": {
                    "type": "object",
                    "properties": {
                        "head": { "oneOf": [{ "$ref": "#/components/schemas/Tick" }, { "type": "null" }] },
                        "ingest": { "type": "object", "description": "Persisted ingest cursor: last_indexed_tick, epoch, verification, updated_at." },
                        "ingest_status": { "$ref": "#/components/schemas/IngestStatus" },
                        "network": { "$ref": "#/components/schemas/NetworkStatus" },
                        "backfill": { "$ref": "#/components/schemas/BackfillStatus" },
                        "stats": { "type": "object", "description": "Counts and ingest cursor returned by /v1/network/stats." }
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
            "/status",
            "/v1/network/stats",
            "/v1/epochs",
            "/v1/epochs/{epoch}",
            "/v1/epochs/{epoch}/computors",
            "/v1/ticks",
            "/v1/ticks/{tick}",
            "/v1/ticks/{tick}/evidence",
            "/v1/ticks/{tick}/transactions",
            "/v1/ticks/{tick}/events",
            "/v1/transactions",
            "/v1/transactions/{hash}",
            "/v1/events",
            "/v1/identities",
            "/v1/identities/{identity}",
            "/v1/identities/{identity}/transactions",
            "/v1/identities/{identity}/activity",
            "/v1/identities/{identity}/events",
            "/v1/assets",
            "/v1/assets/{issuer}/{name}",
            "/v1/assets/{issuer}/{name}/holders",
            "/v1/wallets/{identity}",
            "/v1/wallets/{identity}/assets",
            "/v1/wallets/{identity}/snapshot",
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
            "/v1/transactions/{hash}/network/status",
            "/v1/transactions/{hash}/status",
            "/v1/transactions/validate",
            "/v1/ticks/{tick}/tx-status",
            "/v1/ticks/{tick}/raw",
            "/v1/contracts/{contract_index}/query",
            "/v1/prices",
            "/v1/prices/history",
            "/v1/prices/latest",
            "/v1/prices/snapshot",
            "/v1/wallets/{identity}",
            "/v1/wallets/{identity}/assets",
            "/v1/wallets/{identity}/snapshot",
            "/v1/assets/{issuer}/{name}/holders",
            "/v1/search",
        ] {
            assert!(paths.contains_key(path), "missing {path}");
        }
    }
}
