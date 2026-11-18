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
            "x-logo": { "url": "/logo.png", "altText": "Glyph" },
            "description": "# Glyph API\n\nGlyph indexes Qubic by connecting straight to peers and verifying quorum evidence. There is no hosted RPC in the path: every tick, transaction, and event served here came from peer messages that Glyph fetched, aligned, and persisted itself.\n\n## Reading the API\n\n- Every response wraps its payload in `data` and, where relevant, `meta`.\n- Newest-first collections accept `limit` and an opaque `cursor` from `meta.next_cursor`.\n- Tick, epoch, transaction, event, identity, wallet, asset, and price routes are read-only.\n- `/v1/status` reports live ingest and network progress; `/status` renders the same state as HTML.\n\n## Freshness\n\nNewest-first responses are cached briefly. `/v1/status` is never cached and includes the latest indexed tick, the current network head sampled from peers, indexer throughput, and current-epoch backfill coverage."
        },
        "servers": [{ "url": "/" }],
        "tags": [
            { "name": "Status", "description": "Liveness, indexer progress, and the HTML operations dashboard." },
            { "name": "Epochs", "description": "Epoch summaries and elected computor sets." },
            { "name": "Ticks", "description": "Tick headers and their verified transaction and event contents." },
            { "name": "Transactions", "description": "Transactions by hash, source, destination, input type, and tick." },
            { "name": "Events", "description": "Contract event logs decoded into typed bodies and participants." },
            { "name": "Identities", "description": "Identity history, balances, transfers, and contract interactions." },
            { "name": "Wallets", "description": "Wallet-centric balances and issued or held assets." },
            { "name": "Assets", "description": "Asset issuances, metadata, and holder sets." },
            { "name": "Prices", "description": "Oracle price samples and hourly QUBIC/USDT history." },
            { "name": "Market", "description": "External exchange consensus and market reference data." },
            { "name": "Analytics", "description": "Aggregated activity, volume, and network-level counts." },
            { "name": "Stream", "description": "Server-sent event streams for new ticks and transactions." },
            { "name": "Network", "description": "Peer-observed network state and index coverage." },
            { "name": "Contracts", "description": "Contract call history and per-contract activity." },
            { "name": "Archive", "description": "Raw verified tick bundles retained for audit and replay." },
            { "name": "Search", "description": "Resolve hashes, ticks, epochs, assets, and identities in one request." }
        ],
        "x-tagGroups": [
            { "name": "Operations", "tags": ["Status", "Network"] },
            { "name": "Chain Data", "tags": ["Epochs", "Ticks", "Transactions", "Events", "Contracts", "Archive"] },
            { "name": "Accounts & Assets", "tags": ["Identities", "Wallets", "Assets"] },
            { "name": "Markets & Analytics", "tags": ["Prices", "Market", "Analytics"] },
            { "name": "Live & Lookup", "tags": ["Stream", "Search"] }
        ],
        "paths": {
            "/healthz": {
                "get": {
                    "operationId": "getHealth",
                    "tags": ["Status"],
                    "summary": "Liveness probe",
                    "description": "Returns 200 as soon as the process is accepting requests. It does not touch storage or peers, so it is safe for load balancers and container health checks.",
                    "responses": { "200": { "description": "Process is alive" } }
                }
            },
            "/v1/status": {
                "get": {
                    "operationId": "getStatus",
                    "tags": ["Status"],
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
                    "operationId": "getStatusPage",
                    "tags": ["Status"],
                    "summary": "Server-rendered operations dashboard",
                    "description": "Uptime, head, ingest cursor, counts, and runtime feature flags. Refreshes every 5 seconds and does not require JavaScript.",
                    "responses": { "200": { "description": "HTML dashboard", "content": { "text/html": { "schema": { "type": "string" } } } } }
                }
            },
            "/v1/network/stats": {
                "get": {
                    "operationId": "getNetworkStats",
                    "tags": ["Network"],
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
                    "operationId": "listEpochs",
                    "tags": ["Epochs"],
                    "summary": "Epoch summaries, newest first",
                    "description": "One row per indexed epoch with its tick range, transaction and event counts, and observed timestamps. Walk backwards through history with cursor pagination.",
                    "parameters": [limit_param(), cursor_param(), offset_param()],
                    "responses": { "200": { "description": "Paginated epochs" } }
                }
            },
            "/v1/epochs/{epoch}": {
                "get": {
                    "operationId": "getEpoch",
                    "tags": ["Epochs"],
                    "summary": "One epoch summary",
                    "description": "Returns the same summary row as the collection for a single epoch.",
                    "parameters": [path_param("epoch", "Epoch number")],
                    "responses": { "200": { "description": "Epoch" }, "404": { "description": "Epoch not indexed" } }
                }
            },
            "/v1/epochs/{epoch}/computors": {
                "get": {
                    "operationId": "listEpochComputors",
                    "tags": ["Epochs"],
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
                    "operationId": "listTicks",
                    "tags": ["Ticks"],
                    "summary": "Ticks, newest first",
                    "description": "Tick headers with timestamp, computor index, transaction and event counts, and verification tier. Filter by epoch and page with cursors.",
                    "parameters": [
                        { "name": "epoch", "in": "query", "schema": { "type": "integer", "minimum": 0 } },
                        limit_param(), cursor_param(), offset_param()
                    ],
                    "responses": { "200": { "description": "Paginated ticks" } }
                }
            },
            "/v1/ticks/{tick}": {
                "get": {
                    "operationId": "getTick",
                    "tags": ["Ticks"],
                    "summary": "Tick by number",
                    "description": "A single tick header. Tick numbers are global across epochs and increase monotonically.",
                    "parameters": [path_param("tick", "Global tick number")],
                    "responses": {
                        "200": { "description": "Tick" },
                        "404": { "description": "Tick not indexed" }
                    }
                }
            },
            "/v1/ticks/{tick}/evidence": {
                "get": {
                    "operationId": "getTickEvidence",
                    "tags": ["Ticks"],
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
                    "operationId": "listTickTransactions",
                    "tags": ["Ticks", "Transactions"],
                    "summary": "Transactions in a tick, slot order",
                    "description": "Transactions in execution-slot order with hash, source, destination, amount, input type, input data, and status.",
                    "parameters": [path_param("tick", "Global tick number"), limit_param(), cursor_param(), offset_param()],
                    "responses": {
                        "200": { "description": "Paginated transactions" },
                        "404": { "description": "Tick not indexed" }
                    }
                }
            },
            "/v1/ticks/{tick}/events": {
                "get": {
                    "operationId": "listTickEvents",
                    "tags": ["Ticks", "Events"],
                    "summary": "Events in a tick, log id order",
                    "description": "Decoded contract events in log-id order with type name, typed body, participant identities, and the transaction that produced them.",
                    "parameters": [path_param("tick", "Global tick number"), limit_param(), cursor_param(), offset_param()],
                    "responses": {
                        "200": { "description": "Paginated events" },
                        "404": { "description": "Tick not indexed" }
                    }
                }
            },
            "/v1/transactions": {
                "get": {
                    "operationId": "listTransactions",
                    "tags": ["Transactions"],
                    "summary": "Transactions, newest tick first",
                    "description": "Transactions filtered by epoch, tick, or identity. Newest tick and slot first. Use the dedicated identity or tick routes for larger histories.",
                    "parameters": [
                        { "name": "epoch", "in": "query", "schema": { "type": "integer", "minimum": 0 } },
                        { "name": "tick", "in": "query", "schema": { "type": "integer", "minimum": 0 } },
                        { "name": "identity", "in": "query", "schema": { "type": "string" } },
                        limit_param(), cursor_param(), offset_param()
                    ],
                    "responses": { "200": { "description": "Paginated transactions" }, "400": { "description": "Invalid identity or cursor" } }
                },
                "post": {
                    "operationId": "broadcastTransaction",
                    "tags": ["Transactions"],
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
                    "operationId": "getTransaction",
                    "tags": ["Transactions"],
                    "summary": "Transaction by lowercase base-26 hash",
                    "description": "Look up one transaction by its 60-character lowercase base-26 hash.",
                    "parameters": [path_param("hash", "60-character transaction hash")],
                    "responses": {
                        "200": { "description": "Transaction" },
                        "400": { "description": "Invalid hash" },
                        "404": { "description": "Transaction not indexed" }
                    }
                }
            },
            "/v1/transactions/{hash}/effects": {
                "get": {
                    "operationId": "getTransactionEffects",
                    "tags": ["Transactions", "Events"],
                    "summary": "Transaction, tick header, events, and normalized effects",
                    "description": "One request for an explorer transaction page. Returns the indexed transaction, its tick header, every decoded event, a typed effect list for transfers, asset movements, burns, contract messages, and custom messages, plus an aggregate summary. This route is local-only and never opens a peer connection.",
                    "parameters": [path_param("hash", "60-character transaction hash")],
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
                    "summary": "Typed events, newest tick first",
                    "description": "Decoded logs with type name, body, participants, and transaction linkage. Filter by epoch, tick, or event type.",
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
                    "operationId": "listIdentities",
                    "tags": ["Identities"],
                    "summary": "Identities by most recent activity",
                    "description": "Ranked by latest activity. Each row carries observed QU balance, incoming and outgoing totals, transfer count, and first and last seen ticks.",
                    "parameters": [limit_param(), cursor_param(), offset_param()],
                    "responses": { "200": { "description": "Paginated identities" } }
                }
            },
            "/v1/identities/{identity}": {
                "get": {
                    "operationId": "getIdentity",
                    "tags": ["Identities"],
                    "summary": "Identity summary and observed QU flow",
                    "description": "Indexed QU flow, transfer count, and first and last activity for one identity. This is observed history, not a live spectrum lookup.",
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
                    "operationId": "listIdentityTransactions",
                    "tags": ["Identities", "Transactions"],
                    "summary": "Transactions sent or received by an identity",
                    "description": "Both outgoing transactions and transfers where the identity is the destination, newest first.",
                    "parameters": [path_param("identity", "Uppercase base-26 identity"), limit_param(), cursor_param(), offset_param()],
                    "responses": {
                        "200": { "description": "Paginated transactions" },
                        "400": { "description": "Invalid identity or cursor" }
                    }
                }
            },
            "/v1/identities/{identity}/activity": {
                "get": {
                    "operationId": "getIdentityActivity",
                    "tags": ["Identities", "Transactions", "Events"],
                    "summary": "Linear transaction and contract-effect history",
                    "description": "Merges direct transactions with decoded events where the identity participates, including contract payouts and asset movements.",
                    "parameters": [path_param("identity", "Uppercase base-26 identity"), limit_param(), cursor_param(), offset_param()],
                    "responses": {
                        "200": { "description": "Paginated activity rows with kind transaction or event" },
                        "400": { "description": "Invalid identity or cursor" }
                    }
                }
            },
            "/v1/identities/{identity}/counterparties": {
                "get": {
                    "operationId": "listIdentityCounterparties",
                    "tags": ["Identities", "Analytics"],
                    "summary": "Top counterparties by transfer volume",
                    "description": "Aggregates indexed transfers where the identity is the source or destination. Each row reports incoming and outgoing amounts, net flow, transaction count, and first and last tick. Self-transfers are excluded. This route is local-only.",
                    "parameters": [
                        path_param("identity", "Uppercase base-26 identity"),
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
                    "tags": ["Identities", "Analytics"],
                    "summary": "Bucketed incoming and outgoing QU flow",
                    "description": "Returns a fixed window of buckets ending at the indexed head. Empty buckets are zero-filled so charts do not need to resample. Amounts are in the network's base unit and are not rescaled.",
                    "parameters": [
                        path_param("identity", "Uppercase base-26 identity"),
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
                    "tags": ["Identities", "Events"],
                    "summary": "Events where the identity participates",
                    "description": "Contract events where the identity appears as a sender, receiver, or other participant, newest first.",
                    "parameters": [path_param("identity", "Uppercase base-26 identity"), limit_param(), cursor_param(), offset_param()],
                    "responses": {
                        "200": { "description": "Paginated event rows" },
                        "400": { "description": "Invalid identity or cursor" }
                    }
                }
            },
            "/v1/assets": {
                "get": {
                    "operationId": "listAssets",
                    "tags": ["Assets"],
                    "summary": "Assets derived from issuance events, by issuer and name",
                    "description": "Assets discovered from issuance events. Filter by issuer or owner and page newest first.",
                    "parameters": [
                        { "name": "issuer", "in": "query", "schema": { "type": "string" } },
                        limit_param(), cursor_param(), offset_param()
                    ],
                    "responses": { "200": { "description": "Paginated assets" }, "400": { "description": "Invalid issuer or cursor" } }
                }
            },
            "/v1/assets/{issuer}/{name}": {
                "get": {
                    "operationId": "getAsset",
                    "tags": ["Assets"],
                    "summary": "One asset by issuer and name",
                    "description": "Asset metadata and issuance details for one issuer and asset name.",
                    "parameters": [path_param("issuer", "Issuer identity"), path_param("name", "Asset name")],
                    "responses": {
                        "200": { "description": "Asset" },
                        "400": { "description": "Invalid issuer" },
                        "404": { "description": "Asset not indexed" }
                    }
                }
            },
            "/v1/assets/{issuer}/{name}/transfers": {
                "get": {
                    "operationId": "listAssetTransfers",
                    "tags": ["Assets", "Events"],
                    "summary": "Ownership and possession transfers for one asset",
                    "description": "Local indexed history of decoded asset_ownership_change and asset_possession_change events, newest first. Works without a peer and does not rebuild the holder set.",
                    "parameters": [
                        path_param("issuer", "Issuer identity"),
                        path_param("name", "Asset name, 1-7 ASCII letters or digits"),
                        { "name": "kind", "in": "query", "schema": { "type": "string", "enum": ["all", "ownership", "possession"], "default": "all" } },
                        limit_param(), cursor_param(), offset_param()
                    ],
                    "responses": {
                        "200": { "description": "Paginated transfer events", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "type": "array", "items": { "$ref": "#/components/schemas/AssetTransfer" } } } } } } },
                        "400": { "description": "Invalid issuer, asset name, kind, or cursor" }
                    }
                }
            },
            "/v1/wallets/{identity}": {
                "get": {
                    "operationId": "getWallet",
                    "tags": ["Wallets", "Identities", "Network"],
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
                    "operationId": "getWalletAssets",
                    "tags": ["Wallets", "Assets", "Network"],
                    "summary": "Issued, owned, and possessed assets for a wallet",
                    "description": "Asset lists from one peer connection, filtered by kind. Use kind=all for a single round trip.",
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
                    "operationId": "getWalletSnapshot",
                    "tags": ["Wallets", "Network"],
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
                    "operationId": "listAssetHolders",
                    "tags": ["Assets", "Network"],
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
                    "operationId": "search",
                    "tags": ["Search"],
                    "summary": "Resolve a tick, transaction hash, or identity",
                    "description": "One query parameter, typed results. Ticks resolve as integers, transaction hashes by base-26 digest, and identities in either canonical or hex form.",
                    "parameters": [{ "name": "q", "in": "query", "required": true, "schema": { "type": "string" } }],
                    "responses": { "200": { "description": "Typed matches" }, "400": { "description": "Missing query" } }
                }
            },
            "/v1/transactions/batch": {
                "post": {
                    "operationId": "batchTransactions",
                    "tags": ["Transactions"],
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
                    "operationId": "getActivityAnalytics",
                    "tags": ["Analytics"],
                    "summary": "Transaction and event counts per time bucket",
                    "description": "Bucketed activity with deduplicated counts across replayed ticks and summed QU volume. Choose the interval and window with query parameters.",
                    "parameters": [
                        { "name": "interval", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 86400, "default": 3600 }, "description": "Bucket width in seconds" },
                        { "name": "limit", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 500, "default": 24 } }
                    ],
                    "responses": { "200": { "description": "Activity buckets, newest first" } }
                }
            },
            "/v1/analytics/top-identities": {
                "get": {
                    "operationId": "listTopIdentities",
                    "tags": ["Analytics"],
                    "summary": "Identities with the most observed activity",
                    "description": "Ranked by transfer count, with balance and first and last seen ticks for each identity.",
                    "parameters": [{ "name": "limit", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 200, "default": 25 } }],
                    "responses": { "200": { "description": "Ranked identities" } }
                }
            },
            "/v1/analytics/event-types": {
                "get": {
                    "operationId": "getEventTypeAnalytics",
                    "tags": ["Analytics"],
                    "summary": "Event counts grouped by decoded type",
                    "description": "Total occurrences for each decoded event type, highest first.",
                    "parameters": [{ "name": "limit", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 200, "default": 50 } }],
                    "responses": { "200": { "description": "Event type counts" } }
                }
            },
            "/v1/stream": {
                "get": {
                    "operationId": "streamAll",
                    "tags": ["Stream"],
                    "summary": "Server-sent events for ticks, transactions, and events",
                    "description": "Polls the local index head only; never opens a peer connection. Sends keep-alive comments every 15 seconds and lag events when a subscriber falls behind.",
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
                    "summary": "Tick events only",
                    "description": "Server-sent tick headers as new ticks are indexed.",
                    "responses": { "200": { "description": "text/event-stream", "content": { "text/event-stream": { "schema": { "type": "string" } } } }, "503": { "description": "Streaming is not enabled" } }
                }
            },
            "/v1/stream/transactions": {
                "get": {
                    "operationId": "streamTransactions",
                    "tags": ["Stream"],
                    "summary": "Transaction events only",
                    "description": "Server-sent transaction rows as new ticks are indexed.",
                    "responses": { "200": { "description": "text/event-stream", "content": { "text/event-stream": { "schema": { "type": "string" } } } }, "503": { "description": "Streaming is not enabled" } }
                }
            },
            "/v1/stream/events": {
                "get": {
                    "operationId": "streamEvents",
                    "tags": ["Stream"],
                    "summary": "Log event events only",
                    "description": "Server-sent decoded contract events as new ticks are indexed.",
                    "responses": { "200": { "description": "text/event-stream", "content": { "text/event-stream": { "schema": { "type": "string" } } } }, "503": { "description": "Streaming is not enabled" } }
                }
            },
            "/v1/ws": {
                "get": {
                    "operationId": "websocketStream",
                    "tags": ["Stream"],
                    "summary": "WebSocket feed with the same event stream",
                    "description": "Upgrade to a WebSocket and receive the same JSON events as the SSE stream on one connection.",
                    "responses": { "200": { "description": "WebSocket upgrade" }, "503": { "description": "Streaming is not enabled" } }
                }
            },
            "/v1/identities/{identity}/entity": {
                "get": {
                    "operationId": "getIdentityEntity",
                    "tags": ["Identities", "Network"],
                    "summary": "Entity record from a direct peer",
                    "description": "Reads the peer spectrum entity for an identity. This is live peer state and is never cached.",
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
                    "operationId": "getNetworkTransaction",
                    "tags": ["Transactions", "Network"],
                    "summary": "Transaction as reported by a direct peer",
                    "description": "Reads a transaction from a peer by hash. Useful for transactions newer than the local index.",
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
                    "operationId": "getNetworkTransactionStatus",
                    "tags": ["Transactions", "Network"],
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
                    "operationId": "validateTransaction",
                    "tags": ["Transactions"],
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
                    "operationId": "getTransactionStatus",
                    "tags": ["Transactions", "Network"],
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
                    "operationId": "getTickTransactionStatus",
                    "tags": ["Ticks", "Network"],
                    "summary": "Transaction confirmation status from a direct peer",
                    "description": "Peer-side status for every transaction in a tick, including whether money moved for each hash.",
                    "parameters": [path_param("tick", "Global tick number")],
                    "responses": {
                        "200": { "description": "Peer transaction status" },
                        "503": { "description": "No peer available" }
                    }
                }
            },
            "/v1/ticks/{tick}/raw": {
                "get": {
                    "operationId": "getRawTickBundle",
                    "tags": ["Ticks", "Archive"],
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
                    "operationId": "queryContract",
                    "tags": ["Contracts", "Network"],
                    "summary": "Call a read-only contract function on a direct peer",
                    "description": "Queries a contract index with a raw input type and hex payload against a direct peer. No ABI or contract name is assumed; the caller supplies the encoded input and receives raw output bytes.",
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
                    "operationId": "listPrices",
                    "tags": ["Prices"],
                    "summary": "Hourly QUBIC/USDT points, ascending",
                    "description": "Points recorded from the latest verified QUBIC/USDT Price oracle reply or the QPayhub GetQuUsdPrice fallback. When neither on-chain source exists, rows come from the external exchange consensus and omit on-chain-only fields such as epoch, ticks and series_digest.",
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
                    "operationId": "getPriceHistory",
                    "tags": ["Prices"],
                    "summary": "Trailing QUBIC/USDT price window",
                    "description": "The most recent stored points for a requested number of hours, ascending. Missing hours are omitted rather than zero-filled.",
                    "parameters": [{ "name": "hours", "in": "query", "schema": { "type": "integer", "minimum": 1, "maximum": 1000, "default": 24 } }],
                    "responses": { "200": { "description": "Ascending price window" } }
                }
            },
            "/v1/prices/latest": {
                "get": {
                    "operationId": "getLatestPrice",
                    "tags": ["Prices"],
                    "summary": "Latest stored QUBIC/USDT price point",
                    "description": "The newest stored oracle sample. The response is null before the first successful poll.",
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
                    "summary": "QUBIC/USDT price metadata and latest point",
                    "description": "Source, staleness, series digest, and the latest point in one response. Use this to render a price header without a second request.",
                    "responses": {
                        "200": { "description": "Price snapshot; data is null before the first poll", "content": { "application/json": { "schema": { "type": "object", "properties": { "data": { "oneOf": [{ "$ref": "#/components/schemas/PriceSnapshot" }, { "type": "null" }] }, "meta": { "type": "object" } } } } } },
                        "500": { "description": "Storage failure" }
                    }
                }
            },
            "/v1/market": {
                "get": {
                    "operationId": "getMarket",
                    "tags": ["Market", "Prices"],
                    "summary": "Current QUBIC/USDT market snapshot",
                    "description": "Cached external exchange consensus plus CoinGecko reference data. MEXC and Gate.io spot tickers provide the price; CoinGecko supplies market cap, 24-hour volume, and 24-hour change. The endpoint never fans out to an exchange per request.",
                    "responses": {
                        "200": { "description": "Market snapshot" },
                        "503": { "description": "No market snapshot has been fetched yet" }
                    }
                }
            },
            "/v1/market/qubic": {
                "get": {
                    "operationId": "getMarketQubic",
                    "tags": ["Market"],
                    "summary": "QUBIC market alias",
                    "description": "Alias for /v1/market. Returns the same cached QUBIC/USDT market snapshot.",
                    "responses": {
                        "200": { "description": "Market snapshot" },
                        "503": { "description": "No market snapshot has been fetched yet" }
                    }
                }
            },
            "/v1/prices/market": {
                "get": {
                    "operationId": "getPriceMarket",
                    "tags": ["Market", "Prices"],
                    "summary": "Market price alias",
                    "description": "Alias for /v1/market. Returns the external QUBIC/USDT price and market reference, not an on-chain price series point.",
                    "responses": {
                        "200": { "description": "Market snapshot" },
                        "503": { "description": "No market snapshot has been fetched yet" }
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
            "/v1/transactions/{hash}/effects",
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
            "/v1/market",
            "/v1/market/qubic",
            "/v1/prices/market",
            "/v1/wallets/{identity}",
            "/v1/wallets/{identity}/assets",
            "/v1/wallets/{identity}/snapshot",
            "/v1/assets/{issuer}/{name}/holders",
            "/v1/search",
        ] {
            assert!(paths.contains_key(path), "missing {path}");
        }
        for tag in spec["tags"].as_array().expect("tags array") {
            let name = tag["name"].as_str().expect("tag name");
            assert!(
                name.chars().next().is_some_and(char::is_uppercase),
                "tag names are displayed in docs and should be title case: {name}"
            );
        }
        for (path, item) in paths {
            for (method, operation) in item.as_object().expect("path item") {
                assert!(
                    operation.get("operationId").is_some(),
                    "{method} {path} needs an operationId"
                );
            }
        }
    }
}
