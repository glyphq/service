use serde_json::{Value, json};

pub fn spec() -> Value {
    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Glyph Qubic API",
            "version": "0.1.0",
            "description": "Resource-oriented API over a from-scratch Qubic index fed by direct peer connections."
        },
        "servers": [{ "url": "/" }],
        "paths": {
            "/v1/status": {
                "get": {
                    "summary": "Indexer head and ingest status",
                    "responses": { "200": { "description": "Current status" } }
                }
            },
            "/v1/ticks/{tick}": {
                "get": {
                    "summary": "Tick by number",
                    "parameters": [{ "name": "tick", "in": "path", "required": true, "schema": { "type": "integer", "minimum": 0 } }],
                    "responses": {
                        "200": { "description": "Tick" },
                        "404": { "description": "Tick not indexed" }
                    }
                }
            },
            "/v1/ticks/{tick}/transactions": {
                "get": {
                    "summary": "Transactions in a tick",
                    "parameters": [
                        { "name": "tick", "in": "path", "required": true, "schema": { "type": "integer" } },
                        { "name": "limit", "in": "query", "schema": { "type": "integer", "default": 50, "maximum": 200 } },
                        { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
                    ],
                    "responses": { "200": { "description": "Paginated transactions" } }
                }
            },
            "/v1/ticks/{tick}/events": {
                "get": {
                    "summary": "Events in a tick",
                    "parameters": [
                        { "name": "tick", "in": "path", "required": true, "schema": { "type": "integer" } },
                        { "name": "limit", "in": "query", "schema": { "type": "integer", "default": 50, "maximum": 200 } },
                        { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
                    ],
                    "responses": { "200": { "description": "Paginated events" } }
                }
            },
            "/v1/transactions/{hash}": {
                "get": {
                    "summary": "Transaction by 60-character hash",
                    "parameters": [{ "name": "hash", "in": "path", "required": true, "schema": { "type": "string" } }],
                    "responses": {
                        "200": { "description": "Transaction" },
                        "400": { "description": "Invalid hash" },
                        "404": { "description": "Transaction not indexed" }
                    }
                }
            },
            "/v1/transactions": {
                "get": {
                    "summary": "Most recent transactions across indexed ticks",
                    "parameters": [
                        { "name": "limit", "in": "query", "schema": { "type": "integer", "default": 50, "maximum": 200 } },
                        { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
                    ],
                    "responses": { "200": { "description": "Paginated transactions" } }
                }
            },
            "/v1/events": {
                "get": {
                    "summary": "Most recent typed events across indexed ticks",
                    "parameters": [
                        { "name": "limit", "in": "query", "schema": { "type": "integer", "default": 50, "maximum": 200 } },
                        { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
                    ],
                    "responses": { "200": { "description": "Paginated events" } }
                }
            },
            "/v1/identities/{identity}": {
                "get": {
                    "summary": "Identity summary and observed QU flow",
                    "parameters": [{ "name": "identity", "in": "path", "required": true, "schema": { "type": "string" } }],
                    "responses": {
                        "200": { "description": "Identity" },
                        "400": { "description": "Invalid identity" },
                        "404": { "description": "Identity not indexed" }
                    }
                }
            },
            "/v1/identities/{identity}/transactions": {
                "get": {
                    "summary": "Transactions sent or received by an identity",
                    "parameters": [
                        { "name": "identity", "in": "path", "required": true, "schema": { "type": "string" } },
                        { "name": "limit", "in": "query", "schema": { "type": "integer", "default": 50, "maximum": 200 } },
                        { "name": "offset", "in": "query", "schema": { "type": "integer", "default": 0 } }
                    ],
                    "responses": {
                        "200": { "description": "Paginated transactions" },
                        "400": { "description": "Invalid identity" }
                    }
                }
            },
            "/v1/search": {
                "get": {
                    "summary": "Resolve a tick, transaction hash, or identity",
                    "parameters": [{ "name": "q", "in": "query", "required": true, "schema": { "type": "string" } }],
                    "responses": { "200": { "description": "Typed matches" } }
                }
            }
        },
        "components": {
            "schemas": {
                "Problem": {
                    "type": "object",
                    "properties": {
                        "type": { "type": "string" },
                        "title": { "type": "string" },
                        "status": { "type": "integer" },
                        "code": { "type": "string" },
                        "detail": { "type": "string" }
                    }
                }
            }
        }
    })
}
