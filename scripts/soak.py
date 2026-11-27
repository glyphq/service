#!/usr/bin/env python3
"""Long-running read-path soak for the Glyph API.

The default is seven days at one request every ten seconds. Every cycle reads
the status document and a spread of index, analytics, explorer, contract, and
label endpoints. Any non-2xx response, invalid JSON, or transport error fails
the run. The status head is checked for forward progress and a stall is logged.

Example:
    python3 scripts/soak.py --base https://api.glyphq.org --log soak.jsonl
"""

import argparse
import http.client
import json
import signal
import sys
import time
from urllib.parse import urlsplit

ENDPOINTS = [
    "/healthz",
    "/v1/status",
    "/v1/ticks?limit=1",
    "/v1/transactions?limit=1",
    "/v1/events?limit=1",
    "/v1/identities?limit=1",
    "/v1/assets?limit=1",
    "/v1/analytics/activity?interval=3600&limit=2",
    "/v1/analytics/active-identities?interval=3600&limit=2",
    "/v1/analytics/whales?limit=2",
    "/v1/analytics/event-types?limit=5",
    "/v1/contracts?limit=5",
    "/v1/contracts/dividends?limit=5",
    "/v1/labels?limit=5",
    "/v1/explorer/overview?limit=2",
    "/v1/network/index",
    "/v1/market",
    "/v1/prices",
]


def request(connection, base, path, timeout):
    started = time.monotonic()
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        body = response.read()
        elapsed = (time.monotonic() - started) * 1000.0
        return response.status, body, elapsed, None
    except Exception as error:  # noqa: BLE001 - soak records every failure
        elapsed = (time.monotonic() - started) * 1000.0
        return None, b"", elapsed, f"{type(error).__name__}: {error}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:8080")
    parser.add_argument("--duration", type=float, default=7 * 24 * 3600)
    parser.add_argument("--interval", type=float, default=10.0)
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--log", help="append one JSON line per cycle")
    parser.add_argument("--stall-cycles", type=int, default=30)
    args = parser.parse_args()

    target = urlsplit(args.base)
    host = target.hostname or "127.0.0.1"
    port = target.port or (443 if target.scheme == "https" else 80)
    connection_class = (
        http.client.HTTPSConnection if target.scheme == "https" else http.client.HTTPConnection
    )
    connection = connection_class(host, port, timeout=args.timeout)
    log = open(args.log, "a", encoding="utf-8") if args.log else None
    stop = False

    def handle_signal(_signum, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    started_at = time.monotonic()
    deadline = started_at + args.duration
    failures = 0
    cycles = 0
    last_head = None
    unchanged_cycles = 0

    while not stop and time.monotonic() < deadline:
        cycle_started = time.monotonic()
        cycle = {"at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "cycles": cycles}
        cycle_failures = []
        head = None
        latencies = []

        for path in ENDPOINTS:
            status, body, elapsed, error = request(connection, args.base, path, args.timeout)
            latencies.append(round(elapsed, 1))
            if error is not None:
                cycle_failures.append({"path": path, "error": error})
                connection.close()
                connection = connection_class(host, port, timeout=args.timeout)
                continue
            if status < 200 or status >= 300:
                cycle_failures.append({"path": path, "status": status})
                continue
            if path != "/healthz":
                try:
                    payload = json.loads(body)
                except json.JSONDecodeError as error:
                    cycle_failures.append({"path": path, "invalid_json": str(error)})
                    continue
                if path == "/v1/status":
                    head = payload.get("data", {}).get("head", {}).get("tick")

        if head is not None:
            if last_head is None or head > last_head:
                last_head = head
                unchanged_cycles = 0
            else:
                unchanged_cycles += 1
            if unchanged_cycles >= args.stall_cycles:
                cycle_failures.append(
                    {
                        "stall": True,
                        "head_tick": head,
                        "unchanged_cycles": unchanged_cycles,
                    }
                )

        cycle["failures"] = cycle_failures
        cycle["head_tick"] = head
        cycle["latency_ms"] = latencies
        failures += len(cycle_failures)
        cycles += 1
        if log:
            log.write(json.dumps(cycle, separators=(",", ":")) + "\n")
            log.flush()
        if cycle_failures:
            print(json.dumps(cycle, separators=(",", ":")), flush=True)
        elif cycles == 1 or cycles % 360 == 0:
            print(
                f"soak cycles={cycles} head={head} elapsed={int(time.monotonic() - started_at)}s",
                flush=True,
            )

        sleep_for = args.interval - (time.monotonic() - cycle_started)
        if sleep_for > 0:
            end = time.monotonic() + sleep_for
            while not stop and time.monotonic() < end:
                time.sleep(min(1.0, end - time.monotonic()))

    if log:
        log.close()
    print(
        f"soak finished cycles={cycles} failures={failures} "
        f"head={last_head} elapsed={int(time.monotonic() - started_at)}s",
        flush=True,
    )
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
