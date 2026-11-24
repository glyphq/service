#!/usr/bin/env python3
"""Concurrency benchmark for the Glyph read API.

Runs only the Python standard library so it can be copied to the production
host. Each worker keeps one HTTP connection and loops until the duration ends.

Example:
    python3 scripts/bench-load.py \
        --base http://127.0.0.1:8080 \
        --path '/v1/transactions?limit=50' \
        --concurrency 20 --duration 10
"""

import argparse
import http.client
import threading
import time
from collections import Counter
from urllib.parse import urlsplit


def percentile(sorted_values, fraction):
    if not sorted_values:
        return 0.0
    index = min(len(sorted_values) - 1, int((len(sorted_values) - 1) * fraction))
    return sorted_values[index]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:8080")
    parser.add_argument("--path", required=True)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--duration", type=float, default=10.0)
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args()

    target = urlsplit(args.base)
    host = target.hostname or "127.0.0.1"
    port = target.port or (443 if target.scheme == "https" else 80)
    path = args.path

    latencies = []
    statuses = Counter()
    errors = Counter()
    lock = threading.Lock()
    stop_at = time.monotonic() + args.duration
    start_gate = threading.Event()

    def worker():
        connection = None
        local_latencies = []
        local_statuses = Counter()
        local_errors = Counter()
        try:
            start_gate.wait()
            while time.monotonic() < stop_at:
                if connection is None:
                    connection = http.client.HTTPConnection(host, port, timeout=args.timeout)
                started = time.monotonic()
                try:
                    connection.request("GET", path)
                    response = connection.getresponse()
                    response.read()
                    local_statuses[response.status] += 1
                    if response.will_close:
                        connection.close()
                        connection = None
                except Exception as error:  # noqa: BLE001 - benchmark records all failures
                    local_errors[type(error).__name__] += 1
                    if connection is not None:
                        connection.close()
                    connection = None
                local_latencies.append((time.monotonic() - started) * 1000.0)
        finally:
            if connection is not None:
                connection.close()
            with lock:
                latencies.extend(local_latencies)
                statuses.update(local_statuses)
                errors.update(local_errors)

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(args.concurrency)]
    for thread in threads:
        thread.start()
    started = time.monotonic()
    start_gate.set()
    for thread in threads:
        thread.join()
    elapsed = time.monotonic() - started

    latencies.sort()
    requests = len(latencies)
    failures = sum(count for status, count in statuses.items() if status >= 400) + sum(errors.values())
    print(
        "path={} concurrency={} duration={:.1f}s requests={} rps={:.1f} "
        "p50={:.1f}ms p95={:.1f}ms p99={:.1f}ms max={:.1f}ms errors={} statuses={} error_types={}".format(
            path,
            args.concurrency,
            elapsed,
            requests,
            requests / elapsed if elapsed > 0 else 0.0,
            percentile(latencies, 0.50),
            percentile(latencies, 0.95),
            percentile(latencies, 0.99),
            latencies[-1] if latencies else 0.0,
            failures,
            dict(sorted(statuses.items())),
            dict(sorted(errors.items())),
        )
    )
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
