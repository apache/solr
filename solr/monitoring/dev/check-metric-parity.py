#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Check that both collection routes deliver the same metric names.

The dev stack scrapes Solr directly as job "solr" and the OpenTelemetry Collector
the nodes push to as job "solr-otlp". The Collector config transforms names so the
two agree; this asks Prometheus whether they actually do.

Run it with the stack up (./stack.sh) and the trafficgen running:

    ./check-metric-parity.py

Exit code 0 when the names match, 1 when they differ, 2 when the stack is not in a
state where the question can be answered. Values are not compared, only names.
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

SCRAPE_JOB = "solr"
OTLP_JOB = "solr-otlp"

# Added by Prometheus for every target rather than coming from Solr
SYNTHETIC = {"up", "ALERTS", "ALERTS_FOR_STATE"}
SYNTHETIC_PREFIX = "scrape_"

# Differences that are currently expected, and why. Empty this as they are fixed.
KNOWN_DIFFERENCES = {
    "target_info": "the Collector cannot emit it until Solr sends service.instance.id (SOLR-18594)",
}


def die(message):
    """Exit 2: the stack is not in a state where the question can be answered."""
    print(message, file=sys.stderr)
    sys.exit(2)


def query(base_url, path, params):
    url = f"{base_url}/api/v1/{path}?{urllib.parse.urlencode(params, doseq=True)}"
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            payload = json.load(response)
    except urllib.error.URLError as e:
        die(f"Cannot reach Prometheus at {base_url}: {e.reason}\nIs the stack up? ./stack.sh")
    if payload.get("status") != "success":
        die(f"Prometheus rejected {path}: {payload.get('error', payload)}")
    return payload["data"]


def metric_names(base_url, job):
    """Metric names Prometheus holds for one job, Solr's own only."""
    names = query(base_url, "label/__name__/values", {"match[]": '{job="%s"}' % job})
    return {
        n for n in names if n not in SYNTHETIC and not n.startswith(SYNTHETIC_PREFIX)
    }


def check_targets(base_url, settle):
    """Both jobs must have a healthy target, or the comparison is meaningless.

    Retries, since Prometheus needs a scrape interval or two after start before it has
    targets at all, and the stack is usually only seconds old when this runs.
    """
    deadline = time.monotonic() + settle
    while True:
        targets = query(base_url, "targets", {"state": "active"})["activeTargets"]
        problems = []
        for job in (SCRAPE_JOB, OTLP_JOB):
            if any(t["labels"].get("job") == job and t["health"] == "up" for t in targets):
                continue
            configured = [t for t in targets if t["labels"].get("job") == job]
            if not configured:
                problems.append(f"no target for job '{job}'")
            else:
                why = configured[0].get("lastError") or configured[0]["health"]
                problems.append(f"job '{job}' has no healthy target: {why}")
        if not problems:
            return
        if time.monotonic() >= deadline:
            hint = "\nIs this the dev stack's Prometheus, and is the stack up? ./stack.sh"
            if any(SCRAPE_JOB in p for p in problems):
                hint += (
                    "\nA node pushing OTLP answers HTTP 500 on /solr/admin/metrics without "
                    "the SOLR-18595 fix, which takes the 'solr' job down."
                )
            die("; ".join(problems) + hint)
        time.sleep(2)


def collect(base_url, timeout, poll, stable_polls, verbose):
    """Poll until both name sets stop growing, so late registries are not read as differences."""
    deadline = time.monotonic() + timeout
    previous = None
    unchanged = 0
    while True:
        current = (metric_names(base_url, SCRAPE_JOB), metric_names(base_url, OTLP_JOB))
        if verbose:
            print(f"  {SCRAPE_JOB}: {len(current[0])}  {OTLP_JOB}: {len(current[1])}")
        unchanged = unchanged + 1 if current == previous else 0
        previous = current
        if unchanged >= stable_polls:
            return current
        if time.monotonic() >= deadline:
            print(
                f"Name counts still changing after {timeout}s; comparing anyway.",
                file=sys.stderr,
            )
            return current
        time.sleep(poll)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--prometheus", default="http://localhost:9090", help="Prometheus base URL")
    parser.add_argument("--timeout", type=int, default=180, help="seconds to wait for a stable set")
    parser.add_argument("--poll", type=int, default=10, help="seconds between polls")
    parser.add_argument("--stable", type=int, default=3, help="unchanged polls before comparing")
    parser.add_argument("--settle", type=int, default=60, help="seconds to wait for healthy targets")
    parser.add_argument(
        "--list",
        action="store_true",
        help="also print every metric name alphabetically, and which routes have it",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="print each poll")
    args = parser.parse_args()

    base_url = args.prometheus.rstrip("/")
    check_targets(base_url, args.settle)

    print(f"Waiting for both jobs to settle (up to {args.timeout}s)...")
    scraped, pushed = collect(base_url, args.timeout, args.poll, args.stable, args.verbose)

    only_scraped = scraped - pushed
    only_pushed = pushed - scraped
    known = {n: KNOWN_DIFFERENCES[n] for n in only_scraped | only_pushed if n in KNOWN_DIFFERENCES}
    only_scraped -= known.keys()
    only_pushed -= known.keys()

    if args.list:
        for name in sorted(scraped | pushed):
            where = "both" if name in scraped and name in pushed else (
                SCRAPE_JOB if name in scraped else OTLP_JOB
            )
            print(f"{where:<10} {name}")
        print()

    print(f"{len(scraped)} names via {SCRAPE_JOB}, {len(pushed)} via {OTLP_JOB}")
    for name, reason in sorted(known.items()):
        print(f"  known difference: {name} — {reason}")
    for name in sorted(only_scraped):
        print(f"  only via {SCRAPE_JOB}:     {name}")
    for name in sorted(only_pushed):
        print(f"  only via {OTLP_JOB}: {name}")

    if only_scraped or only_pushed:
        print(
            "\nThe routes disagree. Either the Collector config in "
            "../otel-collector-config.yaml needs a transform, or one was made obsolete "
            "by a Solr, SDK or Collector upgrade."
        )
        return 1
    print("\nParity OK: both routes deliver the same metric names.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
