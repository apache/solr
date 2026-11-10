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
"""Inspect the metrics the dev stack collects.

The stack scrapes Solr directly as job "solr" and the OpenTelemetry Collector the
nodes push to as job "solr-otlp", and the Collector config transforms names so the
two agree. Run with the stack up (./stack.sh) and the trafficgen running:

    ./inspect-metrics.py parity      # do both routes deliver the same names?
    ./inspect-metrics.py list        # every name, and which routes have it
    ./inspect-metrics.py list --otel # the OpenTelemetry names Solr pushes

Exit code 0 on success, 1 when parity fails, 2 when the stack is not in a state
where the question can be answered. Names only; values are never compared.
"""

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

SCRAPE_JOB = "solr"
OTLP_JOB = "solr-otlp"

# The collector's second endpoint, added by the dev-only otel-collector-native.yaml overlay,
# which leaves OpenTelemetry names untranslated. Dotted names only survive the wire when the
# reader says it accepts UTF-8, otherwise they come back escaped to underscores.
NATIVE_ACCEPT = "text/plain;version=0.0.4;escaping=allow-utf-8"
TYPE_LINE = re.compile(r'^# TYPE "?([^"\s]+)"? ')

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


def otel_metric_names(url):
    """The names Solr pushes, before any Prometheus naming is applied."""
    request = urllib.request.Request(url, headers={"Accept": NATIVE_ACCEPT})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
    except urllib.error.URLError as e:
        die(
            f"Cannot reach the collector's untranslated endpoint at {url}: {e.reason}\n"
            "It is added by otel-collector-native.yaml; is the stack up? ./stack.sh"
        )
    names = {m.group(1) for m in (TYPE_LINE.match(line) for line in body.splitlines()) if m}
    if not names:
        die(f"No metrics at {url} yet; is Solr pushing?")
    return names


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
            die(
                "; ".join(problems)
                + "\nIs this the dev stack's Prometheus, and is the stack up? ./stack.sh"
            )
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


def add_common_arguments(parser):
    parser.add_argument("--prometheus", default="http://localhost:9090", help="Prometheus base URL")
    parser.add_argument("--timeout", type=int, default=180, help="seconds to wait for a stable set")
    parser.add_argument("--poll", type=int, default=10, help="seconds between polls")
    parser.add_argument("--stable", type=int, default=3, help="unchanged polls before comparing")
    parser.add_argument("--settle", type=int, default=60, help="seconds to wait for healthy targets")
    parser.add_argument("-v", "--verbose", action="store_true", help="print each poll")


def both_routes(args):
    """The names each route delivers, once both have stopped producing new ones."""
    base_url = args.prometheus.rstrip("/")
    check_targets(base_url, args.settle)
    print(f"Waiting for both jobs to settle (up to {args.timeout}s)...", file=sys.stderr)
    return collect(base_url, args.timeout, args.poll, args.stable, args.verbose)


def command_parity(args):
    scraped, pushed = both_routes(args)
    only_scraped = scraped - pushed
    only_pushed = pushed - scraped
    known = {n: KNOWN_DIFFERENCES[n] for n in only_scraped | only_pushed if n in KNOWN_DIFFERENCES}
    only_scraped -= known.keys()
    only_pushed -= known.keys()

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


def command_list(args):
    if args.otel:
        for name in sorted(otel_metric_names(args.collector_native)):
            print(name)
        return 0
    scraped, pushed = both_routes(args)
    for name in sorted(scraped | pushed):
        where = "both" if name in scraped and name in pushed else (
            SCRAPE_JOB if name in scraped else OTLP_JOB
        )
        print(f"{where:<10} {name}")
    return 0


def main():
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        epilog="Run a subcommand with --help for its options.",
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    parity = subcommands.add_parser("parity", help="compare the names both routes deliver")
    add_common_arguments(parity)
    parity.set_defaults(run=command_parity)

    listing = subcommands.add_parser("list", help="print metric names alphabetically")
    add_common_arguments(listing)
    listing.add_argument(
        "--otel",
        action="store_true",
        help="the OpenTelemetry names Solr pushes, before any Prometheus naming",
    )
    listing.add_argument(
        "--collector-native",
        default="http://localhost:9465/metrics",
        help="collector endpoint serving untranslated OpenTelemetry names",
    )
    listing.set_defaults(run=command_list)

    args = parser.parse_args()
    return args.run(args)


if __name__ == "__main__":
    sys.exit(main())
