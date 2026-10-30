#!/usr/bin/env bash
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

# check-otel-parity.sh — verify that the OTel collector re-exposes the same Prometheus
# metric names, types and label keys as Solr's own metrics endpoint.
#
# Usage:
#   ./check-otel-parity.sh [SOLR_METRICS_URL] [COLLECTOR_METRICS_URL]
#
# Defaults: http://localhost:8983/solr/admin/metrics and http://localhost:9464/metrics
# (the bundled solr1 and otel-collector of this stack). Exit code 1 on any difference,
# 2 if either endpoint cannot be read.
#
# Both endpoints are fetched with the Accept header Prometheus sends, and compared the
# way Prometheus ingests them: empty-valued labels are dropped, and the job/instance
# labels are ignored since Prometheus sets those itself on a direct scrape. Metrics
# that only exist on one side because the event never happened there (e.g. no errors
# yet) show up as differences; let the trafficgen run a minute first.

set -euo pipefail

SOLR_URL="${1:-http://localhost:8983/solr/admin/metrics}"
COLLECTOR_URL="${2:-http://localhost:9464/metrics}"
ACCEPT='application/openmetrics-text;version=1.0.0;q=0.5,application/openmetrics-text;version=0.0.1;q=0.4,text/plain;version=1.0.0;q=0.3,text/plain;version=0.0.4;q=0.2,*/*;q=0.1'

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

fetch() {
  if ! curl -sf -H "Accept: $ACCEPT" "$1" > "$2"; then
    echo "Could not read $1" >&2
    echo "(Solr answers HTTP 500 there with OTLP push enabled on a build predating SOLR-18595)" >&2
    exit 2
  fi
}

fetch "$SOLR_URL" "$tmp/solr.txt"
fetch "$COLLECTOR_URL" "$tmp/collector.txt"

# One line per series: <name> <sorted label keys>, plus one per family: TYPE <name> <type>
#
# Label values are quoted and may contain commas, spaces, braces and escaped quotes
# (e.g. jvm_memory_pool_name="G1 Eden Space"), so the label list is parsed
# quote-aware rather than split on whitespace: squash() first reduces every value to
# "" or "x", after which splitting on commas is safe. Keys are sorted, since their
# order on the wire is not significant to Prometheus.
signature() {
  awk '
    function squash(line,   out, i, c, inq, hasval) {
      out = ""; inq = 0; hasval = 0
      for (i = 1; i <= length(line); i++) {
        c = substr(line, i, 1)
        if (inq) {
          if (c == "\\") { i++; hasval = 1; continue }   # escape: skip the next char
          if (c == "\"") { out = out (hasval ? "x\"" : "\""); inq = 0; continue }
          hasval = 1
          continue
        }
        out = out c
        if (c == "\"") { inq = 1; hasval = 0 }
      }
      return out
    }
    /^#/ || /^[ \t]*$/ { next }
    {
      s = squash($0)
      keys = ""
      b = index(s, "{")
      e = index(s, "}")
      if (b == 0 || e < b) {
        name = s
        sub(/[ \t].*$/, "", name)
      } else {
        name = substr(s, 1, b - 1)
        n = split(substr(s, b + 1, e - b - 1), kv, ",")
        split("", k)
        m = 0
        for (i = 1; i <= n; i++) {
          eq = index(kv[i], "=")
          if (eq == 0) continue
          key = substr(kv[i], 1, eq - 1)
          val = substr(kv[i], eq + 1)
          # Prometheus drops empty-valued labels, and sets job/instance itself.
          if (val == "\"\"" || key == "job" || key == "instance") continue
          k[++m] = key
        }
        for (i = 2; i <= m; i++) {          # insertion sort
          t = k[i]
          for (j = i - 1; j >= 1 && k[j] > t; j--) k[j + 1] = k[j]
          k[j + 1] = t
        }
        for (i = 1; i <= m; i++) keys = keys (i > 1 ? "," : "") k[i]
      }
      print name, keys
    }' "$1" | sort -u
  grep '^# TYPE' "$1" | awk '{print "TYPE", $3, $4}' | sort -u
}

signature "$tmp/solr.txt" | sort > "$tmp/solr.sig"
signature "$tmp/collector.txt" | sort > "$tmp/collector.sig"

status=0
if ! diff_out="$(diff "$tmp/solr.sig" "$tmp/collector.sig")"; then
  status=1
  echo "Differences (< only via Solr, > only via collector):"
  echo "$diff_out" | grep '^[<>]'
fi

solr_names=$(awk '$1!="TYPE"{print $1}' "$tmp/solr.sig" | sort -u | wc -l | tr -d ' ')
coll_names=$(awk '$1!="TYPE"{print $1}' "$tmp/collector.sig" | sort -u | wc -l | tr -d ' ')
echo "Metric names: $solr_names via Solr, $coll_names via collector."
[[ $status -eq 0 ]] && echo "Parity OK: identical names, types and label keys."
exit $status
