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
# (the bundled solr1 and otel-collector of this stack). Exit code 1 on any difference.
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

curl -sf -H "Accept: $ACCEPT" "$SOLR_URL" > "$tmp/solr.txt"
curl -sf -H "Accept: $ACCEPT" "$COLLECTOR_URL" > "$tmp/collector.txt"

# One line per series: <name> <sorted label keys>, plus one per family: TYPE <name> <type>
signature() {
  grep -v '^#' "$1" | sed -E 's/^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{([^}]*)\})?.*/\1 \3/' \
    | awk '{ n=split($2,kv,","); keys=""
             for(i=1;i<=n;i++){ split(kv[i],p,"=")
               if (p[2]=="\"\"" || p[1]=="job" || p[1]=="instance") continue
               keys=keys (keys?",":"") p[1] }
             print $1, keys }' \
    | sort -u
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
