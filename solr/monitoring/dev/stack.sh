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

# stack.sh — convenience launcher for the Solr monitoring dev stack.
#
# Usage:
#   ./stack.sh                            Full stack: bundled Solr + monitoring + trafficgen
#   ./stack.sh --local-solr               Monitoring only; Prometheus scrapes your local Solr
#                                         at localhost:8983 (no bundled Solr started)
#   ./stack.sh --local-solr URL           Same, but trafficgen is also pointed at URL
#                                         (implies --trafficgen)
#   ./stack.sh --local-solr --trafficgen  Local mode + trafficgen at default localhost:8983
#   ./stack.sh --metrics-scrape=otel      Prometheus scrapes the OTel collector that Solr pushes
#                                         to, instead of Solr directly (default: solr)
#   ./stack.sh --down                     Stop and remove all containers
#   ./stack.sh --help                     Show this help
#
# In --local-solr mode Prometheus uses prometheus/prometheus-local.yml which scrapes
# host.docker.internal:8983.  Edit that file to add further nodes or change the port.
#
# The default for --metrics-scrape can be set with METRICS_SCRAPE in the environment or
# in a ./.env file (gitignored), e.g. METRICS_SCRAPE=otel.

set -euo pipefail

cd "$(dirname "$0")"

LOCAL_MODE=false
WITH_TRAFFICGEN=false
CUSTOM_SOLR_URL=""
DOWN_MODE=false
METRICS_SCRAPE_ARG=""

usage() {
  awk '/^# Usage:/,/^[^#]/' "$0" | grep '^#' | sed 's/^# \{0,1\}//'
  exit 0
}

# Translate long options to short equivalents so getopts can process them uniformly.
_args=()
for _a in "$@"; do
  case "$_a" in
    --local-solr) _args+=(-l) ;;
    --trafficgen) _args+=(-t) ;;
    --metrics-scrape=*) _args+=(-m "${_a#*=}") ;;
    --down)       _args+=(-d) ;;
    --help)       _args+=(-h) ;;
    *)            _args+=("$_a") ;;
  esac
done
set -- "${_args[@]+"${_args[@]}"}"

while getopts "ltm:dh" opt; do
  case "$opt" in
    l) LOCAL_MODE=true ;;
    t) WITH_TRAFFICGEN=true ;;
    m) METRICS_SCRAPE_ARG="$OPTARG" ;;
    d) DOWN_MODE=true ;;
    h) usage ;;
    ?) echo "Unknown option. Run ./stack.sh --help for usage." >&2; exit 1 ;;
  esac
done
shift $((OPTIND - 1))

# Optional URL positional argument following -l / --local-solr
if $LOCAL_MODE && [[ $# -gt 0 ]]; then
  CUSTOM_SOLR_URL="$1"
  WITH_TRAFFICGEN=true
  shift
fi

if $DOWN_MODE; then
  # Include the bundled-solr profile so solr1/solr2 are stopped even if they were started
  # with that profile active.  Passing --profile to a non-running service is harmless.
  docker compose --profile bundled-solr down
  echo "All containers stopped and removed."
  exit 0
fi

# Resolve --metrics-scrape: flag > METRICS_SCRAPE env var > ./.env > solr.
# docker compose reads .env itself for interpolation; this is only for the choice below.
if [[ -z "${METRICS_SCRAPE:-}" && -f .env ]]; then
  METRICS_SCRAPE="$(sed -n 's/^METRICS_SCRAPE=//p' .env | tail -1)"
fi
METRICS_SCRAPE="${METRICS_SCRAPE_ARG:-${METRICS_SCRAPE:-solr}}"
case "$METRICS_SCRAPE" in
  solr|otel) ;;
  *) echo "Unknown --metrics-scrape value '$METRICS_SCRAPE'; use solr or otel." >&2; exit 1 ;;
esac
export METRICS_SCRAPE

if [[ "$METRICS_SCRAPE" == "otel" ]]; then
  export PROMETHEUS_CONFIG="./prometheus/prometheus-otel.yml"
elif $LOCAL_MODE; then
  export PROMETHEUS_CONFIG="./prometheus/prometheus-local.yml"
else
  export PROMETHEUS_CONFIG="./prometheus/prometheus.yml"
fi

if $LOCAL_MODE; then
  if [[ -n "$CUSTOM_SOLR_URL" ]]; then
    # Replace 'localhost' with host.docker.internal so trafficgen can reach the host
    export SOLR_URL="${CUSTOM_SOLR_URL/localhost/host.docker.internal}"
  else
    export SOLR_URL="http://host.docker.internal:8983/solr"
  fi

  SERVICES="prometheus grafana alertmanager otel-collector aspire-dashboard"
  if $WITH_TRAFFICGEN; then
    SERVICES="$SERVICES trafficgen"
  fi

  # shellcheck disable=SC2086
  docker compose up -d $SERVICES

  echo ""
  echo "Monitoring stack started in local-Solr mode (metrics via: $METRICS_SCRAPE)."
  if [[ "$METRICS_SCRAPE" == "otel" ]]; then
    echo "Prometheus is scraping the OTel collector. Start your local Solr with:"
    echo "  SOLR_MODULES=opentelemetry SOLR_METRICS_OTLP_EXPORTER_ENABLED=true bin/solr start"
    echo "(the collector listens on localhost:4318, which is Solr's default OTLP/HTTP endpoint)"
  else
    echo "Prometheus is scraping your local Solr via host.docker.internal"
  fi
  if $WITH_TRAFFICGEN; then
    echo "Traffic generator is running against: $SOLR_URL"
  fi
  echo ""
  echo "  Dashboard:  http://localhost:3000/d/solr-mixin-example"
  echo "  Grafana:    http://localhost:3000  (view without login; admin/admin to edit)"
  echo "  Prometheus: http://localhost:9090"
  echo "  Alertmanager: http://localhost:9093"
  echo "  Collector:  http://localhost:9464/metrics  (what Solr pushed, in Prometheus format)"
  echo "  Aspire:     http://localhost:18888  (raw view of pushed metrics and traces)"
  echo ""
  echo "Tip: edit prometheus/prometheus-local.yml to add further nodes or change the port."
else
  # Bundled mode: activate the bundled-solr profile so solr1 and solr2 start
  COMPOSE_PROFILES=bundled-solr docker compose up -d

  echo ""
  echo "Full monitoring stack started (metrics via: $METRICS_SCRAPE)."
  echo "  Solr 1:       http://localhost:8983"
  echo "  Solr 2:       http://localhost:8984"
  echo "  Dashboard:    http://localhost:3000/d/solr-mixin-example"
  echo "  Grafana:      http://localhost:3000  (view without login; admin/admin to edit)"
  echo "  Prometheus:   http://localhost:9090"
  echo "  Alertmanager: http://localhost:9093"
  echo "  Collector:    http://localhost:9464/metrics  (what Solr pushed, in Prometheus format)"
  echo "  Aspire:       http://localhost:18888  (raw view of pushed metrics and traces)"
  echo ""
  echo "Tip: Prometheus may show scrape errors for the first 30-60 seconds while Solr starts."
fi
