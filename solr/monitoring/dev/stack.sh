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
#                                         at localhost:8983 and :8984 (no bundled Solr started)
#   ./stack.sh --local-solr URL           Same, but trafficgen is also pointed at URL
#                                         (implies --trafficgen)
#   ./stack.sh --local-solr --trafficgen  Local mode + trafficgen at default localhost:8983
#   ./stack.sh --down                     Stop and remove all containers
#   ./stack.sh --help                     Show this help
#
# In --local-solr mode Prometheus uses prometheus/prometheus-local.yml which scrapes
# host.docker.internal:8983 and :8984.  Edit that file to change ports or targets.
# The metrics_path is /api/metrics (Solr 10.1+ built from source); change to
# /solr/admin/metrics for released Solr 10.0 builds.

set -euo pipefail

cd "$(dirname "$0")"

LOCAL_MODE=false
WITH_TRAFFICGEN=false
CUSTOM_SOLR_URL=""
DOWN_MODE=false

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
    --down)       _args+=(-d) ;;
    --help)       _args+=(-h) ;;
    *)            _args+=("$_a") ;;
  esac
done
set -- "${_args[@]+"${_args[@]}"}"

while getopts "ltdh" opt; do
  case "$opt" in
    l) LOCAL_MODE=true ;;
    t) WITH_TRAFFICGEN=true ;;
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

if $LOCAL_MODE; then
  export PROMETHEUS_CONFIG="./prometheus/prometheus-local.yml"

  if [[ -n "$CUSTOM_SOLR_URL" ]]; then
    # Replace 'localhost' with host.docker.internal so trafficgen can reach the host
    export SOLR_URL="${CUSTOM_SOLR_URL/localhost/host.docker.internal}"
  else
    export SOLR_URL="http://host.docker.internal:8983/solr"
  fi

  SERVICES="prometheus grafana alertmanager"
  if $WITH_TRAFFICGEN; then
    SERVICES="$SERVICES trafficgen"
  fi

  # shellcheck disable=SC2086
  docker compose up -d $SERVICES

  echo ""
  echo "Monitoring stack started in local-Solr mode."
  echo "Prometheus is scraping your local Solr via host.docker.internal"
  if $WITH_TRAFFICGEN; then
    echo "Traffic generator is running against: $SOLR_URL"
  fi
  echo ""
  echo "  Grafana:    http://localhost:3000  (admin / admin)"
  echo "  Prometheus: http://localhost:9090"
  echo "  Alertmanager: http://localhost:9093"
  echo ""
  echo "Tip: Prometheus may show scrape errors for :8984 if you are running only one Solr node."
  echo "     Edit prometheus/prometheus-local.yml to adjust scrape targets."
else
  # Bundled mode: activate the bundled-solr profile so solr1 and solr2 start
  COMPOSE_PROFILES=bundled-solr docker compose up -d

  echo ""
  echo "Full monitoring stack started."
  echo "  Solr 1:       http://localhost:8983"
  echo "  Solr 2:       http://localhost:8984"
  echo "  Grafana:      http://localhost:3000  (admin / admin)"
  echo "  Prometheus:   http://localhost:9090"
  echo "  Alertmanager: http://localhost:9093"
  echo ""
  echo "Tip: Prometheus may show scrape errors for the first 30-60 seconds while Solr starts."
fi
