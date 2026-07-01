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

# make.sh — Docker-based wrapper around the mixin Makefile.
#
# Usage:
#   ./make.sh              # equivalent to: make all
#   ./make.sh install all  # equivalent to: make install all
#   ./make.sh check        # equivalent to: make check
#   ./make.sh --rebuild    # force-rebuild the Docker image, then make all
#
# The Docker image (solr-mixin-make:latest) is built automatically on first
# use and reused on subsequent runs. Pass --rebuild to force a fresh build
# (e.g. after updating the Dockerfile or tool versions).

set -euo pipefail

IMAGE="solr-mixin-make:latest"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MONITORING_DIR="$(dirname "$SCRIPT_DIR")"

# --- parse flags -------------------------------------------------------------
REBUILD=false
MAKE_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --rebuild) REBUILD=true ;;
    *)         MAKE_ARGS+=("$arg") ;;
  esac
done

# --- build image if missing or explicitly requested --------------------------
if ! docker image inspect "$IMAGE" >/dev/null 2>&1 || [ "$REBUILD" = true ]; then
  echo "[make.sh] Building Docker image $IMAGE ..."
  docker build -t "$IMAGE" "$SCRIPT_DIR"
fi

# --- run make inside the container -------------------------------------------
# Mount the monitoring/ directory as /work so that the Makefile's relative
# paths (../grafana-solr-dashboard.json, ../prometheus-solr-alerts.yml) resolve
# correctly to the host filesystem.
TTY_FLAG=""
[ -t 0 ] && TTY_FLAG="-t"

docker run --rm -i $TTY_FLAG \
  -v "$MONITORING_DIR:/work" \
  "$IMAGE" "${MAKE_ARGS[@]:-all}"
