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

# ============================================================================
# Build image for the Solr monitoring mixin.
# Contains: jsonnet, jsonnetfmt, jb (jsonnet-bundler), gojsontoyaml, promtool.
#
# Not intended for production use — developer tooling only.
# See make.sh for usage.
# ============================================================================

# --- Stage 1: compile Go-based tools -----------------------------------------
FROM golang:1-bookworm AS go-tools

ENV CGO_ENABLED=0

RUN go install github.com/google/go-jsonnet/cmd/jsonnet@latest    \
 && go install github.com/google/go-jsonnet/cmd/jsonnetfmt@latest \
 && go install github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb@latest \
 && go install github.com/brancz/gojsontoyaml@latest

# --- Stage 2: extract promtool from the official Prometheus image -------------
FROM prom/prometheus:latest AS prometheus

# --- Stage 3: lean runtime image ---------------------------------------------
FROM ubuntu:24.04

RUN apt-get update \
 && apt-get install -y --no-install-recommends make ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=go-tools   /go/bin/jsonnet      /usr/local/bin/
COPY --from=go-tools   /go/bin/jsonnetfmt   /usr/local/bin/
COPY --from=go-tools   /go/bin/jb           /usr/local/bin/
COPY --from=go-tools   /go/bin/gojsontoyaml /usr/local/bin/
COPY --from=prometheus /bin/promtool        /usr/local/bin/

# The mixin source tree is expected to be mounted at /work/mixin
# (i.e., the monitoring/ directory is mounted as /work).
WORKDIR /work/mixin

ENTRYPOINT ["make"]
CMD ["all"]
