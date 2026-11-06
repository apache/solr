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

JSONNET      = jsonnet
JSONNETFMT   = jsonnetfmt
JB           = jb
GOJSONTOYAML = gojsontoyaml
PROMTOOL     = promtool

JSONNET_FLAGS = -J vendor
FMT_FLAGS     = -n 2 --max-blank-lines 2 --string-style s --comment-style s

DASHBOARD_OUT = ../grafana-solr-dashboard.json
ALERTS_OUT    = ../prometheus-solr-alerts.yml

# Generated artifacts are written to a temp file and moved into place only on
# success, so a missing tool or a jsonnet error can never leave a committed
# artifact truncated.  pipefail makes a failure anywhere in a pipe fatal.
SHELL      := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

# Abort before touching any output if a required tool is missing.
require_tool = @command -v $(1) >/dev/null 2>&1 || { \
	  echo "ERROR: required tool '$(1)' not found in PATH." >&2; \
	  echo "       Install the local toolchain (see ../README.md, 'Mixin Build')," >&2; \
	  echo "       or run ./make.sh $@ to build inside Docker instead." >&2; \
	  exit 1; }

require_vendor = @test -d vendor || { \
	  echo "ERROR: vendor/ is missing — run 'make install' (or ./make.sh install) first." >&2; \
	  exit 1; }

.PHONY: all dashboards alerts install check fmt clean

all: dashboards alerts

install:
	$(call require_tool,$(JB))
	$(JB) install
	@echo "→ Installed jsonnet dependencies into vendor/"

dashboards:
	$(call require_tool,$(JSONNET))
	$(require_vendor)
	$(JSONNET) $(JSONNET_FLAGS) \
	  -e '(import "mixin.libsonnet").grafanaDashboards["grafana-solr-dashboard.json"]' \
	  > $(DASHBOARD_OUT).tmp || { rm -f $(DASHBOARD_OUT).tmp; exit 1; }
	mv $(DASHBOARD_OUT).tmp $(DASHBOARD_OUT)
	@echo "→ Generated $(DASHBOARD_OUT)"

alerts:
	$(call require_tool,$(JSONNET))
	$(call require_tool,$(GOJSONTOYAML))
	$(require_vendor)
	cat license-header.yml > $(ALERTS_OUT).tmp
	$(JSONNET) $(JSONNET_FLAGS) \
	  -e '(import "mixin.libsonnet").prometheusAlerts' \
	  | $(GOJSONTOYAML) >> $(ALERTS_OUT).tmp || { rm -f $(ALERTS_OUT).tmp; exit 1; }
	mv $(ALERTS_OUT).tmp $(ALERTS_OUT)
	@echo "→ Generated $(ALERTS_OUT)"

check:
	$(call require_tool,$(PROMTOOL))
	$(PROMTOOL) check rules $(ALERTS_OUT)
	@echo "→ Alert rules validated OK"

fmt:
	$(call require_tool,$(JSONNETFMT))
	find . -path ./vendor -prune -o -name '*.libsonnet' -exec $(JSONNETFMT) $(FMT_FLAGS) -i {} \;
	@echo "→ Formatted all .libsonnet files (vendor/ excluded)"

clean:
	rm -f $(DASHBOARD_OUT) $(ALERTS_OUT) $(DASHBOARD_OUT).tmp $(ALERTS_OUT).tmp
	@echo "→ Removed generated artifacts"
