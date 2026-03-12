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

.PHONY: all dashboards alerts install check fmt clean

all: dashboards alerts

install:
	$(JB) install

dashboards:
	$(JSONNET) $(JSONNET_FLAGS) \
	  -e '(import "mixin.libsonnet").grafanaDashboards["grafana-solr-dashboard.json"]' \
	  > ../grafana-solr-dashboard.json

alerts:
	$(JSONNET) $(JSONNET_FLAGS) \
	  -e '(import "mixin.libsonnet").prometheusAlerts' \
	  | $(GOJSONTOYAML) > ../prometheus-solr-alerts.yml

check:
	$(PROMTOOL) check rules ../prometheus-solr-alerts.yml

fmt:
	find . -name '*.libsonnet' -exec $(JSONNETFMT) $(FMT_FLAGS) -i {} \;

clean:
	rm -f ../grafana-solr-dashboard.json ../prometheus-solr-alerts.yml
