// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// mixin.libsonnet — root entry point for the Solr 10.x monitoring mixin.
//
// Exposes:
//   grafanaDashboards::  — map of filename -> Grafana dashboard JSON object
//   prometheusAlerts::   — Prometheus alert groups object (for -S / YAML output)
//
// Usage:
//   jsonnet -J vendor -e '(import "mixin.libsonnet").grafanaDashboards["grafana-solr-dashboard.json"]' \
//     > ../grafana-solr-dashboard.json
//   jsonnet -J vendor -S -e '(import "mixin.libsonnet").prometheusAlerts' \
//     | gojsontoyaml > ../prometheus-solr-alerts.yml

local config = import 'config.libsonnet';
local dashboards = import 'dashboards/dashboards.libsonnet';
local alerts = import 'alerts/alerts.libsonnet';

config {
  // grafanaDashboards is a map from output filename to dashboard JSON.
  // The Makefile uses the key "grafana-solr-dashboard.json" to generate the output file.
  grafanaDashboards:: dashboards,

  // prometheusAlerts is the Prometheus alert rule groups structure.
  // Pipe through gojsontoyaml to produce the YAML file.
  prometheusAlerts:: alerts,
}
