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

// mixin.libsonnet — root entry point for the Solr monitoring mixin.
//
// Composes the configuration with the dashboards and alerts, so that both read
// their settings from the merged root. That late binding is what lets a caller
// override settings without editing this tree:
//
//   (import 'mixin.libsonnet') + { _config+:: { solrSelector: 'job="my-solr"' } }
//
// Exposes:
//   grafanaDashboards::  — map of filename -> Grafana dashboard JSON object
//   prometheusAlerts::   — Prometheus alert groups object
//
// Usage:
//   jsonnet -J vendor -e '(import "mixin.libsonnet").grafanaDashboards["grafana-solr-dashboard.json"]' \
//     > ../grafana-solr-dashboard.json
//   jsonnet -J vendor -e '(import "mixin.libsonnet").prometheusAlerts' \
//     | gojsontoyaml > ../prometheus-solr-alerts.yml

(import 'config.libsonnet')
+ (import 'dashboards/dashboards.libsonnet')
+ (import 'alerts/alerts.libsonnet')
