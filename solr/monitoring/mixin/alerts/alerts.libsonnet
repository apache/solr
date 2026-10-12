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

// alerts.libsonnet — placeholder alert rule.
//
// One rule, enough to show that the mixin build produces a file promtool
// accepts and Prometheus loads. The real rule set replaces this wholesale;
// see SOLR-18147.

local config = import '../config.libsonnet';
local cfg = config._config;

{
  groups: [
    {
      name: 'SolrExampleAlerts',
      rules: [
        {
          alert: 'SolrNodeDown',
          expr: 'up{%s} == 0' % cfg.solrSelector,
          'for': '2m',
          labels: { severity: 'warning' },
          annotations: {
            summary: 'Solr node {{ $labels.instance }} is not responding to scrapes',
            description: |||
              Prometheus has failed to scrape {{ $labels.instance }} for 2 minutes.
              Either the node is down or its metrics endpoint is unreachable.
            |||,
          },
        },
      ],
    },
  ],
}
