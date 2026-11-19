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

// config.libsonnet — settings you can override without editing the dashboard
// or alert sources.
//
// Every key here is consumed by dashboards/ or alerts/; nothing is declared
// ahead of its use. To override, import the mixin and patch _config:
//
//   (import 'mixin.libsonnet') + { _config+:: { solrSelector: 'job="my-solr"' } }

{
  _config:: {
    // Label selector that identifies Solr targets in your Prometheus. Applied to
    // every dashboard query and alert expression, so JVM metrics scraped from
    // other Java services are not mistaken for Solr's.
    solrSelector: 'job="solr"',

    // Window for rate() queries, and the preselected value of the dashboard's
    // Interval dropdown. Raise it for sparse scrape intervals.
    rateInterval: '5m',

    // Alert thresholds. Each is consumed by exactly one rule in alerts/.

    // Fraction of the committed heap in use that fires SolrHighHeapUsage.
    // bin/solr pins -Xms to -Xmx, so that is the maximum heap; where they
    // differ, the rule measures the heap as currently sized.
    heapUsageThreshold: 0.9,

    // Seconds of garbage collection per minute that fire SolrJvmGcThrashing.
    gcSecondsPerMinuteThreshold: 10,

    // Fraction of the partition still free that fires SolrLowDiskSpace.
    diskFreeThreshold: 0.15,

    // 99th percentile /select latency in milliseconds that fires
    // SolrHighSearchLatency.
    searchLatencyThresholdMs: 1000,

    // Fraction of requests failing that fires SolrHighErrorRate.
    errorRateThreshold: 0.01,

    // Pending Overseer collection operations that fire
    // SolrOverseerQueueBuildup.
    overseerQueueThreshold: 50,

    // Percentage of the index that still fits in memory outside the heap,
    // below which SolrHighMmapRatio fires.
    mmapRatioThreshold: 10,

    // How long a node may send no metrics before SolrNodeDown treats it as
    // gone. Must comfortably exceed the scrape interval, or the OTLP push
    // interval where metrics are pushed through a collector.
    nodeReportingWindow: '5m',

    // How far back SolrNodeDown looks to decide that a node ever existed, and
    // so for how long it keeps firing for a node that never comes back. A node
    // absent for longer than this is taken to be decommissioned.
    nodeReportingLookback: '1h',

    // How long SolrNodeDown must hold before it fires, on top of the detection
    // above. Lower it to be told sooner, raise it to ride out longer restarts.
    nodeDownFor: '5m',
  },
}
