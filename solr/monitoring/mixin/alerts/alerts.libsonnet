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

// alerts.libsonnet — Prometheus alert rules for Solr, in the group "SolrAlerts".
//
// Every expression carries cfg.solrSelector, so that metrics from other Java
// services are never read as Solr's, and aggregates by instance, so that each
// node alerts and recovers on its own. Thresholds live in config.libsonnet.
//
// The rules are built from the configuration of the merged mixin root
// ($._config), not from a direct import, so that callers can override settings
// without editing this tree.

local alertsFor(cfg) = {
  groups: [
    {
      name: 'SolrAlerts',
      rules: [

        // Committed, not jvm_memory_limit_bytes: generational ZGC reports the
        // whole -Xmx as the limit of both its young and old pool, so summing
        // limits doubles the heap and the rule never fires. Committed is always
        // a partition of one heap, whatever the collector, and bin/solr pins
        // -Xms to -Xmx, so committed is the maximum heap on a Solr node. Where
        // the two differ, this reads as "how full is the heap as sized now".
        // The inner max collapses pools reported by more than one OTel scope,
        // which differ by Solr version.
        {
          alert: 'SolrHighHeapUsage',
          expr: |||
            sum by (instance) (
              max by (instance, jvm_memory_pool_name) (
                jvm_memory_used_bytes{%(solrSelector)s,jvm_memory_type="heap"}
              )
            )
            /
            sum by (instance) (
              max by (instance, jvm_memory_pool_name) (
                jvm_memory_committed_bytes{%(solrSelector)s,jvm_memory_type="heap"}
              )
            )
            > %(threshold)g
          ||| % { threshold: cfg.heapUsageThreshold, solrSelector: cfg.solrSelector },
          'for': '2m',
          labels: { severity: 'critical' },
          annotations: {
            summary: 'Solr node {{ $labels.instance }} has high JVM heap usage',
            description: |||
              Node {{ $labels.instance }} is using {{ $value | humanizePercentage }} of its JVM
              heap (threshold: %(threshold)s%%). Raise -Xmx, or reduce cache sizes.
            ||| % { threshold: std.floor(cfg.heapUsageThreshold * 100) },
          },
        },

        // Scaled from per-second to per-minute so the threshold reads in the
        // unit operators think in. Inner max dedupes scopes as above.
        {
          alert: 'SolrJvmGcThrashing',
          expr: |||
            sum by (instance) (
              max by (instance, jvm_gc_name, jvm_gc_action) (
                rate(jvm_gc_duration_seconds_sum{%(solrSelector)s}[%(rateInterval)s])
              )
            ) * 60
            > %(threshold)s
          ||| % {
            threshold: cfg.gcSecondsPerMinuteThreshold,
            solrSelector: cfg.solrSelector,
            rateInterval: cfg.rateInterval,
          },
          'for': '3m',
          labels: { severity: 'critical' },
          annotations: {
            summary: 'Solr node {{ $labels.instance }} is spending much of its time in GC',
            description: |||
              Node {{ $labels.instance }} spends {{ $value | humanize }} seconds per minute in
              garbage collection (threshold: %(threshold)s). Pauses show up as search latency;
              check heap usage first, then heap sizing and GC settings.
            ||| % { threshold: cfg.gcSecondsPerMinuteThreshold },
          },
        },

        // Node-level metric: the partition holding the Solr home directory.
        {
          alert: 'SolrLowDiskSpace',
          expr: |||
            min by (instance) (solr_disk_space_megabytes{%(solrSelector)s,type="usable_space"})
            /
            min by (instance) (solr_disk_space_megabytes{%(solrSelector)s,type="total_space"})
            < %(threshold)g
          ||| % { threshold: cfg.diskFreeThreshold, solrSelector: cfg.solrSelector },
          'for': '5m',
          labels: { severity: 'critical' },
          annotations: {
            summary: 'Solr node {{ $labels.instance }} is low on disk space',
            description: |||
              Node {{ $labels.instance }} has {{ $value | humanizePercentage }} of its disk free
              (threshold: %(threshold)s%%). Indexing fails once the disk fills, and a merge needs
              room for the segments it rewrites.
            ||| % { threshold: std.floor(cfg.diskFreeThreshold * 100) },
          },
        },

        // Covers a node that is down, one that is unreachable, and one with
        // metrics collection switched off — every rule above goes blind.
        //
        // Two branches, because 'up' only describes what Prometheus scrapes.
        // Where Solr pushes over OTLP, the scrape target is the collector, and
        // it stays up while every node behind it dies; what stops is the data.
        // The second branch therefore alerts on a node that reported within
        // the lookback but has sent nothing for the window. It also covers a
        // scraped node that drops out of service discovery, which 'up' cannot
        // report either because the target stops existing.
        //
        // max_over_time, not offset: it keeps a node visible on the left after
        // its series has gone stale, which is exactly the state being detected.
        // The aggregations are what drop __name__, so both branches carry the
        // same label set and 'or' deduplicates them rather than raising two
        // alerts for one node.
        {
          alert: 'SolrNodeDown',
          expr: |||
            max by (job, instance) (up{%(solrSelector)s} == 0)
            or
            (
              count by (job, instance) (
                max_over_time(solr_cores_loaded{%(solrSelector)s}[%(lookback)s])
              )
              unless
              count by (job, instance) (
                max_over_time(solr_cores_loaded{%(solrSelector)s}[%(window)s])
              )
            )
          ||| % {
            solrSelector: cfg.solrSelector,
            lookback: cfg.nodeReportingLookback,
            window: cfg.nodeReportingWindow,
          },
          'for': cfg.nodeDownFor,
          labels: { severity: 'warning' },
          annotations: {
            summary: 'Solr node {{ $labels.instance }} is down or has stopped reporting',
            description: |||
              {{ $labels.instance }} is failing scrapes, or has sent no metrics for
              %(window)s. Either the node is down, its metrics endpoint is unreachable, or
              metrics collection is switched off. Where metrics arrive through an
              OpenTelemetry collector, the label names the collector rather than the node
              unless Solr sends service.instance.id.
            ||| % { window: cfg.nodeReportingWindow },
          },
        },

        // internal="false" drops the shard requests a distributed search fans
        // out, which would otherwise be counted alongside the request that
        // issued them. core!="" drops the node rollup that a handler with
        // aggregateNodeLevelMetricsEnabled emits under this same metric name,
        // which would weight the quantile towards the cores that enable it.
        {
          alert: 'SolrHighSearchLatency',
          expr: |||
            histogram_quantile(0.99,
              sum by (le, instance) (
                rate(
                  solr_core_requests_times_milliseconds_bucket{%(solrSelector)s,handler=~"/select.*",internal="false",core!=""}[%(rateInterval)s]
                )
              )
            )
            > %(threshold)s
          ||| % {
            threshold: cfg.searchLatencyThresholdMs,
            solrSelector: cfg.solrSelector,
            rateInterval: cfg.rateInterval,
          },
          'for': '5m',
          labels: { severity: 'warning' },
          annotations: {
            summary: 'Solr node {{ $labels.instance }} has high search latency',
            description: |||
              Node {{ $labels.instance }} answers 99%% of searches in {{ $value | humanize }}ms
              (threshold: %(threshold)sms). Usual causes are expensive queries or faceting, caches
              too small for the working set, and GC pauses.
            ||| % { threshold: cfg.searchLatencyThresholdMs },
          },
        },

        // Counts client (4xx) and server (5xx) failures alike; add
        // source="server" to the errors selector to ignore malformed requests.
        // core!="" drops the node rollups, as in the latency rule above.
        {
          alert: 'SolrHighErrorRate',
          expr: |||
            sum by (instance) (rate(solr_core_requests_errors_total{%(solrSelector)s,core!=""}[%(rateInterval)s]))
            /
            sum by (instance) (rate(solr_core_requests_total{%(solrSelector)s,core!=""}[%(rateInterval)s]))
            > %(threshold)g
          ||| % {
            threshold: cfg.errorRateThreshold,
            solrSelector: cfg.solrSelector,
            rateInterval: cfg.rateInterval,
          },
          'for': '5m',
          labels: { severity: 'warning' },
          annotations: {
            summary: 'Solr node {{ $labels.instance }} has a high request error rate',
            description: |||
              {{ $value | humanizePercentage }} of requests to {{ $labels.instance }} are failing
              (threshold: %(threshold)s%%). The Solr log carries the cause: invalid queries,
              missing fields, cores that failed to load, or connectivity problems.
            ||| % { threshold: std.floor(cfg.errorRateThreshold * 100) },
          },
        },

        // SolrCloud only; silent on a standalone node.
        {
          alert: 'SolrOverseerQueueBuildup',
          expr: |||
            max by (instance) (solr_overseer_collection_work_queue_size{%(solrSelector)s})
            > %(threshold)s
          ||| % { threshold: cfg.overseerQueueThreshold, solrSelector: cfg.solrSelector },
          'for': '5m',
          labels: { severity: 'warning' },
          annotations: {
            summary: 'Solr Overseer collection work queue is building up on {{ $labels.instance }}',
            description: |||
              {{ $value }} collection operations are queued on {{ $labels.instance }}
              (threshold: %(threshold)s). The Overseer is falling behind: look for long-running
              collection operations, a disrupted election, or slow ZooKeeper.
            ||| % { threshold: cfg.overseerQueueThreshold },
          },
        },

        // How much of the index fits in the memory left for the page cache.
        // Committed heap is what is actually denied to the page cache, and
        // MetricUtils.bytesToMegabytes() divides by 1024^2, so the physical
        // memory is converted the same way before the two are compared.
        // The trailing guard keeps the rule silent when jvm_system_memory_bytes
        // is missing or zero, which would make available memory look negative.
        {
          alert: 'SolrHighMmapRatio',
          expr: |||
            (
              clamp_max(
                (
                  max by (instance) (jvm_system_memory_bytes{%(solrSelector)s,state="total"})
                  - sum by (instance) (
                    max by (instance, jvm_memory_pool_name) (
                      jvm_memory_committed_bytes{%(solrSelector)s,jvm_memory_type="heap"}
                    )
                  )
                )
                / (1024 * 1024)
                / sum by (instance) (solr_core_index_size_megabytes{%(solrSelector)s})
                * 100,
                100
              ) < %(threshold)s
            )
            and on (instance)
              max by (instance) (jvm_system_memory_bytes{%(solrSelector)s,state="total"}) > 0
          ||| % { threshold: cfg.mmapRatioThreshold, solrSelector: cfg.solrSelector },
          'for': '5m',
          labels: { severity: 'warning' },
          annotations: {
            summary: 'Little of the Solr index fits in memory on {{ $labels.instance }}',
            description: |||
              Only {{ $value | humanize }}%% of the index on {{ $labels.instance }} fits in the
              memory left outside the JVM heap (threshold: %(threshold)s%%). The rest is read from
              disk on every search that touches it.
            ||| % { threshold: cfg.mmapRatioThreshold },
          },
        },

      ],
    },
  ],
};

{
  prometheusAlerts+:: alertsFor($._config),
}
