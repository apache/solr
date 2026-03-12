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

// alerts.libsonnet — Prometheus alert rules for Solr 10.x.
//
// Seven rules in group "SolrAlerts":
//   Critical (3): SolrHighHeapUsage, SolrJvmGcThrashing, SolrLowDiskSpace
//   Warning  (4): SolrHighSearchLatency, SolrHighErrorRate, SolrOverseerQueueBuildup, SolrHighMmapRatio
//
// Thresholds are defined in config.libsonnet and can be overridden.
// All expressions include "by (instance)" so alerts fire and resolve per-node.

local config = import '../config.libsonnet';
local cfg = config._config;

{
  groups: [
    {
      name: 'SolrAlerts',
      rules: [

        // ---------------------------------------------------------------
        // CRITICAL: SolrHighHeapUsage
        // Fires when a single JVM instance uses > 90% of its max heap for 2 minutes.
        // Uses max by (instance) to deduplicate the dual OTel JVM scopes.
        // ---------------------------------------------------------------
        {
          alert: 'SolrHighHeapUsage',
          expr: |||
            max by (instance) (jvm_memory_used_bytes{jvm_memory_type="heap"})
            /
            max by (instance) (jvm_memory_limit_bytes{jvm_memory_type="heap"})
            > %(threshold)s
          ||| % {threshold: cfg.heapUsageThreshold},
          'for': '2m',
          labels: {severity: 'critical'},
          annotations: {
            summary: 'Solr instance {{ $labels.instance }} has high JVM heap usage',
            description: |||
              Instance {{ $labels.instance }} is using {{ $value | humanizePercentage }} of its
              maximum JVM heap (threshold: %(threshold)s%%).
              High heap usage increases GC pressure and risks OutOfMemoryError.
              Consider increasing -Xmx or reducing cache sizes in solrconfig.xml.
            ||| % {threshold: std.floor(cfg.heapUsageThreshold * 100)},
          },
        },

        // ---------------------------------------------------------------
        // CRITICAL: SolrJvmGcThrashing
        // Fires when the sum of GC wall-clock time across all collectors exceeds
        // cfg.gcThrashThresholdSecsPerMin seconds per minute for 3 minutes.
        // ---------------------------------------------------------------
        {
          alert: 'SolrJvmGcThrashing',
          expr: |||
            sum by (instance) (rate(jvm_gc_duration_seconds_sum{instance=~".+"}[1m]))
            > %(threshold)s
          ||| % {threshold: cfg.gcThrashThresholdSecsPerMin},
          'for': '3m',
          labels: {severity: 'critical'},
          annotations: {
            summary: 'Solr instance {{ $labels.instance }} is experiencing GC thrashing',
            description: |||
              Instance {{ $labels.instance }} is spending {{ $value | humanizeDuration }} per second
              in garbage collection (threshold: %(threshold)s seconds/minute).
              GC thrashing causes stop-the-world pauses and severely degrades search latency.
              Check heap usage (SolrHighHeapUsage), consider tuning GC or increasing heap.
            ||| % {threshold: cfg.gcThrashThresholdSecsPerMin},
          },
        },

        // ---------------------------------------------------------------
        // CRITICAL: SolrLowDiskSpace
        // Fires when free disk space drops below 15% of total for 5 minutes.
        // Uses min/by(instance) so the alert fires per-node independently.
        // ---------------------------------------------------------------
        {
          alert: 'SolrLowDiskSpace',
          expr: |||
            min by (instance) (solr_disk_space_megabytes{type="free_space"})
            /
            min by (instance) (solr_disk_space_megabytes{type="total_space"})
            < %(threshold)s
          ||| % {threshold: cfg.diskFreeThreshold},
          'for': '5m',
          labels: {severity: 'critical'},
          annotations: {
            summary: 'Solr instance {{ $labels.instance }} is low on disk space',
            description: |||
              Instance {{ $labels.instance }} has only {{ $value | humanizePercentage }} disk space
              free (threshold: %(threshold)s%%).
              Solr will stop accepting index updates when disk space is exhausted.
              Free up space or expand the disk immediately.
            ||| % {threshold: std.floor(cfg.diskFreeThreshold * 100)},
          },
        },

        // ---------------------------------------------------------------
        // WARNING: SolrHighSearchLatency
        // Fires when p99 search latency exceeds 1000ms for /select handlers for 5 minutes.
        // ---------------------------------------------------------------
        {
          alert: 'SolrHighSearchLatency',
          expr: |||
            histogram_quantile(0.99,
              sum by (le, instance) (
                rate(solr_core_requests_times_milliseconds_bucket{handler=~"/select.*",internal="false"}[5m])
              )
            ) > %(threshold)s
          ||| % {threshold: cfg.searchLatencyThresholdMs},
          'for': '5m',
          labels: {severity: 'warning'},
          annotations: {
            summary: 'Solr instance {{ $labels.instance }} has high search latency',
            description: |||
              Instance {{ $labels.instance }} p99 search latency is {{ $value | humanizeDuration }}ms
              (threshold: %(threshold)sms).
              Possible causes: large result sets, expensive faceting, insufficient cache, or GC pauses.
              Check the Search Latency panel in the Grafana dashboard for trends.
            ||| % {threshold: cfg.searchLatencyThresholdMs},
          },
        },

        // ---------------------------------------------------------------
        // WARNING: SolrHighErrorRate
        // Fires when error requests exceed 1% of total requests for 5 minutes.
        //
        // NOTE: Verify the category label value for errors on your Solr 10.x instance.
        // The regex "(?i)error" is a best-effort match. If solr_node_requests_total
        // does not carry an error category, consider a handler-based proxy metric or
        // check solr_core_requests_total with status=error (if available).
        // ---------------------------------------------------------------
        {
          alert: 'SolrHighErrorRate',
          expr: |||
            sum by (instance) (rate(solr_node_requests_total{category=~"(?i)error"}[5m]))
            /
            sum by (instance) (rate(solr_node_requests_total[5m]))
            > %(threshold)s
          ||| % {threshold: cfg.errorRateThreshold},
          'for': '5m',
          labels: {severity: 'warning'},
          annotations: {
            summary: 'Solr instance {{ $labels.instance }} has a high error rate',
            description: |||
              Instance {{ $labels.instance }} error rate is {{ $value | humanizePercentage }}
              (threshold: %(threshold)s%%).
              Check Solr logs for the root cause. Common causes: invalid queries, missing fields,
              core loading failures, or network connectivity issues.
            ||| % {threshold: std.floor(cfg.errorRateThreshold * 100)},
          },
        },

        // ---------------------------------------------------------------
        // WARNING: SolrOverseerQueueBuildup
        // Fires when the Overseer collection work queue exceeds 50 items for 5 minutes.
        // This metric is only emitted in SolrCloud mode; alert does not fire in standalone.
        // ---------------------------------------------------------------
        {
          alert: 'SolrOverseerQueueBuildup',
          expr: |||
            sum by (instance) (solr_overseer_collection_work_queue_size)
            > %(threshold)s
          ||| % {threshold: cfg.overseerQueueThreshold},
          'for': '5m',
          labels: {severity: 'warning'},
          annotations: {
            summary: 'Solr Overseer collection work queue is building up on {{ $labels.instance }}',
            description: |||
              The Overseer collection work queue has {{ $value }} pending operations on
              instance {{ $labels.instance }} (threshold: %(threshold)s).
              A growing queue indicates the Overseer is falling behind; check for long-running
              collection operations, Overseer leader election issues, or ZooKeeper latency.
            ||| % {threshold: cfg.overseerQueueThreshold},
          },
        },

        // ---------------------------------------------------------------
        // WARNING: SolrHighMmapRatio
        // Fires when index size exceeds 85% of available MMap memory (RAM minus heap).
        // No absent() guard needed: when jvm_system_memory_total_bytes is absent, the
        // expression produces no data and the alert naturally does not fire.
        // This alert requires jvm_system_memory_total_bytes (Solr 10.x physical RAM metric).
        // ---------------------------------------------------------------
        {
          alert: 'SolrHighMmapRatio',
          expr: |||
            sum(solr_core_index_size_megabytes)
            /
            (
              (max(jvm_system_memory_total_bytes) - sum(jvm_memory_limit_bytes{jvm_memory_type="heap"}))
              / 1e6
            )
            * 100 > %(threshold)s
          ||| % {threshold: cfg.mmapRatioThreshold},
          'for': '5m',
          labels: {severity: 'warning'},
          annotations: {
            summary: 'Solr index is using a high fraction of available MMap memory',
            description: |||
              The Solr index is using {{ $value | humanize }}%% of available MMap address space
              (physical RAM minus JVM heap), threshold: %(threshold)s%%.
              When the index exceeds available MMap memory, Lucene falls back to I/O reads,
              significantly degrading search performance. Consider adding RAM, reducing index size,
              or increasing the JVM heap ratio.
            ||| % {threshold: cfg.mmapRatioThreshold},
          },
        },

      ],  // end rules
    },  // end SolrAlerts group
  ],  // end groups
}
