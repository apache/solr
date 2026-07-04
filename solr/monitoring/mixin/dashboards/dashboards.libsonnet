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

// dashboards.libsonnet — Solr 10.x Grafana dashboard definition.
//
// Rows:
//   Cluster Overview (open by default)  — distributed query/update rates, latency, cores, disk
//   JVM              (open by default)  — heap, GC, threads, CPU
//   Solr Core        (collapsed)        — per-core QPS, update rate, latency, commits, optimizes
//   SolrCloud        (collapsed)        — Overseer queues, ZK ops, shard leaders
//   Index Health     (collapsed)        — segments, index size, merge rates, MMap efficiency
//   Solr Caches      (collapsed)        — filter/query/document cache hit rates and evictions

local config = import '../config.libsonnet';
local g = import 'github.com/grafana/grafonnet/gen/grafonnet-latest/main.libsonnet';

local d = g.dashboard;
local p = g.panel;
local q = g.query.prometheus;
local v = g.dashboard.variable;
local cfg = config._config;

// -----------------------------------------------------------------------
// Computed label selectors (uses configurable label names from config.libsonnet)
// -----------------------------------------------------------------------
local envSel = '%s=~"$environment"' % cfg.environmentLabel;
local clusterSel = '%s=~"$cluster"' % cfg.clusterLabel;
local instSel = '%s=~"$instance"' % cfg.instanceLabel;
local colSel = 'collection=~"$collection",shard=~"$shard",replica_type=~"$replica_type"';

// -----------------------------------------------------------------------
// Template variables (T012)
// Ordered: datasource → environment → cluster → instance →
//          collection → shard → replica_type → interval
// -----------------------------------------------------------------------
local datasourceVar =
  v.datasource.new('datasource', 'prometheus')
  + v.datasource.generalOptions.withLabel('Data Source');

local environmentVar =
  v.query.new(
    'environment',
    'label_values(solr_cores_loaded, %s)' % cfg.environmentLabel
  )
  + v.query.withDatasourceFromVariable(datasourceVar)
  + v.query.selectionOptions.withMulti()
  + v.query.selectionOptions.withIncludeAll(value=true, customAllValue='.*')
  + v.query.refresh.onTime()
  + v.query.generalOptions.withLabel('Environment');

local clusterVar =
  v.query.new(
    'cluster',
    'label_values(solr_cores_loaded{%s}, %s)' % [envSel, cfg.clusterLabel]
  )
  + v.query.withDatasourceFromVariable(datasourceVar)
  + v.query.selectionOptions.withMulti()
  + v.query.selectionOptions.withIncludeAll(value=true, customAllValue='.*')
  + v.query.refresh.onTime()
  + v.query.generalOptions.withLabel('Cluster');

local instanceVar =
  v.query.new(
    'instance',
    'label_values(solr_cores_loaded{%s,%s}, %s)' % [envSel, clusterSel, cfg.instanceLabel]
  )
  + v.query.withDatasourceFromVariable(datasourceVar)
  + v.query.selectionOptions.withMulti()
  + v.query.selectionOptions.withIncludeAll(value=true, customAllValue='.*')
  + v.query.refresh.onTime()
  + v.query.generalOptions.withLabel('Instance');

local collectionVar =
  v.query.new(
    'collection',
    'label_values(solr_core_requests_total{%s}, collection)' % instSel
  )
  + v.query.withDatasourceFromVariable(datasourceVar)
  + v.query.selectionOptions.withMulti()
  + v.query.selectionOptions.withIncludeAll(value=true, customAllValue='.*')
  + v.query.refresh.onTime()
  + v.query.generalOptions.withLabel('Collection');

local shardVar =
  v.query.new(
    'shard',
    'label_values(solr_core_requests_total{%s,collection=~"$collection"}, shard)' % instSel
  )
  + v.query.withDatasourceFromVariable(datasourceVar)
  + v.query.selectionOptions.withMulti()
  + v.query.selectionOptions.withIncludeAll(value=true, customAllValue='.*')
  + v.query.refresh.onTime()
  + v.query.generalOptions.withLabel('Shard');

local replicaTypeVar =
  v.query.new(
    'replica_type',
    'label_values(solr_core_requests_total{%s,collection=~"$collection"}, replica_type)' % instSel
  )
  + v.query.withDatasourceFromVariable(datasourceVar)
  + v.query.selectionOptions.withMulti()
  + v.query.selectionOptions.withIncludeAll(value=true, customAllValue='.*')
  + v.query.refresh.onTime()
  + v.query.generalOptions.withLabel('Replica Type');

local intervalVar =
  v.interval.new('interval', ['1m', '5m', '10m', '30m', '1h'])
  + v.interval.generalOptions.withCurrent(cfg.defaultRateInterval)
  + v.interval.generalOptions.withLabel('Interval');

// -----------------------------------------------------------------------
// Panel builder helpers
// -----------------------------------------------------------------------
local ts(title, exprs, unit='short', desc='') =
  p.timeSeries.new(title)
  + p.timeSeries.queryOptions.withTargets(exprs)
  + p.timeSeries.standardOptions.withUnit(unit)
  + p.timeSeries.panelOptions.withDescription(desc)
  + p.timeSeries.options.legend.withDisplayMode('list')
  + p.timeSeries.options.tooltip.withMode('multi');

local statPanel(title, exprs, unit='short', desc='') =
  p.stat.new(title)
  + p.stat.queryOptions.withTargets(exprs)
  + p.stat.standardOptions.withUnit(unit)
  + p.stat.panelOptions.withDescription(desc)
  + p.stat.options.withColorMode('value')
  + p.stat.options.withGraphMode('none')
  + p.stat.options.reduceOptions.withCalcs(['lastNotNull']);

local gaugePanel(title, exprs, unit='percent', desc='', min=0, max=100, steps=[]) =
  p.gauge.new(title)
  + p.gauge.queryOptions.withTargets(exprs)
  + p.gauge.standardOptions.withUnit(unit)
  + p.gauge.standardOptions.withMin(min)
  + p.gauge.standardOptions.withMax(max)
  + p.gauge.panelOptions.withDescription(desc)
  + p.gauge.options.reduceOptions.withCalcs(['lastNotNull'])
  + (if std.length(steps) > 0
     then p.gauge.standardOptions.thresholds.withSteps(steps) + p.gauge.standardOptions.color.withMode('thresholds')
     else {});

local barPanel(title, exprs, unit='short', desc='') =
  p.barChart.new(title)
  + p.barChart.queryOptions.withTargets(exprs)
  + p.barChart.standardOptions.withUnit(unit)
  + p.barChart.panelOptions.withDescription(desc);

local prom(expr, legend='{{instance}}') =
  q.new('$datasource', expr)
  + q.withLegendFormat(legend)
  + q.withInterval('$interval');

local promInstant(expr, legend='{{instance}}') =
  q.new('$datasource', expr)
  + q.withLegendFormat(legend)
  + q.withInstant(true);

local gp(x, y, w, h) = p.timeSeries.gridPos.withX(x) + p.timeSeries.gridPos.withY(y)
                       + p.timeSeries.gridPos.withW(w) + p.timeSeries.gridPos.withH(h);

// -----------------------------------------------------------------------
// Cluster Overview panels (T013) — open by default, y starts at 0
// -----------------------------------------------------------------------
local nodeOverviewPanels = [
  p.row.new('Cluster Overview')
  + p.row.withCollapsed(false)
  + { gridPos: { x: 0, y: 0, w: 24, h: 1 } },

  ts(
    'Distributed QPS',
    [prom(
      'sum by (collection)(rate(solr_core_requests_times_milliseconds_count{%s,%s,%s,category="QUERY",internal="false"}[$interval]))' % [envSel, clusterSel, instSel],
      '{{collection}}'
    )],
    unit='reqps',
    desc='Incoming user query requests per second per collection (internal shard fan-out excluded). Shows cluster-wide QPS distribution across collections. Use the collection dropdown to focus on a specific collection.'
  ) + { gridPos: { x: 0, y: 1, w: 12, h: 8 } },

  ts(
    'Search Latency p50 / p95 / p99',
    [
      prom(
        'histogram_quantile(0.50, sum by (le, collection)(rate(solr_core_requests_times_milliseconds_bucket{%s,%s,%s,handler=~"/select.*",internal="false"}[$interval])))' % [envSel, clusterSel, instSel],
        'p50 {{collection}}'
      ),
      prom(
        'histogram_quantile(0.95, sum by (le, collection)(rate(solr_core_requests_times_milliseconds_bucket{%s,%s,%s,handler=~"/select.*",internal="false"}[$interval])))' % [envSel, clusterSel, instSel],
        'p95 {{collection}}'
      ),
      prom(
        'histogram_quantile(0.99, sum by (le, collection)(rate(solr_core_requests_times_milliseconds_bucket{%s,%s,%s,handler=~"/select.*",internal="false"}[$interval])))' % [envSel, clusterSel, instSel],
        'p99 {{collection}}'
      ),
    ],
    unit='ms',
    desc='Search request latency percentiles per collection for /select handlers (user-facing requests only). Alert fires at p99 > 1000ms for 5 minutes (SolrHighSearchLatency).'
  ) + { gridPos: { x: 12, y: 1, w: 12, h: 8 } },

  ts(
    'Total Update Rate',
    [prom(
      'sum by (collection)(rate(solr_core_requests_times_milliseconds_count{%s,%s,%s,category="UPDATE"}[$interval]))' % [envSel, clusterSel, instSel],
      '{{collection}}'
    )],
    unit='reqps',
    desc='Update request rate per collection across the cluster. Includes all update traffic reaching each collection.'
  ) + { gridPos: { x: 0, y: 9, w: 12, h: 8 } },

  ts(
    'Update Latency p99',
    [prom(
      'histogram_quantile(0.99, sum by (le, collection)(rate(solr_core_requests_times_milliseconds_bucket{%s,%s,%s,handler="/update"}[$interval])))' % [envSel, clusterSel, instSel],
      '{{collection}}'
    )],
    unit='ms',
    desc='p99 latency for /update (indexing) requests per collection. High latency may indicate index merge pressure or I/O bottlenecks.'
  ) + { gridPos: { x: 12, y: 9, w: 12, h: 8 } },

  ts(
    'Document Count',
    [prom(
      'max by (collection)(solr_core_indexsearcher_index_num_docs{%s,%s,%s,collection=~"$collection"})' % [envSel, clusterSel, instSel],
      '{{collection}}'
    )],
    unit='short',
    desc='Searchable document count per collection (max across cores, to avoid double-counting replicas). Tracks index growth over time.'
  ) + { gridPos: { x: 12, y: 17, w: 8, h: 8 } },

  statPanel(
    'Collections',
    [prom(
      'count(count by (collection)(solr_core_is_leader{%s,%s,%s}))' % [envSel, clusterSel, instSel],
      'Collections'
    )],
    unit='short',
    desc='Number of distinct collections currently present in the cluster.'
  ) + { gridPos: { x: 20, y: 17, w: 4, h: 8 } },

  p.timeSeries.new('Shard Leaders')
  + p.timeSeries.queryOptions.withTargets([
    prom(
      'sum by (collection, shard)(solr_core_is_leader{%s,%s,%s} == 1)' % [envSel, clusterSel, instSel],
      '{{collection}} {{shard}}'
    ),
  ])
  + p.timeSeries.standardOptions.withUnit('short')
  + p.timeSeries.panelOptions.withDescription('Number of active shard leaders per shard over time. Each shard contributes 1 when it has a leader; a band dropping to 0 indicates that shard has lost its leader.')
  + p.timeSeries.options.legend.withDisplayMode('list')
  + p.timeSeries.options.tooltip.withMode('multi')
  + p.timeSeries.fieldConfig.defaults.custom.stacking.withMode('normal')
  + p.timeSeries.fieldConfig.defaults.custom.withFillOpacity(100)
  + p.timeSeries.fieldConfig.defaults.custom.withLineWidth(0)
  + p.timeSeries.standardOptions.withMin(0)
  + { gridPos: { x: 0, y: 17, w: 12, h: 8 } },
];

// -----------------------------------------------------------------------
// JVM panels (T014) — open by default, y starts at 25
// ALL panels use max by (instance,...) to deduplicate dual OTel scopes.
// -----------------------------------------------------------------------
local jvmPanels = [
  p.row.new('JVM')
  + p.row.withCollapsed(false)
  + { gridPos: { x: 0, y: 25, w: 24, h: 1 } },

  ts(
    'Heap Used',
    [
      prom(
        'max by (instance, jvm_memory_pool_name)(jvm_memory_used_bytes{%s,%s,%s,jvm_memory_type="heap"})' % [envSel, clusterSel, instSel],
        '{{instance}} {{jvm_memory_pool_name}}'
      ),
      prom(
        'max by (instance)(jvm_memory_limit_bytes{%s,%s,%s,jvm_memory_type="heap"})' % [envSel, clusterSel, instSel],
        '{{instance}} heap max'
      ),
    ],
    unit='bytes',
    desc='JVM heap memory currently in use per instance and memory pool, with -Xmx (heap max) as reference line. Uses max() to avoid double-counting the two OTel JVM instrumentation scopes (java8 + java17) emitted by Solr.'
  ) + { gridPos: { x: 0, y: 26, w: 8, h: 8 } },

  ts(
    'Heap Committed',
    [
      prom(
        'max by (instance, jvm_memory_pool_name)(jvm_memory_committed_bytes{%s,%s,%s,jvm_memory_type="heap"})' % [envSel, clusterSel, instSel],
        '{{instance}} {{jvm_memory_pool_name}}'
      ),
      prom(
        'max by (instance)(jvm_memory_limit_bytes{%s,%s,%s,jvm_memory_type="heap"})' % [envSel, clusterSel, instSel],
        '{{instance}} heap max'
      ),
    ],
    unit='bytes',
    desc='JVM heap memory committed (reserved from the OS) per instance and pool, with -Xmx (heap max) as reference line. Uses max() to avoid double-counting.'
  ) + { gridPos: { x: 8, y: 26, w: 8, h: 8 } },

  p.timeSeries.new('System and Heap Memory')
  + p.timeSeries.queryOptions.withTargets([
    prom(
      'max by (instance)(jvm_memory_limit_bytes{%s,%s,%s,jvm_memory_type="heap"})' % [envSel, clusterSel, instSel],
      '{{instance}} heap max'
    ),
    prom(
      'max by (instance)(jvm_system_memory_bytes{%s,%s,%s,state="total"})' % [envSel, clusterSel, instSel],
      '{{instance}} system total'
    ),
    prom(
      'max by (instance)(jvm_system_memory_bytes{%s,%s,%s,state="total"}) - max by (instance)(jvm_system_memory_bytes{%s,%s,%s,state="free"})' % [envSel, clusterSel, instSel, envSel, clusterSel, instSel],
      '{{instance}} system used'
    ),
  ])
  + p.timeSeries.standardOptions.withUnit('bytes')
  + p.timeSeries.panelOptions.withDescription('Physical system memory (total and used) alongside JVM heap max (-Xmx) per instance. Shows how much of physical RAM is consumed and how the heap ceiling compares to total RAM.')
  + p.timeSeries.options.legend.withDisplayMode('list')
  + p.timeSeries.options.tooltip.withMode('multi')
  + p.timeSeries.fieldConfig.defaults.custom.withFillOpacity(10)
  + { gridPos: { x: 16, y: 26, w: 8, h: 8 } },

  ts(
    'GC Pause p99',
    [prom(
      'histogram_quantile(0.99, sum by (le, jvm_gc_name, instance)(rate(jvm_gc_duration_seconds_bucket{%s,%s,%s}[$interval])))' % [envSel, clusterSel, instSel],
      '{{instance}} {{jvm_gc_name}}'
    )],
    unit='s',
    desc='p99 GC pause duration per collector and instance. Alert fires when total GC time > 10s/min for 3 minutes (SolrJvmGcThrashing).'
  ) + { gridPos: { x: 0, y: 34, w: 8, h: 8 } },

  ts(
    'GC Collection Rate',
    [prom(
      'sum by (jvm_gc_name, instance)(rate(jvm_gc_duration_seconds_count{%s,%s,%s}[$interval]))' % [envSel, clusterSel, instSel],
      '{{instance}} {{jvm_gc_name}}'
    )],
    unit='cps',
    desc='GC collection frequency per collector and instance. Frequent major GC indicates memory pressure.'
  ) + { gridPos: { x: 8, y: 34, w: 8, h: 8 } },

  ts(
    'JVM Threads',
    [prom(
      'sum by (jvm_thread_state, instance)(jvm_thread_count{%s,%s,%s})' % [envSel, clusterSel, instSel],
      '{{instance}} {{jvm_thread_state}}'
    )],
    unit='short',
    desc='JVM thread count by state per instance. Large BLOCKED or WAITING counts indicate lock contention or stalled I/O.'
  ) + { gridPos: { x: 16, y: 34, w: 8, h: 8 } },

  ts(
    'JVM CPU Utilization',
    [prom(
      'max by (instance)(jvm_cpu_recent_utilization_ratio{%s,%s,%s})' % [envSel, clusterSel, instSel],
      '{{instance}}'
    )],
    unit='percentunit',
    desc='Recent JVM CPU utilization (0–1) per instance. Sustained values near 1.0 indicate CPU saturation; combine with GC rate for root-cause analysis.'
  ) + { gridPos: { x: 0, y: 42, w: 24, h: 8 } },
];

// -----------------------------------------------------------------------
// Solr Core panels — collapsed by default, y=50
// All panels show per-core breakdown; respond to all dropdowns including colSel.
// -----------------------------------------------------------------------
local solrCoreRow =
  p.row.new('Solr Core')
  + p.row.withCollapsed(true)
  + {
    gridPos: { x: 0, y: 50, w: 24, h: 1 },
    panels: [
      ts(
        'QPS',
        [prom(
          'sum by (core)(rate(solr_core_requests_times_milliseconds_count{%s,%s,%s,%s,handler=~"/select|/query"}[$interval]))' % [envSel, clusterSel, instSel, colSel],
          '{{core}}'
        )],
        unit='reqps',
        desc='Query requests per second per core on /select and /query handlers (all traffic, including internal shard requests). Use the collection, shard, and replica_type dropdowns to narrow the view in large clusters.'
      ) + { gridPos: { x: 0, y: 47, w: 12, h: 8 } },

      ts(
        'Update Rate',
        [prom(
          'sum by (core)(rate(solr_core_requests_times_milliseconds_count{%s,%s,%s,%s,category="UPDATE"}[$interval]))' % [envSel, clusterSel, instSel, colSel],
          '{{core}}'
        )],
        unit='reqps',
        desc='Indexing (update) request rate per core. Includes all update traffic reaching each core.'
      ) + { gridPos: { x: 12, y: 47, w: 12, h: 8 } },

      ts(
        'Update Latency p99',
        [prom(
          'histogram_quantile(0.99, sum by (le, core)(rate(solr_core_requests_times_milliseconds_bucket{%s,%s,%s,%s,handler="/update"}[$interval])))' % [envSel, clusterSel, instSel, colSel],
          '{{core}}'
        )],
        unit='ms',
        desc='p99 /update latency per core. High per-core latency relative to the distributed latency indicates uneven load or merge pressure on specific cores.'
      ) + { gridPos: { x: 0, y: 55, w: 12, h: 8 } },

      ts(
        'Commit Rate',
        [prom(
          'sum by (core)(rate(solr_core_update_handler_commits_total{%s,%s,%s,%s}[$interval]))' % [envSel, clusterSel, instSel, colSel],
          '{{core}}'
        )],
        unit='ops',
        desc='Hard commit rate per core. Frequent hard commits increase I/O load; tune autoCommit intervals in solrconfig.xml if this is elevated.'
      ) + { gridPos: { x: 12, y: 55, w: 12, h: 8 } },

      ts(
        'Optimize Rate',
        [prom(
          'sum by (core)(rate(solr_core_update_handler_optimizes_total{%s,%s,%s,%s}[$interval]))' % [envSel, clusterSel, instSel, colSel],
          '{{core}}'
        )],
        unit='ops',
        desc='Optimize (force-merge to 1 segment) rate per core. Optimizes are expensive and block searches; should be infrequent and scheduled during off-peak hours.'
      ) + { gridPos: { x: 0, y: 63, w: 12, h: 8 } },
    ],
  };

// -----------------------------------------------------------------------
// SolrCloud panels (T019) — collapsed by default, y=51
// -----------------------------------------------------------------------
local solrcloudRow =
  p.row.new('SolrCloud')
  + p.row.withCollapsed(true)
  + {
    gridPos: { x: 0, y: 51, w: 24, h: 1 },
    panels: [
      ts(
        'Overseer Collection Work Queue',
        [prom(
          'solr_overseer_collection_work_queue_size{%s,%s,%s}' % [envSel, clusterSel, instSel],
          '{{instance}}'
        )],
        unit='short',
        desc='Pending operations in the Overseer collection work queue. Alert fires > 50 for 5 minutes (SolrOverseerQueueBuildup). This metric is only emitted in SolrCloud mode.'
      ) + { gridPos: { x: 0, y: 47, w: 12, h: 8 } },

      ts(
        'Overseer State Update Queue',
        [prom(
          'solr_overseer_state_update_queue_size{%s,%s,%s}' % [envSel, clusterSel, instSel],
          '{{instance}}'
        )],
        unit='short',
        desc='Pending cluster state updates in the Overseer queue. High values indicate ZooKeeper write pressure.'
      ) + { gridPos: { x: 12, y: 47, w: 12, h: 8 } },

      ts(
        'Shard Leaders',
        [prom(
          'sum by (instance)(solr_core_is_leader{%s,%s,%s} == 1)' % [envSel, clusterSel, instSel],
          '{{instance}}'
        )],
        unit='short',
        desc='Number of shard leaders per node over time. Uneven distribution indicates imbalanced leader placement; use BALANCELEADERS API to rebalance.'
      ) + { gridPos: { x: 0, y: 55, w: 8, h: 8 } },

      ts(
        'ZooKeeper Ops Rate',
        [prom(
          'rate(solr_zk_ops_total{%s,%s,%s}[$interval])' % [envSel, clusterSel, instSel],
          '{{instance}}'
        )],
        unit='ops',
        desc='Rate of ZooKeeper operations per instance. High rates may indicate excessive ZK polling or state update storms.'
      ) + { gridPos: { x: 8, y: 55, w: 16, h: 8 } },

      ts(
        'Update Log Replay Remaining',
        [prom(
          'sum by (collection)(solr_core_update_log_replay_logs_remaining{%s,%s,%s})' % [envSel, clusterSel, instSel],
          '{{collection}}'
        )],
        unit='short',
        desc='Transaction logs remaining to replay per collection during leader recovery. Non-zero indicates replicas catching up after restart.'
      ) + { gridPos: { x: 0, y: 63, w: 12, h: 8 } },
    ],
  };

// -----------------------------------------------------------------------
// Index Health panels (T021) — collapsed by default, y=52
// -----------------------------------------------------------------------
local indexHealthRow =
  p.row.new('Index Health')
  + p.row.withCollapsed(true)
  + {
    gridPos: { x: 0, y: 52, w: 24, h: 1 },
    panels: [
      barPanel(
        'Segment Count per Collection',
        [promInstant(
          'max by (collection)(solr_core_segments{%s,%s,%s,collection=~"$collection",shard=~"$shard"})' % [envSel, clusterSel, instSel],
          '{{collection}}'
        )],
        unit='short',
        desc='Current maximum Lucene segment count per collection (max across cores). High counts (> 50) degrade search performance; trigger explicit merges or tune mergeFactor.'
      ) + { gridPos: { x: 0, y: 49, w: 12, h: 8 } },

      ts(
        'Total Index Size per Node',
        [prom(
          'sum by (instance)(solr_core_index_size_megabytes{%s,%s,%s})' % [envSel, clusterSel, instSel],
          '{{instance}}'
        )],
        unit='decmbytes',
        desc='Total Lucene index size in megabytes per node (all cores combined). Helps identify nodes carrying disproportionate index data.'
      ) + { gridPos: { x: 12, y: 49, w: 12, h: 8 } },

      gaugePanel(
        'MMap Efficiency',
        [prom(
          'clamp_max((max(jvm_system_memory_bytes{%s,%s,%s,state="total"}) - sum(jvm_memory_limit_bytes{%s,%s,%s,jvm_memory_type="heap"})) / 1e6 / sum(solr_core_index_size_megabytes{%s,%s,%s}) * 100, 100)' % [envSel, clusterSel, instSel, envSel, clusterSel, instSel, envSel, clusterSel, instSel],
          'MMap %%'
        )],
        unit='percent',
        desc='Percentage of the total index that fits in available MMap address space (physical RAM minus heap). 100%% means the entire index fits in RAM (ideal). Below 50%% (orange) means less than half fits; below 25%% (red) risks frequent page-cache evictions and I/O bottlenecks. Requires jvm_system_memory_bytes (Solr 10.x physical RAM metric). Shows "No data" if absent.',
        min=0,
        max=100,
        steps=[
          { color: 'red', value: null },
          { color: 'orange', value: 25 },
          { color: 'green', value: 50 },
        ]
      ) + { gridPos: { x: 18, y: 57, w: 6, h: 8 } },

      ts(
        'Index Size per Collection',
        [prom(
          'sum by (collection)(solr_core_index_size_megabytes{%s,%s,%s,collection=~"$collection"} * on(core, instance) group_left() (solr_core_is_leader{%s,%s,%s} == 1))' % [envSel, clusterSel, instSel, envSel, clusterSel, instSel],
          '{{collection}}'
        )],
        unit='decmbytes',
        desc='Combined index size of shard leader cores per collection (replicas excluded). Represents the logical data size of each collection without counting replica duplication.'
      ) + { gridPos: { x: 0, y: 57, w: 12, h: 8 } },

      gaugePanel(
        'Disk Free',
        [prom(
          'min(solr_disk_space_megabytes{%s,%s,%s,type="usable_space"}) / min(solr_disk_space_megabytes{%s,%s,%s,type="total_space"}) * 100' % [envSel, clusterSel, instSel, envSel, clusterSel, instSel],
          'Free %%'
        )],
        unit='percent',
        desc='Percentage of disk space free on the Solr data directory mount (min across instances). Alert fires below 15%% for 5 minutes (SolrLowDiskSpace).',
        min=0,
        max=100,
        steps=[
          { color: 'red', value: null },
          { color: 'yellow', value: 15 },
          { color: 'green', value: 30 },
        ]
      ) + { gridPos: { x: 12, y: 57, w: 6, h: 8 } },

      ts(
        'Flush Rate',
        [prom(
          'rate(solr_core_indexwriter_flushes_total{%s,%s,%s}[$interval])' % [envSel, clusterSel, instSel],
          '{{instance}}'
        )],
        unit='ops',
        desc='Rate of IndexWriter segment flushes per second. High flush rates indicate heavy write load or small RAM buffer (solr.autoSoftCommitMaxDocs).'
      ) + { gridPos: { x: 0, y: 65, w: 8, h: 8 } },

      ts(
        'Minor Merge Rate',
        [prom(
          'rate(solr_core_indexwriter_merges_total{%s,%s,%s,merge_type="minor",merge_state="completed"}[$interval])' % [envSel, clusterSel, instSel],
          '{{instance}}'
        )],
        unit='ops',
        desc='Rate of completed minor (small segment) Lucene merge operations per second.'
      ) + { gridPos: { x: 8, y: 65, w: 8, h: 8 } },

      ts(
        'Major Merge Rate',
        [prom(
          'rate(solr_core_indexwriter_merges_total{%s,%s,%s,merge_type="major",merge_state="completed"}[$interval])' % [envSel, clusterSel, instSel],
          '{{instance}}'
        )],
        unit='ops',
        desc='Rate of completed major (large segment) Lucene merge operations per second. Sustained major merges impact query latency; consider scheduling merges during off-peak hours.'
      ) + { gridPos: { x: 16, y: 65, w: 8, h: 8 } },

      ts(
        'Pending Commit Docs',
        [prom(
          'sum by (collection)(solr_core_update_docs_pending_commit{%s,%s,%s,collection=~"$collection"})' % [envSel, clusterSel, instSel],
          '{{collection}}'
        )],
        unit='short',
        desc='Documents added but not yet committed and visible to searchers per collection. High values indicate delayed hard commits.'
      ) + { gridPos: { x: 12, y: 73, w: 12, h: 8 } },
    ],
  };

// -----------------------------------------------------------------------
// Solr Caches panels (T025) — collapsed by default, y=53
// -----------------------------------------------------------------------
local cacheRow =
  p.row.new('Solr Caches')
  + p.row.withCollapsed(true)
  + {
    gridPos: { x: 0, y: 53, w: 24, h: 1 },
    panels: [
      ts(
        'Filter Cache Hit Rate',
        [prom(
          'rate(solr_core_indexsearcher_cache_lookups_total{%s,%s,%s,collection=~"$collection",name="filterCache",result="hit"}[$interval]) / rate(solr_core_indexsearcher_cache_lookups_total{%s,%s,%s,collection=~"$collection",name="filterCache"}[$interval]) * 100' % [envSel, clusterSel, instSel, envSel, clusterSel, instSel],
          '{{instance}}'
        )],
        unit='percent',
        desc='Filter (DocSet) cache hit rate. Values below 80%% suggest the cache is too small or queries are too varied for effective caching; increase filterCacheSize in solrconfig.xml.'
      ) + { gridPos: { x: 0, y: 49, w: 8, h: 8 } },

      ts(
        'Query Result Cache Hit Rate',
        [prom(
          'rate(solr_core_indexsearcher_cache_lookups_total{%s,%s,%s,collection=~"$collection",name="queryResultCache",result="hit"}[$interval]) / rate(solr_core_indexsearcher_cache_lookups_total{%s,%s,%s,collection=~"$collection",name="queryResultCache"}[$interval]) * 100' % [envSel, clusterSel, instSel, envSel, clusterSel, instSel],
          '{{instance}}'
        )],
        unit='percent',
        desc='Query result cache hit rate. Caches complete result sets; high hit rates reduce CPU for repeated queries.'
      ) + { gridPos: { x: 8, y: 49, w: 8, h: 8 } },

      ts(
        'Document Cache Hit Rate',
        [prom(
          'rate(solr_core_indexsearcher_cache_lookups_total{%s,%s,%s,collection=~"$collection",name="documentCache",result="hit"}[$interval]) / rate(solr_core_indexsearcher_cache_lookups_total{%s,%s,%s,collection=~"$collection",name="documentCache"}[$interval]) * 100' % [envSel, clusterSel, instSel, envSel, clusterSel, instSel],
          '{{instance}}'
        )],
        unit='percent',
        desc='Document (stored field) cache hit rate. Relevant when stored fields are frequently fetched during results hydration.'
      ) + { gridPos: { x: 16, y: 49, w: 8, h: 8 } },

      ts(
        'Filter Cache Evictions',
        [prom(
          'rate(solr_core_indexsearcher_cache_ops_total{%s,%s,%s,name="filterCache",ops="evictions"}[$interval])' % [envSel, clusterSel, instSel],
          '{{instance}}'
        )],
        unit='ops',
        desc='Rate of filter cache evictions per second. Sustained evictions indicate the cache working set exceeds the configured max size.'
      ) + { gridPos: { x: 0, y: 57, w: 8, h: 8 } },

      ts(
        'Query Result Cache Evictions',
        [prom(
          'rate(solr_core_indexsearcher_cache_ops_total{%s,%s,%s,name="queryResultCache",ops="evictions"}[$interval])' % [envSel, clusterSel, instSel],
          '{{instance}}'
        )],
        unit='ops',
        desc='Rate of query result cache evictions per second. High eviction rates with low hit rates indicate insufficient cache size.'
      ) + { gridPos: { x: 8, y: 57, w: 8, h: 8 } },

      ts(
        'Document Cache Evictions',
        [prom(
          'rate(solr_core_indexsearcher_cache_ops_total{%s,%s,%s,name="documentCache",ops="evictions"}[$interval])' % [envSel, clusterSel, instSel],
          '{{instance}}'
        )],
        unit='ops',
        desc='Rate of document cache evictions per second.'
      ) + { gridPos: { x: 16, y: 57, w: 8, h: 8 } },

      ts(
        'Cache RAM Used',
        [prom(
          'sum by (name)(solr_core_indexsearcher_cache_ram_used_bytes{%s,%s,%s,collection=~"$collection"})' % [envSel, clusterSel, instSel],
          '{{name}}'
        )],
        unit='bytes',
        desc='RAM consumed over time by each Solr cache type (filterCache, queryResultCache, documentCache) across all selected cores.'
      ) + { gridPos: { x: 0, y: 65, w: 24, h: 8 } },
    ],
  };

// -----------------------------------------------------------------------
// Dashboard assembly
// -----------------------------------------------------------------------
local licenseComment =
  'Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. '
  + 'See the NOTICE file distributed with this work for additional information regarding copyright ownership. '
  + 'The ASF licenses this file to You under the Apache License, Version 2.0 (the "License"); '
  + 'you may not use this file except in compliance with the License. '
  + 'You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 '
  + '— Generated by mixin/dashboards/dashboards.libsonnet. To regenerate: cd mixin && make dashboards';

local dashboard =
  { __license: licenseComment }
  + d.new('Solr 10.x Overview')
  + d.withUid('solr10-overview')
  + d.withDescription(
    'Solr 10.x monitoring dashboard: node health, JVM performance, SolrCloud operations, index health, and cache efficiency. '
    + 'Use the environment and cluster dropdowns to scope panels to a specific deployment. '
    + 'Both default to All (matches all series) when labels are not configured.'
  )
  + d.withTags(['solr', 'solr10', 'prometheus'])
  + d.withRefresh('auto')
  + d.withTimezone('browser')
  + d.withEditable(true)
  + d.withSchemaVersion(36)
  + d.time.withFrom('now-15m')
  + d.time.withTo('now')
  + d.withVariables([
    datasourceVar,
    environmentVar,
    clusterVar,
    instanceVar,
    collectionVar,
    shardVar,
    replicaTypeVar,
    intervalVar,
  ])
  + d.withPanels(
    nodeOverviewPanels
    + jvmPanels
    + [solrCoreRow]
    + [solrcloudRow]
    + [indexHealthRow]
    + [cacheRow]
  );

// Export: map of output-filename → dashboard JSON
{
  'grafana-solr-dashboard.json': dashboard,
}
