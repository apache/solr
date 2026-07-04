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

{
  // _config holds all tunable parameters for the Solr monitoring mixin.
  // Override any value by creating a local config object that extends this one.
  _config:: {

    // -----------------------------------------------------------------------
    // Label selectors
    // -----------------------------------------------------------------------

    // Prometheus job selector used in alert expressions (set to match your
    // scrape job name for Solr, e.g. job="solr").
    solrSelector: 'job="solr"',

    // Label name used for the "instance" dimension (hostname:port).
    instanceLabel: 'instance',

    // Label name for the deployment environment (e.g. prod, staging, dev).
    // Operators add this via Prometheus relabel_configs or OTel resource
    // attribute mapping (deployment.environment -> environment).
    // Override to e.g. "deployment_environment" if your label names differ.
    environmentLabel: 'environment',

    // Label name for the SolrCloud cluster identifier.
    // Operators add this via relabel_configs or OTel (service.namespace -> cluster).
    // Override to e.g. "service_namespace" if your label names differ.
    clusterLabel: 'cluster',

    // -----------------------------------------------------------------------
    // Alert thresholds
    // -----------------------------------------------------------------------

    // JVM heap usage fraction (0–1) that triggers SolrHighHeapUsage (critical).
    heapUsageThreshold: 0.9,

    // GC pause seconds-per-minute that triggers SolrJvmGcThrashing (critical).
    gcThrashThresholdSecsPerMin: 10,

    // Fraction of total disk space that triggers SolrLowDiskSpace (critical).
    diskFreeThreshold: 0.15,

    // Search latency p99 milliseconds that triggers SolrHighSearchLatency (warning).
    searchLatencyThresholdMs: 1000,

    // Fraction of requests that triggers SolrHighErrorRate (warning).
    errorRateThreshold: 0.01,

    // Overseer collection work queue depth that triggers SolrOverseerQueueBuildup (warning).
    overseerQueueThreshold: 50,

    // MMap efficiency threshold (%): alert fires when less than this percentage of the
    // index fits in available MMap space (physical RAM minus heap). Below 10% means
    // almost none of the index is RAM-resident, causing heavy I/O.
    mmapRatioThreshold: 10,

    // Default rate interval used in rate() expressions and the $interval variable default.
    defaultRateInterval: '1m',
  },
}
