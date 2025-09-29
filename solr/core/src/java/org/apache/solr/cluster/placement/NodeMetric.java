/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.cluster.placement;

import org.apache.solr.client.solrj.impl.NodeValueFetcher;
import org.apache.solr.cluster.placement.impl.MetricImpl;
import org.apache.solr.cluster.placement.impl.NodeMetricImpl;

/**
 * Node metric identifier, corresponding to a node-level metric name with optional labels for metric
 */
public interface NodeMetric<T> extends Metric<T> {
  /** Total disk space in GB. */
  NodeMetricImpl<Double> TOTAL_DISK_GB =
      new NodeMetricImpl<>(
          "totalDisk",
          "solr_disk_space_bytes",
          "type",
          "total_space",
          MetricImpl.BYTES_TO_GB_CONVERTER);

  /** Free (usable) disk space in GB. */
  NodeMetricImpl<Double> FREE_DISK_GB =
      new NodeMetricImpl<>(
          "freeDisk",
          "solr_disk_space_bytes",
          "type",
          "usable_space",
          MetricImpl.BYTES_TO_GB_CONVERTER);

  /** Number of all cores. */
  NodeMetricImpl<Integer> NUM_CORES = new NodeMetricImpl<>(NodeValueFetcher.CORES);

  /** System load average. */
  NodeMetricImpl<Double> SYSLOAD_AVG =
      new NodeMetricImpl<>("sysLoadAvg", "jvm_system_cpu_utilization_ratio");

  //  /** Number of available processors. */
  NodeMetricImpl<Integer> AVAILABLE_PROCESSORS =
      new NodeMetricImpl<>("availableProcessors", "jvm_cpu_count");
}
