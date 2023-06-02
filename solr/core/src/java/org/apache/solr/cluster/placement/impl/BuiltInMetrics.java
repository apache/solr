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

package org.apache.solr.cluster.placement.impl;

import org.apache.solr.client.solrj.impl.NodeValueFetcher;
import org.apache.solr.cluster.placement.NodeMetric;

/** Node and Replica Metrics available to the placement plugins */
public class BuiltInMetrics {

  /*
   * Node Metrics
   */

  /** Total disk space in GB. */
  public static final NodeMetricImpl<Double> NODE_TOTAL_DISK_GB =
      new NodeMetricImpl.DoubleNodeMetricImpl(
          "totalDisk",
          NodeMetric.Registry.SOLR_NODE,
          "CONTAINER.fs.totalSpace",
          MetricImpl.BYTES_TO_GB_CONVERTER);

  /** Free (usable) disk space in GB. */
  public static final NodeMetricImpl<Double> NODE_FREE_DISK_GB =
      new NodeMetricImpl.DoubleNodeMetricImpl(
          "freeDisk",
          NodeMetric.Registry.SOLR_NODE,
          "CONTAINER.fs.usableSpace",
          MetricImpl.BYTES_TO_GB_CONVERTER);

  /** Number of all cores. */
  public static final NodeMetricImpl<Integer> NODE_NUM_CORES =
      new NodeMetricImpl.IntNodeMetricImpl(NodeValueFetcher.CORES);

  public static final NodeMetricImpl<Double> NODE_HEAP_USAGE =
      new NodeMetricImpl.DoubleNodeMetricImpl(NodeValueFetcher.Tags.HEAPUSAGE.tagName);

  /** System load average. */
  public static final NodeMetricImpl<Double> NODE_SYSLOAD_AVG =
      new NodeMetricImpl.DoubleNodeMetricImpl(
          NodeValueFetcher.Tags.SYSLOADAVG.tagName,
          NodeMetric.Registry.SOLR_JVM,
          NodeValueFetcher.Tags.SYSLOADAVG.prefix);

  /** Number of available processors. */
  public static final NodeMetricImpl<Integer> NODE_AVAILABLE_PROCESSORS =
      new NodeMetricImpl.IntNodeMetricImpl(
          "availableProcessors", NodeMetric.Registry.SOLR_JVM, "os.availableProcessors");

  /*
   * Replica Metrics
   */

  /** Replica index size in GB. */
  public static final ReplicaMetricImpl<Double> REPLICA_INDEX_SIZE_GB =
      new ReplicaMetricImpl<>("sizeGB", "INDEX.sizeInBytes", MetricImpl.BYTES_TO_GB_CONVERTER);

  /** 1-min query rate of the /select handler. */
  public static final ReplicaMetricImpl<Double> REPLICA_QUERY_RATE_1MIN =
      new ReplicaMetricImpl<>("queryRate", "QUERY./select.requestTimes:1minRate");
  /** 1-min update rate of the /update handler. */
  public static final ReplicaMetricImpl<Double> REPLICA_UPDATE_RATE_1MIN =
      new ReplicaMetricImpl<>("updateRate", "UPDATE./update.requestTimes:1minRate");
}
