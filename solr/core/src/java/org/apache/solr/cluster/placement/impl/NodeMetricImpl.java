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

import java.util.function.Function;
import org.apache.solr.client.solrj.impl.NodeValueFetcher;
import org.apache.solr.cluster.placement.NodeMetric;

/** Node metric identifier, corresponding to a node-level metric name with labels */
public class NodeMetricImpl<T> extends MetricImpl<T> implements NodeMetric<T> {

  /** Total disk space in GB. */
  public static final NodeMetricImpl<Double> TOTAL_DISK_GB =
      new NodeMetricImpl<>("totaldisk", MB_TO_GB_CONVERTER);

  /** Free (usable) disk space in GB. */
  public static final NodeMetricImpl<Double> FREE_DISK_GB =
      new NodeMetricImpl<>("freedisk", MB_TO_GB_CONVERTER);

  /** Number of all cores. */
  public static final NodeMetricImpl<Integer> NUM_CORES =
      new NodeMetricImpl<>(NodeValueFetcher.CORES);

  /** System load average. */
  public static final NodeMetricImpl<Double> SYSLOAD_AVG =
      new NodeMetricImpl<>("sysLoadAvg", "jvm_system_cpu_utilization_ratio");

  /** Number of available processors. */
  public static final NodeMetricImpl<Integer> AVAILABLE_PROCESSORS =
      new NodeMetricImpl<>("availableProcessors", "jvm_cpu_count");

  public NodeMetricImpl(String name, String internalName) {
    this(name, internalName, null);
  }

  public NodeMetricImpl(String name, String internalName, Function<Object, T> converter) {
    super(name, internalName, converter);
  }

  public NodeMetricImpl(String key, Function<Object, T> converter) {
    super(key, key, null, null, converter);
  }

  public NodeMetricImpl(
      String name,
      String internalName,
      String labelKey,
      String labelValue,
      Function<Object, T> converter) {
    super(name, internalName, labelKey, labelValue, converter);
  }

  public NodeMetricImpl(String key) {
    super(key, key);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof NodeMetricImpl<?>)) {
      return false;
    }
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public String toString() {
    return "NodeMetricImpl{key=" + getInternalName() + "," + labelKey + "=" + labelValue + "}";
  }
}
