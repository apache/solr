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

package org.apache.solr.prometheus.collector;

import io.prometheus.client.Collector;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MetricSamples {

  private final Map<String, Collector.MetricFamilySamples> samplesByMetricName;

  private final Set<Collector.MetricFamilySamples.Sample> seenSamples;

  public MetricSamples(Map<String, Collector.MetricFamilySamples> input) {
    samplesByMetricName = input;
    seenSamples = new HashSet<>();
    for (Collector.MetricFamilySamples metricFamilySamples : input.values()) {
      seenSamples.addAll(metricFamilySamples.samples);
    }
  }

  public MetricSamples() {
    this(new HashMap<>());
  }

  public void addSamplesIfNotPresent(
      String metricName, Collector.MetricFamilySamples metricFamilySamples) {
    if (samplesByMetricName.putIfAbsent(metricName, metricFamilySamples) == null) {
      seenSamples.addAll(metricFamilySamples.samples);
    }
  }

  public void addSampleIfMetricExists(
      String metricName, Collector.MetricFamilySamples.Sample sample) {
    Collector.MetricFamilySamples sampleFamily = samplesByMetricName.get(metricName);

    if (sampleFamily == null) {
      return;
    }

    if (seenSamples.add(sample)) {
      sampleFamily.samples.add(sample);
    }
  }

  public void addAll(MetricSamples other) {
    for (Map.Entry<String, Collector.MetricFamilySamples> entry :
        other.samplesByMetricName.entrySet()) {
      String key = entry.getKey();
      if (this.samplesByMetricName.containsKey(key)) {
        for (Collector.MetricFamilySamples.Sample sample : entry.getValue().samples) {
          addSampleIfMetricExists(key, sample);
        }
      } else {
        this.samplesByMetricName.put(key, entry.getValue());
        seenSamples.addAll(entry.getValue().samples);
      }
    }
  }

  public List<Collector.MetricFamilySamples> asList() {
    return samplesByMetricName.values().stream()
        .filter(value -> !value.samples.isEmpty())
        .collect(Collectors.toList());
  }
}
