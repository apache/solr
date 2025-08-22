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
package org.apache.solr.metrics.otel;

import static java.util.stream.Collectors.toList;

import io.opentelemetry.exporter.prometheus.PrometheusMetricReader;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.InfoSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterablePrometheusMetricReader extends PrometheusMetricReader {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Set<String> PROM_SUFFIXES =
      Set.of("_total", "_sum", "_count", "_bucket", "_gcount", "_gsum", "_created", "_info");

  public FilterablePrometheusMetricReader(
      boolean otelScopeEnabled, Predicate<String> allowedResourceAttributesFilter) {
    super(otelScopeEnabled, allowedResourceAttributesFilter);
  }

  /**
   * Collect metrics with filtering support for metric names and labels.
   *
   * @param includedNames Set of metric names to include. If empty, all metric names are included.
   * @param includedLabels Map of label names to their allowed values. If empty, no label filtering
   *     is applied.
   * @return Filtered MetricSnapshots
   */
  public MetricSnapshots collect(
      Set<String> includedNames, Map<String, Set<String>> includedLabels) {

    // If no filtering is requested then return all metrics
    if (includedNames.isEmpty() && includedLabels.isEmpty()) {
      return super.collect();
    }

    // Prometheus appends a suffix to the metrics depending on the metric type. We need to sanitize
    // the suffix off if they filter by Prometheus name instead of OTEL name.
    Set<String> sanitizedNames =
        includedNames.stream()
            .map(
                (name) -> {
                  for (String suffix : PROM_SUFFIXES) {
                    if (name.endsWith(suffix)) {
                      return name.substring(0, name.lastIndexOf(suffix));
                    }
                  }
                  return name;
                })
            .collect(Collectors.toSet());

    MetricSnapshots snapshotsToFilter;
    if (sanitizedNames.isEmpty()) {
      snapshotsToFilter = super.collect();
    } else {
      snapshotsToFilter = super.collect(sanitizedNames::contains);
    }

    // Return named filtered snapshots if not label filters provided
    if (includedLabels.isEmpty()) {
      return snapshotsToFilter;
    }

    MetricSnapshots.Builder filteredSnapshots = MetricSnapshots.builder();
    snapshotsToFilter.forEach(
        metricSnapshot -> {
          switch (metricSnapshot) {
            case CounterSnapshot counter -> {
              List<CounterSnapshot.CounterDataPointSnapshot> filtered =
                  filterCounterDatapoint(counter, includedLabels);
              if (!filtered.isEmpty()) {
                filteredSnapshots.metricSnapshot(
                    new CounterSnapshot(counter.getMetadata(), filtered));
              }
            }
            case HistogramSnapshot histogram -> {
              List<HistogramSnapshot.HistogramDataPointSnapshot> filtered =
                  filterHistogramDatapoint(histogram, includedLabels);
              if (!filtered.isEmpty()) {
                filteredSnapshots.metricSnapshot(
                    new HistogramSnapshot(histogram.getMetadata(), filtered));
              }
            }
            case GaugeSnapshot gauge -> {
              List<GaugeSnapshot.GaugeDataPointSnapshot> filtered =
                  filterGaugeDatapoint(gauge, includedLabels);
              if (!filtered.isEmpty()) {
                filteredSnapshots.metricSnapshot(new GaugeSnapshot(gauge.getMetadata(), filtered));
              }
            }
            case InfoSnapshot ignored -> {
              // Do nothing for InfoSnapshots. Always filter it out
            }
            default -> {
              log.error("Unknown metric snapshot type {}", metricSnapshot.getClass());
            }
          }
        });
    return filteredSnapshots.build();
  }

  private List<CounterSnapshot.CounterDataPointSnapshot> filterCounterDatapoint(
      CounterSnapshot cs, Map<String, Set<String>> includedLabels) {
    return cs.getDataPoints().stream()
        .filter(dp -> allowedLabelsFilter(dp.getLabels(), includedLabels))
        .collect(toList());
  }

  private List<HistogramSnapshot.HistogramDataPointSnapshot> filterHistogramDatapoint(
      HistogramSnapshot hs, Map<String, Set<String>> includedLabels) {
    return hs.getDataPoints().stream()
        .filter(dp -> allowedLabelsFilter(dp.getLabels(), includedLabels))
        .collect(toList());
  }

  private List<GaugeSnapshot.GaugeDataPointSnapshot> filterGaugeDatapoint(
      GaugeSnapshot gs, Map<String, Set<String>> includedLabels) {
    return gs.getDataPoints().stream()
        .filter(dp -> allowedLabelsFilter(dp.getLabels(), includedLabels))
        .collect(toList());
  }

  private boolean allowedLabelsFilter(Labels labels, Map<String, Set<String>> includedLabels) {
    if (includedLabels.isEmpty()) {
      return true;
    }

    return includedLabels.entrySet().stream()
        .allMatch(
            filterEntry -> {
              String requiredLabelName = filterEntry.getKey();
              Set<String> allowedValues = filterEntry.getValue();

              return labels.stream()
                  .anyMatch(
                      label ->
                          requiredLabelName.equals(label.getName())
                              && allowedValues.contains(label.getValue()));
            });
  }
}
