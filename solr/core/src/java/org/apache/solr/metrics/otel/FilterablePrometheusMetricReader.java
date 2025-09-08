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
import io.prometheus.metrics.model.snapshots.DataPointSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.InfoSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
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
   * @param requiredLabels Map of label names to their allowed values. If empty, no label filtering
   *     is applied.
   * @return Filtered MetricSnapshots
   */
  public MetricSnapshots collect(
      Set<String> includedNames, SortedMap<String, Set<String>> requiredLabels) {

    // If no filtering is requested then return all metrics
    if (includedNames.isEmpty() && requiredLabels.isEmpty()) {
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
    if (requiredLabels.isEmpty()) {
      return snapshotsToFilter;
    }

    MetricSnapshots.Builder filteredSnapshots = MetricSnapshots.builder();
    for (MetricSnapshot metricSnapshot : snapshotsToFilter) {
      switch (metricSnapshot) {
        case CounterSnapshot snapshot -> {
          List<CounterSnapshot.CounterDataPointSnapshot> filtered =
              filterDatapoint(
                  snapshot, requiredLabels, CounterSnapshot.CounterDataPointSnapshot.class);
          if (!filtered.isEmpty()) {
            filteredSnapshots.metricSnapshot(new CounterSnapshot(snapshot.getMetadata(), filtered));
          }
        }
        case HistogramSnapshot snapshot -> {
          List<HistogramSnapshot.HistogramDataPointSnapshot> filtered =
              filterDatapoint(
                  snapshot, requiredLabels, HistogramSnapshot.HistogramDataPointSnapshot.class);
          if (!filtered.isEmpty()) {
            filteredSnapshots.metricSnapshot(
                new HistogramSnapshot(snapshot.getMetadata(), filtered));
          }
        }
        case GaugeSnapshot snapshot -> {
          List<GaugeSnapshot.GaugeDataPointSnapshot> filtered =
              filterDatapoint(snapshot, requiredLabels, GaugeSnapshot.GaugeDataPointSnapshot.class);
          if (!filtered.isEmpty()) {
            filteredSnapshots.metricSnapshot(new GaugeSnapshot(snapshot.getMetadata(), filtered));
          }
        }
        case InfoSnapshot ignored -> {
          // Do nothing for InfoSnapshots. Always filter it out
        }
        default -> {
          log.error("Unknown metric snapshot type {}", metricSnapshot.getClass());
        }
      }
    }
    return filteredSnapshots.build();
  }

  private <D extends DataPointSnapshot> List<D> filterDatapoint(
      MetricSnapshot snapshot,
      SortedMap<String, Set<String>> requiredLabels,
      Class<D> dataPointClass) {
    return snapshot.getDataPoints().stream()
        .filter(dp -> requiredLabelsFilter(dp.getLabels(), requiredLabels))
        .map(dataPointClass::cast)
        .collect(toList());
  }

  static boolean requiredLabelsFilter(
      Labels labels, SortedMap<String, Set<String>> requiredLabels) {
    // Both Labels and requiredLabels are name-sorted with unique names.
    // For each required label, scan forward through labels until we find it.
    int labelIdx = 0;
    requireLoop:
    for (Map.Entry<String, Set<String>> entry : requiredLabels.entrySet()) {
      String requiredLabelName = entry.getKey();
      Set<String> allowedValues = entry.getValue();

      // Labels are sorted, so we can continue from the last position
      while (labelIdx < labels.size()) {
        String labelName = labels.getName(labelIdx);

        int comparison = labelName.compareTo(requiredLabelName);

        if (comparison < 0) {
          // a label before requiredLabelName; we don't care about this one
          assert !requiredLabels.containsKey(labelName);
          labelIdx++;
          continue; // inspect the next input label
        }
        if (comparison > 0) {
          // We've passed where requiredLabelName should be, so it doesn't exist
          assert !labels.contains(requiredLabelName);
          return false;
        }

        assert labels.contains(requiredLabelName);
        if (!allowedValues.contains(labels.getValue(labelIdx))) {
          return false;
        }
        // we satisfied requiredLabelName with its allowedValues.  Move onto next
        labelIdx++;
        continue requireLoop;
      } // end while loop on input Labels
      // didn't find requiredLabelName; ran out of names
      assert !labels.contains(requiredLabelName);
      return false;
    }
    return true; // found all requiredLabels in Labels
  }
}
