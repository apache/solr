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
package org.apache.solr.response;

import io.opentelemetry.exporter.prometheus.PrometheusMetricReader;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.HistogramSnapshot;
import io.prometheus.metrics.model.snapshots.InfoSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.handler.admin.MetricsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Response writer for Prometheus metrics. This is used only by the {@link MetricsHandler} */
@SuppressWarnings(value = "unchecked")
public class PrometheusResponseWriter implements QueryResponseWriter {
  // not TextQueryResponseWriter because Prometheus libs work with an OutputStream

  private static final String CONTENT_TYPE_PROMETHEUS = "text/plain; version=0.0.4";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void write(
      OutputStream out, SolrQueryRequest request, SolrQueryResponse response, String contentType)
      throws IOException {

    Map<String, PrometheusMetricReader> readers =
        (Map<String, PrometheusMetricReader>) response.getValues().get("metrics");

    List<MetricSnapshot> snapshots =
        readers.values().stream().flatMap(r -> r.collect().stream()).toList();

    new PrometheusTextFormatWriter(false).write(out, mergeSnapshots(snapshots));
  }

  @Override
  public String getContentType(SolrQueryRequest request, SolrQueryResponse response) {
    return CONTENT_TYPE_PROMETHEUS;
  }

  /**
   * Merge a collection of individual {@link MetricSnapshot} instances into one {@link
   * MetricSnapshots}. This is necessary because we create a {@link SdkMeterProvider} per Solr core
   * resulting in duplicate metric names across cores which is an illegal format if not under the
   * same prometheus grouping. Merging metrics of the same name from multiple {@link
   * PrometheusMetricReader}s avoids this illegal exposition format
   */
  private MetricSnapshots mergeSnapshots(List<MetricSnapshot> snapshots) {
    Map<String, CounterSnapshot.Builder> counterSnapshotMap = new HashMap<>();
    Map<String, GaugeSnapshot.Builder> gaugeSnapshotMap = new HashMap<>();
    Map<String, HistogramSnapshot.Builder> histogramSnapshotMap = new HashMap<>();
    InfoSnapshot otelInfoSnapshots = null;

    for (MetricSnapshot snapshot : snapshots) {
      String metricName = snapshot.getMetadata().getPrometheusName();

      switch (snapshot) {
        case CounterSnapshot counterSnapshot -> {
          counterSnapshotMap.computeIfAbsent(
              metricName,
              k -> {
                var base =
                    CounterSnapshot.builder()
                        .name(counterSnapshot.getMetadata().getName())
                        .help(counterSnapshot.getMetadata().getHelp());

                return counterSnapshot.getMetadata().hasUnit()
                    ? base.unit(counterSnapshot.getMetadata().getUnit())
                    : base;
              });
          counterSnapshot.getDataPoints().forEach(counterSnapshotMap.get(metricName)::dataPoint);
        }
        case GaugeSnapshot gaugeSnapshot -> {
          gaugeSnapshotMap.computeIfAbsent(
              metricName,
              k -> {
                var base =
                    GaugeSnapshot.builder()
                        .name(gaugeSnapshot.getMetadata().getName())
                        .help(gaugeSnapshot.getMetadata().getHelp());

                return gaugeSnapshot.getMetadata().hasUnit()
                    ? base.unit(gaugeSnapshot.getMetadata().getUnit())
                    : base;
              });
          gaugeSnapshot.getDataPoints().forEach(gaugeSnapshotMap.get(metricName)::dataPoint);
        }
        case HistogramSnapshot histogramSnapshot -> {
          histogramSnapshotMap.computeIfAbsent(
              metricName,
              k -> {
                var base =
                    HistogramSnapshot.builder()
                        .name(histogramSnapshot.getMetadata().getName())
                        .help(histogramSnapshot.getMetadata().getHelp());

                return histogramSnapshot.getMetadata().hasUnit()
                    ? base.unit(histogramSnapshot.getMetadata().getUnit())
                    : base;
              });
          histogramSnapshot
              .getDataPoints()
              .forEach(histogramSnapshotMap.get(metricName)::dataPoint);
        }
        case InfoSnapshot infoSnapshot -> {
          // InfoSnapshot is a special case in that each SdkMeterProvider will create a duplicate
          // metric called target_info containing OTEL SDK metadata. Only one of these need to be
          // kept
          if (otelInfoSnapshots == null) {
            otelInfoSnapshots =
                new InfoSnapshot(infoSnapshot.getMetadata(), infoSnapshot.getDataPoints());
          }
        }
        default -> {
          log.warn(
              "Unexpected snapshot type: {} for metric {}",
              snapshot.getClass().getName(),
              snapshot.getMetadata().getName());
        }
      }
    }

    MetricSnapshots.Builder snapshotsBuilder = MetricSnapshots.builder();
    counterSnapshotMap.values().forEach(b -> snapshotsBuilder.metricSnapshot(b.build()));
    gaugeSnapshotMap.values().forEach(b -> snapshotsBuilder.metricSnapshot(b.build()));
    histogramSnapshotMap.values().forEach(b -> snapshotsBuilder.metricSnapshot(b.build()));
    if (otelInfoSnapshots != null) snapshotsBuilder.metricSnapshot(otelInfoSnapshots);
    return snapshotsBuilder.build();
  }
}
