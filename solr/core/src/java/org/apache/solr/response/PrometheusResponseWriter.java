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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.metrics.AggregateMetric;
import org.apache.solr.metrics.prometheus.SolrPrometheusCoreExporter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.stats.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Response writer for Prometheus metrics. This is used only by the {@link
 * org.apache.solr.handler.admin.MetricsHandler}
 */
@SuppressWarnings(value = "unchecked")
public class PrometheusResponseWriter extends RawResponseWriter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void write(OutputStream out, SolrQueryRequest request, SolrQueryResponse response)
      throws IOException {
    NamedList<Object> prometheusRegistries =
        (NamedList<Object>) response.getValues().get("metrics");
    Iterator<Map.Entry<String, Object>> it = prometheusRegistries.iterator();
    var prometheusTextFormatWriter = new PrometheusTextFormatWriter(false);
    while (it.hasNext()) {
      Map.Entry<String, Object> entry = it.next();
      SolrPrometheusCoreExporter prometheusExporter = (SolrPrometheusCoreExporter) entry.getValue();
      prometheusTextFormatWriter.write(out, prometheusExporter.collect());
    }
  }

  /**
   * Provides a representation of the given Dropwizard metric registry as {@link
   * SolrPrometheusCoreExporter}-s. Only those metrics are converted which match at least one of the
   * given MetricFilter instances.
   *
   * @param registry the {@link MetricRegistry} to be converted
   * @param shouldMatchFilters a list of {@link MetricFilter} instances. A metric must match <em>any
   *     one</em> of the filters from this list to be included in the output
   * @param mustMatchFilter a {@link MetricFilter}. A metric <em>must</em> match this filter to be
   *     included in the output.
   * @param propertyFilter limit what properties of a metric are returned
   * @param skipHistograms discard any {@link Histogram}-s and histogram parts of {@link Timer}-s.
   * @param skipAggregateValues discard internal values of {@link AggregateMetric}-s.
   * @param compact use compact representation for counters and gauges.
   * @param consumer consumer that accepts produced {@link SolrPrometheusCoreExporter}-s
   */
  public static void toPrometheus(
      MetricRegistry registry,
      String registryName,
      List<MetricFilter> shouldMatchFilters,
      MetricFilter mustMatchFilter,
      Predicate<CharSequence> propertyFilter,
      boolean skipHistograms,
      boolean skipAggregateValues,
      boolean compact,
      Consumer<SolrPrometheusCoreExporter> consumer) {
    String coreName;
    boolean cloudMode = false;
    Map<String, Metric> dropwizardMetrics = registry.getMetrics();
    String[] rawParsedRegistry = registryName.split("\\.");
    List<String> parsedRegistry = new ArrayList<>(Arrays.asList(rawParsedRegistry));

    if (parsedRegistry.size() == 3) {
      coreName = parsedRegistry.get(2);
    } else if (parsedRegistry.size() == 5) {
      coreName = parsedRegistry.stream().skip(1).collect(Collectors.joining("_"));
      cloudMode = true;
    } else {
      coreName = registryName;
    }

    var solrPrometheusCoreExporter = new SolrPrometheusCoreExporter(coreName, cloudMode);

    MetricUtils.toMaps(
        registry,
        shouldMatchFilters,
        mustMatchFilter,
        propertyFilter,
        skipHistograms,
        skipAggregateValues,
        compact,
        false,
        (metricName, metric) -> {
          try {
            Metric dropwizardMetric = dropwizardMetrics.get(metricName);
            solrPrometheusCoreExporter.exportDropwizardMetric(dropwizardMetric, metricName);
          } catch (Exception e) {
            // Do not fail entirely for metrics exporting. Log and try to export next metric
            log.warn("Error occurred exporting Dropwizard Metric to Prometheus", e);
          }
        });

    consumer.accept(solrPrometheusCoreExporter);
  }
}
