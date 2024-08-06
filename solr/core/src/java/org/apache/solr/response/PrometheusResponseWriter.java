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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.metrics.AggregateMetric;
import org.apache.solr.metrics.prometheus.SolrPrometheusFormatter;
import org.apache.solr.metrics.prometheus.core.SolrPrometheusCoreFormatter;
import org.apache.solr.metrics.prometheus.jetty.SolrPrometheusJettyFormatter;
import org.apache.solr.metrics.prometheus.jvm.SolrPrometheusJvmFormatter;
import org.apache.solr.metrics.prometheus.node.SolrPrometheusNodeFormatter;
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
    var prometheusTextFormatWriter = new PrometheusTextFormatWriter(false);
    for (Map.Entry<String, Object> prometheusRegistry : prometheusRegistries) {
      var prometheusFormatter = (SolrPrometheusFormatter) prometheusRegistry.getValue();
      prometheusTextFormatWriter.write(out, prometheusFormatter.collect());
    }
  }

  /**
   * Provides a representation of the given Dropwizard metric registry as {@link
   * SolrPrometheusCoreFormatter}-s. Only those metrics are converted which match at least one of
   * the given MetricFilter instances.
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
   * @param consumer consumer that accepts produced {@link SolrPrometheusCoreFormatter}-s
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
      Consumer<SolrPrometheusFormatter> consumer) {
    Map<String, Metric> dropwizardMetrics = registry.getMetrics();
    var formatter = getFormatterType(registryName);
    if (formatter == null) {
      return;
    }

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
            formatter.exportDropwizardMetric(dropwizardMetric, metricName);
          } catch (Exception e) {
            // Do not fail entirely for metrics exporting. Log and try to export next metric
            log.warn("Error occurred exporting Dropwizard Metric to Prometheus", e);
          }
        });

    consumer.accept(formatter);
  }

  public static SolrPrometheusFormatter getFormatterType(String registryName) {
    String coreName;
    boolean cloudMode = false;
    String[] parsedRegistry = registryName.split("\\.");

    switch (parsedRegistry[1]) {
      case "core":
        if (parsedRegistry.length == 3) {
          coreName = parsedRegistry[2];
        } else if (parsedRegistry.length == 5) {
          coreName = Arrays.stream(parsedRegistry).skip(1).collect(Collectors.joining("_"));
          cloudMode = true;
        } else {
          coreName = registryName;
        }
        return new SolrPrometheusCoreFormatter(coreName, cloudMode);
      case "jvm":
        return new SolrPrometheusJvmFormatter();
      case "jetty":
        return new SolrPrometheusJettyFormatter();
      case "node":
        return new SolrPrometheusNodeFormatter();
      default:
        return null;
    }
  }
}
