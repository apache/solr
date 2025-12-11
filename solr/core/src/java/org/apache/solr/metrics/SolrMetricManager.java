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
package org.apache.solr.metrics;

import static org.apache.solr.metrics.otel.MetricExporterFactory.OTLP_EXPORTER_ENABLED;
import static org.apache.solr.metrics.otel.MetricExporterFactory.OTLP_EXPORTER_INTERVAL;

import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.DoubleCounter;
import io.opentelemetry.api.metrics.DoubleCounterBuilder;
import io.opentelemetry.api.metrics.DoubleGauge;
import io.opentelemetry.api.metrics.DoubleGaugeBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.DoubleHistogramBuilder;
import io.opentelemetry.api.metrics.DoubleUpDownCounter;
import io.opentelemetry.api.metrics.DoubleUpDownCounterBuilder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongCounterBuilder;
import io.opentelemetry.api.metrics.LongGauge;
import io.opentelemetry.api.metrics.LongGaugeBuilder;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.LongHistogramBuilder;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.LongUpDownCounterBuilder;
import io.opentelemetry.api.metrics.ObservableDoubleCounter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableDoubleUpDownCounter;
import io.opentelemetry.api.metrics.ObservableLongCounter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.api.metrics.ObservableLongUpDownCounter;
import io.opentelemetry.api.metrics.ObservableMeasurement;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.ExemplarFilter;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.MetricsConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.metrics.otel.FilterablePrometheusMetricReader;
import org.apache.solr.metrics.otel.MetricExporterFactory;
import org.apache.solr.metrics.otel.OtelUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class maintains a repository of named {@link SdkMeterProvider} instances. It provides
 * utility to create, manage, record and export metrics through OpenTelemetry's APIs as well as
 * managing the lifecycle of these MeterProviders.
 *
 * <p>Solr creates 2 main {@link SdkMeterProvider}'s (solr.jvm, solr.node) as well as an additional
 * provider for every {@link SolrCore} JVM metrics and registry are collected from {@link
 * io.opentelemetry.instrumentation.runtimemetrics.java17.RuntimeMetrics}
 *
 * <p>The SolrMetricManager acts as a bridge between Solr and OpenTelemetry SDK providing the
 * following:
 *
 * <ul>
 *   <li>MeterProvider creation and removal.
 *   <li>Access to metric instruments such as {@link LongCounter}, {@link LongUpDownCounter}, {@link
 *       LongGauge}, {@link LongHistogram} and observable instruments to a specific MeterProvider
 *       instances
 *   <li>{@link FilterablePrometheusMetricReader} for reading and fitlering OpenTelemetry metrics in
 *       Prometheus Format from all MeterProviders
 *   <li>Enablement of optional OTLP exporter
 * </ul>
 *
 * <p>Instances of {@code SolrMetricManager} are intended to be owned per {@link
 * org.apache.solr.core.CoreContainer} and automatically manage JVM-wide MeterProviders.
 */
public class SolrMetricManager {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String OTEL_SCOPE_NAME = "org.apache.solr";

  /** Common prefix for all registry names that Solr uses. */
  public static final String REGISTRY_NAME_PREFIX = "solr.";

  /**
   * Registry name for JVM-specific metrics. This name is also subject to overrides controlled by
   * system properties. This registry is shared between instances of {@link SolrMetricManager}.
   */
  public static final String JVM_REGISTRY =
      REGISTRY_NAME_PREFIX + SolrInfoBean.Group.jvm.toString();

  public static final String NODE_REGISTRY =
      REGISTRY_NAME_PREFIX + SolrInfoBean.Group.node.toString();

  private final Lock reportersLock = new ReentrantLock();
  private final Lock swapLock = new ReentrantLock();

  public static final int DEFAULT_CLOUD_REPORTER_PERIOD = 60;

  private final MetricsConfig metricsConfig;

  private final ConcurrentMap<String, MeterProviderAndReaders> meterProviderAndReaders =
      new ConcurrentHashMap<>();

  private final MetricExporter metricExporter;
  private OtelRuntimeJvmMetrics otelRuntimeJvmMetrics;

  private static final List<Double> SOLR_NANOSECOND_HISTOGRAM_BOUNDARIES =
      List.of(
          0.0,
          5_000.0,
          10_000.0,
          25_000.0,
          50_000.0,
          100_000.0,
          250_000.0,
          500_000.0,
          1_000_000.0,
          2_500_000.0,
          5_000_000.0,
          25_000_000.0,
          100_000_000.0,
          1_000_000_000.0);

  public SolrMetricManager(MetricExporter exporter) {
    metricsConfig = new MetricsConfig.MetricsConfigBuilder().build();
    metricExporter = exporter;
  }

  public SolrMetricManager(SolrResourceLoader loader, MetricsConfig metricsConfig) {
    this.metricsConfig = metricsConfig;
    this.metricExporter = loadMetricExporter(loader);
    this.otelRuntimeJvmMetrics = new OtelRuntimeJvmMetrics().initialize(this, JVM_REGISTRY);
  }

  public LongCounter longCounter(
      String registry, String counterName, String description, OtelUnit unit) {
    LongCounterBuilder builder =
        meterProvider(registry)
            .get(OTEL_SCOPE_NAME)
            .counterBuilder(counterName)
            .setDescription(description);
    if (unit != null) builder.setUnit(unit.getSymbol());

    return builder.build();
  }

  public LongUpDownCounter longUpDownCounter(
      String registry, String counterName, String description, OtelUnit unit) {
    LongUpDownCounterBuilder builder =
        meterProvider(registry)
            .get(OTEL_SCOPE_NAME)
            .upDownCounterBuilder(counterName)
            .setDescription(description);
    if (unit != null) builder.setUnit(unit.getSymbol());

    return builder.build();
  }

  public DoubleUpDownCounter doubleUpDownCounter(
      String registry, String counterName, String description, OtelUnit unit) {
    DoubleUpDownCounterBuilder builder =
        meterProvider(registry)
            .get(OTEL_SCOPE_NAME)
            .upDownCounterBuilder(counterName)
            .setDescription(description)
            .ofDoubles();
    if (unit != null) builder.setUnit(unit.getSymbol());

    return builder.build();
  }

  public DoubleCounter doubleCounter(
      String registry, String counterName, String description, OtelUnit unit) {
    DoubleCounterBuilder builder =
        meterProvider(registry)
            .get(OTEL_SCOPE_NAME)
            .counterBuilder(counterName)
            .setDescription(description)
            .ofDoubles();
    if (unit != null) builder.setUnit(unit.getSymbol());

    return builder.build();
  }

  public DoubleHistogram doubleHistogram(
      String registry, String histogramName, String description, OtelUnit unit) {
    DoubleHistogramBuilder builder =
        meterProvider(registry)
            .get(OTEL_SCOPE_NAME)
            .histogramBuilder(histogramName)
            .setDescription(description);
    if (unit != null) builder.setUnit(unit.getSymbol());

    return builder.build();
  }

  public LongHistogram longHistogram(
      String registry, String histogramName, String description, OtelUnit unit) {
    LongHistogramBuilder builder =
        meterProvider(registry)
            .get(OTEL_SCOPE_NAME)
            .histogramBuilder(histogramName)
            .setDescription(description)
            .ofLongs();

    if (unit != null) builder.setUnit(unit.getSymbol());

    return builder.build();
  }

  public DoubleGauge doubleGauge(
      String registry, String gaugeName, String description, OtelUnit unit) {
    DoubleGaugeBuilder builder =
        meterProvider(registry)
            .get(OTEL_SCOPE_NAME)
            .gaugeBuilder(gaugeName)
            .setDescription(description);
    if (unit != null) builder.setUnit(unit.getSymbol());

    return builder.build();
  }

  public LongGauge longGauge(String registry, String gaugeName, String description, OtelUnit unit) {
    return longGaugeBuilder(registry, gaugeName, description, unit).build();
  }

  public ObservableLongCounter observableLongCounter(
      String registry,
      String counterName,
      String description,
      Consumer<ObservableLongMeasurement> callback,
      OtelUnit unit) {
    LongCounterBuilder builder =
        meterProvider(registry)
            .get(OTEL_SCOPE_NAME)
            .counterBuilder(counterName)
            .setDescription(description);
    if (unit != null) builder.setUnit(unit.getSymbol());

    return builder.buildWithCallback(callback);
  }

  public ObservableDoubleCounter observableDoubleCounter(
      String registry,
      String counterName,
      String description,
      Consumer<ObservableDoubleMeasurement> callback,
      OtelUnit unit) {
    DoubleCounterBuilder builder =
        meterProvider(registry)
            .get(OTEL_SCOPE_NAME)
            .counterBuilder(counterName)
            .setDescription(description)
            .ofDoubles();
    if (unit != null) builder.setUnit(unit.getSymbol());

    return builder.buildWithCallback(callback);
  }

  public ObservableLongGauge observableLongGauge(
      String registry,
      String gaugeName,
      String description,
      Consumer<ObservableLongMeasurement> callback,
      OtelUnit unit) {
    return longGaugeBuilder(registry, gaugeName, description, unit).buildWithCallback(callback);
  }

  public ObservableDoubleGauge observableDoubleGauge(
      String registry,
      String gaugeName,
      String description,
      Consumer<ObservableDoubleMeasurement> callback,
      OtelUnit unit) {
    DoubleGaugeBuilder builder =
        meterProvider(registry)
            .get(OTEL_SCOPE_NAME)
            .gaugeBuilder(gaugeName)
            .setDescription(description);

    if (unit != null) builder.setUnit(unit.getSymbol());

    return builder.buildWithCallback(callback);
  }

  public ObservableLongUpDownCounter observableLongUpDownCounter(
      String registry,
      String counterName,
      String description,
      Consumer<ObservableLongMeasurement> callback,
      String unit) {
    LongUpDownCounterBuilder builder =
        meterProvider(registry)
            .get(OTEL_SCOPE_NAME)
            .upDownCounterBuilder(counterName)
            .setDescription(description);
    if (unit != null) builder.setUnit(unit);

    return builder.buildWithCallback(callback);
  }

  public ObservableDoubleUpDownCounter observableDoubleUpDownCounter(
      String registry,
      String counterName,
      String description,
      Consumer<ObservableDoubleMeasurement> callback,
      String unit) {
    DoubleUpDownCounterBuilder builder =
        meterProvider(registry)
            .get(OTEL_SCOPE_NAME)
            .upDownCounterBuilder(counterName)
            .setDescription(description)
            .ofDoubles();
    if (unit != null) builder.setUnit(unit);

    return builder.buildWithCallback(callback);
  }

  BatchCallback batchCallback(
      String registry,
      Runnable callback,
      ObservableMeasurement measurement,
      ObservableMeasurement... additionalMeasurements) {
    return meterProvider(registry)
        .get(OTEL_SCOPE_NAME)
        .batchCallback(callback, measurement, additionalMeasurements);
  }

  ObservableLongMeasurement longGaugeMeasurement(
      String registry, String gaugeName, String description, OtelUnit unit) {
    return longGaugeBuilder(registry, gaugeName, description, unit).buildObserver();
  }

  ObservableDoubleMeasurement doubleGaugeMeasurement(
      String registry, String gaugeName, String description, OtelUnit unit) {
    return doubleGaugeBuilder(registry, gaugeName, description, unit).buildObserver();
  }

  ObservableLongMeasurement longCounterMeasurement(
      String registry, String counterName, String description, OtelUnit unit) {
    return longCounterBuilder(registry, counterName, description, unit).buildObserver();
  }

  ObservableDoubleMeasurement doubleCounterMeasurement(
      String registry, String counterName, String description, OtelUnit unit) {
    return doubleCounterBuilder(registry, counterName, description, unit).buildObserver();
  }

  private LongGaugeBuilder longGaugeBuilder(
      String registry, String gaugeName, String description, OtelUnit unit) {
    LongGaugeBuilder builder =
        meterProvider(registry)
            .get(OTEL_SCOPE_NAME)
            .gaugeBuilder(gaugeName)
            .setDescription(description)
            .ofLongs();
    if (unit != null) builder.setUnit(unit.getSymbol());

    return builder;
  }

  private DoubleGaugeBuilder doubleGaugeBuilder(
      String registry, String gaugeName, String description, OtelUnit unit) {
    DoubleGaugeBuilder builder =
        meterProvider(registry)
            .get(OTEL_SCOPE_NAME)
            .gaugeBuilder(gaugeName)
            .setDescription(description);
    if (unit != null) builder.setUnit(unit.getSymbol());

    return builder;
  }

  private LongCounterBuilder longCounterBuilder(
      String registry, String counterName, String description, OtelUnit unit) {
    LongCounterBuilder builder =
        meterProvider(registry)
            .get(OTEL_SCOPE_NAME)
            .counterBuilder(counterName)
            .setDescription(description);
    if (unit != null) builder.setUnit(unit.getSymbol());

    return builder;
  }

  private DoubleCounterBuilder doubleCounterBuilder(
      String registry, String counterName, String description, OtelUnit unit) {
    DoubleCounterBuilder builder =
        meterProvider(registry)
            .get(OTEL_SCOPE_NAME)
            .counterBuilder(counterName)
            .setDescription(description)
            .ofDoubles();
    if (unit != null) builder.setUnit(unit.getSymbol());

    return builder;
  }

  /**
   * Check whether a registry with a given name already exists.
   *
   * @param name registry name
   * @return true if this name points to a registry that already exists, false otherwise
   */
  public boolean hasRegistry(String name) {
    return meterProviderAndReaders.containsKey(enforcePrefix(name));
  }

  /**
   * Get (or create if not present) a named {@link SdkMeterProvider}.
   *
   * @param providerName name of the meter provider and prometheus metric reader
   * @return existing or newly created meter provider
   */
  public SdkMeterProvider meterProvider(String providerName) {
    providerName = enforcePrefix(providerName);
    return meterProviderAndReaders
        .computeIfAbsent(
            providerName,
            key -> {
              var reader = new FilterablePrometheusMetricReader(true, null);
              var builder =
                  SdkMeterProvider.builder()
                      .registerMetricReader(reader)
                      .setExemplarFilter(ExemplarFilter.traceBased())
                      .registerView(
                          InstrumentSelector.builder()
                              .setType(InstrumentType.HISTOGRAM)
                              .setUnit(OtelUnit.NANOSECONDS.getSymbol())
                              .build(),
                          View.builder()
                              .setAggregation(
                                  Aggregation.explicitBucketHistogram(
                                      SOLR_NANOSECOND_HISTOGRAM_BOUNDARIES))
                              .build()); // TODO: Make histogram bucket boundaries configurable;
              if (metricExporter != null)
                builder.registerMetricReader(
                    PeriodicMetricReader.builder(metricExporter)
                        .setInterval(OTLP_EXPORTER_INTERVAL, TimeUnit.MILLISECONDS)
                        .build());
              return new MeterProviderAndReaders(builder.build(), reader);
            })
        .sdkMeterProvider();
  }

  /** Return a set of existing registry names. */
  public Set<String> registryNames() {
    Set<String> set = new HashSet<>();
    set.addAll(meterProviderAndReaders.keySet());
    return set;
  }

  /**
   * Remove a named registry and close an existing {@link SdkMeterProvider}. Upon closing of
   * provider, all metric readers registered to it are closed.
   *
   * @param registry name of the registry to remove
   */
  public void removeRegistry(String registry) {
    String key = enforcePrefix(registry);
    MeterProviderAndReaders mpr = meterProviderAndReaders.remove(key);
    if (mpr != null) {
      IOUtils.closeQuietly(mpr.sdkMeterProvider());
    }
  }

  /** Close all meter providers and their associated metric readers. */
  public void closeAllRegistries() {
    meterProviderAndReaders
        .values()
        .forEach(
            meterAndReader -> {
              IOUtils.closeQuietly(meterAndReader.sdkMeterProvider);
            });
    meterProviderAndReaders.clear();
    if (otelRuntimeJvmMetrics != null) {
      otelRuntimeJvmMetrics.close();
    }
  }

  /**
   * This method creates a hierarchical name with arbitrary levels of hierarchy
   *
   * @param name the final segment of the name, must not be null or empty.
   * @param path optional path segments, starting from the top level. Empty or null segments will be
   *     skipped.
   * @return fully-qualified name using dotted notation, with all valid hierarchy segments prepended
   *     to the name.
   */
  public static String mkName(String name, String... path) {
    return makeName(
        path == null || path.length == 0 ? Collections.emptyList() : Arrays.asList(path), name);
  }

  public static String makeName(List<String> path, String name) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("name must not be empty");
    }
    if (path == null || path.size() == 0) {
      return name;
    } else {
      StringBuilder sb = new StringBuilder();
      for (String s : path) {
        if (s == null || s.isEmpty()) {
          continue;
        }
        if (sb.length() > 0) {
          sb.append('.');
        }
        sb.append(s);
      }
      if (sb.length() > 0) {
        sb.append('.');
      }
      sb.append(name);
      return sb.toString();
    }
  }

  /**
   * Enforces the leading {@link #REGISTRY_NAME_PREFIX} in a name.
   *
   * @param name input name, possibly without the prefix
   * @return original name if it contained the prefix, or the input name with the prefix prepended.
   */
  public static String enforcePrefix(String name) {
    if (name.startsWith(REGISTRY_NAME_PREFIX)) {
      return name;
    } else {
      return REGISTRY_NAME_PREFIX + name;
    }
  }

  /** Get a shallow copied map of {@link FilterablePrometheusMetricReader}. */
  public Map<String, FilterablePrometheusMetricReader> getPrometheusMetricReaders() {
    return meterProviderAndReaders.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().prometheusMetricReader()));
  }

  public FilterablePrometheusMetricReader getPrometheusMetricReader(String providerName) {
    MeterProviderAndReaders mpr = meterProviderAndReaders.get(enforcePrefix(providerName));
    return (mpr != null) ? mpr.prometheusMetricReader() : null;
  }

  public MetricExporter getMetricExporter() {
    return metricExporter;
  }

  private MetricExporter loadMetricExporter(SolrResourceLoader loader) {
    if (!OTLP_EXPORTER_ENABLED) return null;
    try {
      MetricExporterFactory exporterFactory =
          loader.newInstance(
              "org.apache.solr.opentelemetry.OtlpExporterFactory", MetricExporterFactory.class);
      return exporterFactory.getExporter();
    } catch (SolrException e) {
      log.error(
          "Could not load OTLP exporter. Check that the Open Telemetry module is enabled.", e);
      return null;
    }
  }

  private record MeterProviderAndReaders(
      SdkMeterProvider sdkMeterProvider, FilterablePrometheusMetricReader prometheusMetricReader) {}
}
