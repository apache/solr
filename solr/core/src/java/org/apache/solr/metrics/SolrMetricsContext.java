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

import io.opentelemetry.api.metrics.BatchCallback;
import io.opentelemetry.api.metrics.DoubleCounter;
import io.opentelemetry.api.metrics.DoubleGauge;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.DoubleUpDownCounter;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongGauge;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.ObservableDoubleCounter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongCounter;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.api.metrics.ObservableMeasurement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.otel.OtelUnit;
import org.apache.solr.util.stats.OtelInstrumentedExecutorService;

/**
 * This class represents a metrics context that ties together components with the same life-cycle
 * and provides convenient access to the metric registry.
 *
 * <p>Additionally it's used for registering and reporting metrics specific to the components that
 * use the same instance of context.
 */
public class SolrMetricsContext {
  private final String registryName;
  private final SolrMetricManager metricManager;
  private final List<AutoCloseable> closeables = new ArrayList<>();

  public SolrMetricsContext(SolrMetricManager metricManager, String registryName) {
    this.registryName = registryName;
    this.metricManager = metricManager;
  }

  /** Return metric registry name used in this context. */
  public String getRegistryName() {
    return registryName;
  }

  /** Return the instance of {@link SolrMetricManager} used in this context. */
  public SolrMetricManager getMetricManager() {
    return metricManager;
  }

  /**
   * Get a context with the same registry name but a tag that represents a parent-child
   * relationship. Since it's a different tag than the parent's context it is assumed that the
   * life-cycle of the parent and child are different.
   *
   * @param child child object that produces metrics with a different life-cycle than the parent.
   */
  public SolrMetricsContext getChildContext(Object child) {
    SolrMetricsContext childContext = new SolrMetricsContext(metricManager, registryName);
    return childContext;
  }

  public LongCounter longCounter(String metricName, String description) {
    return longCounter(metricName, description, null);
  }

  public LongCounter longCounter(String metricName, String description, OtelUnit unit) {
    return metricManager.longCounter(registryName, metricName, description, unit);
  }

  public LongUpDownCounter longUpDownCounter(String metricName, String description) {
    return longUpDownCounter(metricName, description, null);
  }

  public LongUpDownCounter longUpDownCounter(String metricName, String description, OtelUnit unit) {
    return metricManager.longUpDownCounter(registryName, metricName, description, unit);
  }

  public DoubleCounter doubleCounter(String metricName, String description) {
    return doubleCounter(metricName, description, null);
  }

  public DoubleCounter doubleCounter(String metricName, String description, OtelUnit unit) {
    return metricManager.doubleCounter(registryName, metricName, description, unit);
  }

  public DoubleUpDownCounter doubleUpDownCounter(String metricName, String description) {
    return doubleUpDownCounter(metricName, description, null);
  }

  public DoubleUpDownCounter doubleUpDownCounter(
      String metricName, String description, OtelUnit unit) {
    return metricManager.doubleUpDownCounter(registryName, metricName, description, unit);
  }

  public DoubleHistogram doubleHistogram(String metricName, String description) {
    return metricManager.doubleHistogram(registryName, metricName, description, null);
  }

  public DoubleHistogram doubleHistogram(String metricName, String description, OtelUnit unit) {
    return metricManager.doubleHistogram(registryName, metricName, description, unit);
  }

  public LongHistogram longHistogram(String metricName, String description) {
    return metricManager.longHistogram(registryName, metricName, description, null);
  }

  public LongHistogram longHistogram(String metricName, String description, OtelUnit unit) {
    return metricManager.longHistogram(registryName, metricName, description, unit);
  }

  public LongGauge longGauge(String metricName, String description) {
    return metricManager.longGauge(registryName, metricName, description, null);
  }

  public LongGauge longGauge(String metricName, String description, OtelUnit unit) {
    return metricManager.longGauge(registryName, metricName, description, unit);
  }

  public DoubleGauge doubleGauge(String metricName, String description) {
    return metricManager.doubleGauge(registryName, metricName, description, null);
  }

  public DoubleGauge doubleGauge(String metricName, String description, OtelUnit unit) {
    return metricManager.doubleGauge(registryName, metricName, description, unit);
  }

  public ObservableLongGauge observableLongGauge(
      String metricName, String description, Consumer<ObservableLongMeasurement> callback) {
    var observableLongGauge = observableLongGauge(metricName, description, callback, null);
    closeables.add(observableLongGauge);
    return observableLongGauge;
  }

  public ObservableLongGauge observableLongGauge(
      String metricName,
      String description,
      Consumer<ObservableLongMeasurement> callback,
      OtelUnit unit) {
    return metricManager.observableLongGauge(registryName, metricName, description, callback, unit);
  }

  public ObservableDoubleGauge observableDoubleGauge(
      String metricName, String description, Consumer<ObservableDoubleMeasurement> callback) {
    var observableDoubleGauge = observableDoubleGauge(metricName, description, callback, null);
    closeables.add(observableDoubleGauge);
    return observableDoubleGauge;
  }

  public ObservableDoubleGauge observableDoubleGauge(
      String metricName,
      String description,
      Consumer<ObservableDoubleMeasurement> callback,
      OtelUnit unit) {
    return metricManager.observableDoubleGauge(
        registryName, metricName, description, callback, unit);
  }

  public ObservableLongCounter observableLongCounter(
      String metricName, String description, Consumer<ObservableLongMeasurement> callback) {
    var observableLongCounter = observableLongCounter(metricName, description, callback, null);
    closeables.add(observableLongCounter);
    return observableLongCounter;
  }

  public ObservableLongCounter observableLongCounter(
      String metricName,
      String description,
      Consumer<ObservableLongMeasurement> callback,
      OtelUnit unit) {
    return metricManager.observableLongCounter(
        registryName, metricName, description, callback, unit);
  }

  public ObservableDoubleCounter observableDoubleCounter(
      String metricName, String description, Consumer<ObservableDoubleMeasurement> callback) {
    var observableDoubleCounter = observableDoubleCounter(metricName, description, callback, null);
    closeables.add(observableDoubleCounter);
    return observableDoubleCounter;
  }

  public ObservableDoubleCounter observableDoubleCounter(
      String metricName,
      String description,
      Consumer<ObservableDoubleMeasurement> callback,
      OtelUnit unit) {
    return metricManager.observableDoubleCounter(
        registryName, metricName, description, callback, unit);
  }

  public ObservableLongMeasurement longGaugeMeasurement(String metricName, String description) {
    return longGaugeMeasurement(metricName, description, null);
  }

  public ObservableLongMeasurement longGaugeMeasurement(
      String metricName, String description, OtelUnit unit) {
    return metricManager.longGaugeMeasurement(registryName, metricName, description, unit);
  }

  public ObservableDoubleMeasurement doubleGaugeMeasurement(String metricName, String description) {
    return doubleGaugeMeasurement(metricName, description, null);
  }

  public ObservableDoubleMeasurement doubleGaugeMeasurement(
      String metricName, String description, OtelUnit unit) {
    return metricManager.doubleGaugeMeasurement(registryName, metricName, description, unit);
  }

  public ObservableLongMeasurement longCounterMeasurement(String metricName, String description) {
    return longCounterMeasurement(metricName, description, null);
  }

  public ObservableLongMeasurement longCounterMeasurement(
      String metricName, String description, OtelUnit unit) {
    return metricManager.longCounterMeasurement(registryName, metricName, description, unit);
  }

  public ObservableDoubleMeasurement doubleCounterMeasurement(
      String metricName, String description) {
    return doubleCounterMeasurement(metricName, description, null);
  }

  public ObservableDoubleMeasurement doubleCounterMeasurement(
      String metricName, String description, OtelUnit unit) {
    return metricManager.doubleCounterMeasurement(registryName, metricName, description, unit);
  }

  public BatchCallback batchCallback(
      Runnable callback,
      ObservableMeasurement measurement,
      ObservableMeasurement... additionalMeasurements) {
    var batchCallback =
        metricManager.batchCallback(registryName, callback, measurement, additionalMeasurements);
    closeables.add(batchCallback);
    return batchCallback;
  }

  /** Returns an instrumented wrapper over the given executor service. */
  public ExecutorService instrumentedExecutorService(
      ExecutorService delegate,
      String metricNamePrefix,
      String executorName,
      SolrInfoBean.Category category) {
    return new OtelInstrumentedExecutorService(
        delegate, this, metricNamePrefix, executorName, category);
  }

  public void unregister() {
    IOUtils.closeQuietly(closeables);
    closeables.clear();
  }
}
