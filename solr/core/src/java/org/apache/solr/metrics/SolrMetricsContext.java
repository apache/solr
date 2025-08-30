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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.solr.metrics.otel.OtelUnit;
import org.apache.solr.util.stats.MetricUtils;

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
  private final String tag;
  private final Set<String> metricNames = ConcurrentHashMap.newKeySet();

  public SolrMetricsContext(SolrMetricManager metricManager, String registryName, String tag) {
    this.registryName = registryName;
    this.metricManager = metricManager;
    this.tag = tag;
  }

  /** See {@link SolrMetricManager#nullNumber()}. */
  // TODO Remove
  public Object nullNumber() {
    return metricManager.nullNumber();
  }

  /** See {@link SolrMetricManager#notANumber()}. */
  // TODO Remove
  public Object notANumber() {
    return metricManager.notANumber();
  }

  /** See {@link SolrMetricManager#nullString()}. */
  // TODO Remove
  public Object nullString() {
    return metricManager.nullString();
  }

  /** See {@link SolrMetricManager#nullObject()}. */
  // TODO Remove
  public Object nullObject() {
    return metricManager.nullObject();
  }

  /** Metrics tag that represents objects with the same life-cycle. */
  // TODO Remove
  public String getTag() {
    return tag;
  }

  /** Return metric registry name used in this context. */
  // TODO Change this to OTEL Scope
  public String getRegistryName() {
    return registryName;
  }

  /** Return the instance of {@link SolrMetricManager} used in this context. */
  public SolrMetricManager getMetricManager() {
    return metricManager;
  }

  /** Return a modifiable set of metric names that this component registers. */
  public Set<String> getMetricNames() {
    return metricNames;
  }

  /**
   * Unregister all {@link Gauge} metrics that use this context's tag.
   *
   * <p><b>NOTE: This method MUST be called at the end of a life-cycle (typically in <code>close()
   * </code>) of components that register gauge metrics with references to the current object's
   * instance. Failure to do so may result in hard-to-debug memory leaks.</b>
   */
  // TODO don't need this
  public void unregister() {
    metricManager.unregisterGauges(registryName, tag);
  }

  /**
   * Get a context with the same registry name but a tag that represents a parent-child
   * relationship. Since it's a different tag than the parent's context it is assumed that the
   * life-cycle of the parent and child are different.
   *
   * @param child child object that produces metrics with a different life-cycle than the parent.
   */
  // TODO We shouldn't need child context anymore
  public SolrMetricsContext getChildContext(Object child) {
    SolrMetricsContext childContext =
        new SolrMetricsContext(
            metricManager, registryName, SolrMetricProducer.getUniqueMetricTag(child, tag));
    return childContext;
  }

  /**
   * Register a metric name that this component reports. This method is called by various metric
   * registration methods in {@link org.apache.solr.metrics.SolrMetricManager} in order to capture
   * what metric names are reported from this component (which in turn is called from {@link
   * SolrMetricProducer#initializeMetrics(SolrMetricsContext,
   * io.opentelemetry.api.common.Attributes, String)}).
   */
  // TODO We can continue to register metric names
  public void registerMetricName(String name) {
    metricNames.add(name);
  }

  /** Return a snapshot of metric values that this component reports. */
  // TODO Don't think this is needed anymore
  public Map<String, Object> getMetricsSnapshot() {
    return MetricUtils.convertMetrics(getMetricRegistry(), metricNames);
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
    return observableLongGauge(metricName, description, callback, null);
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
    return observableDoubleGauge(metricName, description, callback, null);
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
    return observableLongCounter(metricName, description, callback, null);
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
    return observableDoubleCounter(metricName, description, callback, null);
  }

  public ObservableDoubleCounter observableDoubleCounter(
      String metricName,
      String description,
      Consumer<ObservableDoubleMeasurement> callback,
      OtelUnit unit) {
    return metricManager.observableDoubleCounter(
        registryName, metricName, description, callback, unit);
  }

  public ObservableLongMeasurement longMeasurement(String metricName, String description) {
    return longMeasurement(metricName, description, null);
  }

  public ObservableLongMeasurement longMeasurement(
      String metricName, String description, OtelUnit unit) {
    return metricManager.longMeasurement(registryName, metricName, description, unit);
  }

  public BatchCallback batchCallback(
      Runnable callback,
      ObservableMeasurement measurement,
      ObservableMeasurement... additionalMeasurements) {
    return metricManager.batchCallback(registryName, callback, measurement, additionalMeasurements);
  }

  /**
   * Convenience method for {@link SolrMetricManager#meter(SolrMetricsContext, String, String,
   * String...)}.
   */
  // TODO Remove
  public Meter meter(String metricName, String... metricPath) {
    return metricManager.meter(this, registryName, metricName, metricPath);
  }

  /**
   * Convenience method for {@link SolrMetricManager#counter(SolrMetricsContext, String, String,
   * String...)}.
   */
  // TODO Remove
  public Counter counter(String metricName, String... metricPath) {
    return metricManager.counter(this, registryName, metricName, metricPath);
  }

  /**
   * Convenience method for {@link SolrMetricManager#registerGauge(SolrMetricsContext, String,
   * Gauge, String, SolrMetricManager.ResolutionStrategy, String, String...)}.
   */
  // TODO Remove
  public void gauge(Gauge<?> gauge, boolean force, String metricName, String... metricPath) {
    metricManager.registerGauge(
        this,
        registryName,
        gauge,
        tag,
        force
            ? SolrMetricManager.ResolutionStrategy.REPLACE
            : SolrMetricManager.ResolutionStrategy.ERROR,
        metricName,
        metricPath);
  }

  /**
   * Convenience method for {@link SolrMetricManager#meter(SolrMetricsContext, String, String,
   * String...)}.
   */
  // TODO Remove
  public Timer timer(String metricName, String... metricPath) {
    return metricManager.timer(this, registryName, metricName, metricPath);
  }

  /**
   * Convenience method for {@link SolrMetricManager#histogram(SolrMetricsContext, String, String,
   * String...)}.
   */
  // TODO Remove
  public Histogram histogram(String metricName, String... metricPath) {
    return metricManager.histogram(this, registryName, metricName, metricPath);
  }

  /**
   * Get the {@link MetricRegistry} instance that is used for registering metrics in this context.
   */
  // TODO Change this to OTEL Scope?
  public MetricRegistry getMetricRegistry() {
    return metricManager.registry(registryName);
  }
}
