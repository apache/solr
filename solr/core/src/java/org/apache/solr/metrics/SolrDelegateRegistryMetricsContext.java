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
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

/**
 * This class represents a metrics context that is delegate aware in that it is aware of multiple
 * metric registries, a primary and a delegate. This enables creating metrics that are tracked at
 * multiple levels, i.e. core-level and node-level. This class will create instances of new Timer,
 * Meter, Counter, Histogram implementations that hold references to both primary and delegate
 * implementations of corresponding classes. The DelegateRegistry* metric classes are just
 * pass-through to two different implementations. As such the DelegateRegistry* metric classes do
 * not hold any metric data themselves.
 *
 * @see org.apache.solr.metrics.SolrMetricsContext
 */
public class SolrDelegateRegistryMetricsContext extends SolrMetricsContext {

  private final String delegateRegistry;

  public SolrDelegateRegistryMetricsContext(
      SolrMetricManager metricManager, String registry, String tag, String delegateRegistry) {
    super(metricManager, registry, tag);
    this.delegateRegistry = delegateRegistry;
  }

  @Override
  public Meter meter(String metricName, String... metricPath) {
    return new DelegateRegistryMeter(
        super.meter(metricName, metricPath),
        getMetricManager().meter(this, delegateRegistry, metricName, metricPath));
  }

  @Override
  public Counter counter(String metricName, String... metricPath) {
    return new DelegateRegistryCounter(
        super.counter(metricName, metricPath),
        getMetricManager().counter(this, delegateRegistry, metricName, metricPath));
  }

  @Override
  public Timer timer(String metricName, String... metricPath) {
    return new DelegateRegistryTimer(
        MetricSuppliers.getClock(
            getMetricManager().getMetricsConfig().getTimerSupplier(), MetricSuppliers.CLOCK),
        super.timer(metricName, metricPath),
        getMetricManager().timer(this, delegateRegistry, metricName, metricPath));
  }

  @Override
  public Histogram histogram(String metricName, String... metricPath) {
    return new DelegateRegistryHistogram(
        super.histogram(metricName, metricPath),
        getMetricManager().histogram(this, delegateRegistry, metricName, metricPath));
  }

  @Override
  public SolrMetricsContext getChildContext(Object child) {
    return new SolrDelegateRegistryMetricsContext(
        getMetricManager(),
        getRegistryName(),
        SolrMetricProducer.getUniqueMetricTag(child, getTag()),
        delegateRegistry);
  }
}
