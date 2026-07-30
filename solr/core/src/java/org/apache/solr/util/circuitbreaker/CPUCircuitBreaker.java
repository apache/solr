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

package org.apache.solr.util.circuitbreaker;

import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Locale;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.OtelRuntimeJvmMetrics;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks current CPU usage and triggers if the specified threshold is breached.
 *
 * <p>This circuit breaker gets the recent average CPU usage and uses that data to take a decision.
 * We depend on OperatingSystemMXBean which does not allow a configurable interval of collection of
 * data.
 */
public class CPUCircuitBreaker extends CircuitBreaker implements SolrCoreAware {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private boolean enabled = false;
  private double cpuUsageThreshold;
  private CoreContainer cc;

  private static final ThreadLocal<Double> seenCPUUsage = ThreadLocal.withInitial(() -> 0.0);

  private static final ThreadLocal<Double> allowedCPUUsage = ThreadLocal.withInitial(() -> 0.0);

  public CPUCircuitBreaker() {
    super();
  }

  public CPUCircuitBreaker(CoreContainer coreContainer) {
    super();
    this.cc = coreContainer;
    enableIfSupported();
  }

  @Override
  public boolean isTripped() {
    if (!enabled) {
      if (log.isDebugEnabled()) {
        log.debug("CPU circuit breaker is disabled due to initialization failure.");
      }
      return false;
    }
    double localAllowedCPUUsage = getCpuUsageThreshold();
    double localSeenCPUUsage = calculateLiveCPUUsage();

    allowedCPUUsage.set(localAllowedCPUUsage);

    seenCPUUsage.set(localSeenCPUUsage);

    return (localSeenCPUUsage >= localAllowedCPUUsage);
  }

  @Override
  public String getErrorMessage() {
    return "CPU Circuit Breaker triggered as seen CPU usage is above allowed threshold. "
        + "Seen CPU usage "
        + seenCPUUsage.get()
        + " and allocated threshold "
        + allowedCPUUsage.get();
  }

  public CPUCircuitBreaker setThreshold(double thresholdValueInPercentage) {
    if (thresholdValueInPercentage > 100) {
      throw new IllegalArgumentException("Invalid Invalid threshold value.");
    }

    if (thresholdValueInPercentage <= 0) {
      throw new IllegalStateException("Threshold cannot be less than or equal to zero");
    }
    cpuUsageThreshold = thresholdValueInPercentage;
    return this;
  }

  public double getCpuUsageThreshold() {
    return cpuUsageThreshold;
  }

  /**
   * Calculate the CPU usage for the system in percentage.
   *
   * @return Percent CPU usage of -1 if value could not be obtained.
   */
  protected double calculateLiveCPUUsage() {
    if (!OtelRuntimeJvmMetrics.isJvmMetricsEnabled()) {
      throw new IllegalStateException("JVM metrics disabled. Cannot calculate CPU usage");
    }

    return this.cc
        .getMetricManager()
        .getPrometheusMetricReader("solr.jvm")
        .collect(name -> name.contains("jvm_system_cpu_utilization_ratio"))
        .stream()
        .filter(GaugeSnapshot.class::isInstance)
        .map(GaugeSnapshot.class::cast)
        .map(GaugeSnapshot::getDataPoints)
        .flatMap(Collection::stream)
        .findFirst()
        .map(dp -> dp.getValue() * 100)
        .orElse(-1.0); // Unable to unpack metric
  }

  @Override
  public void inform(SolrCore core) {
    this.cc = core.getCoreContainer();
    enableIfSupported();
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT,
        "%s(threshold=%f, warnOnly=%b)",
        getClass().getSimpleName(),
        cpuUsageThreshold,
        isWarnOnly());
  }

  private void enableIfSupported() {
    if (calculateLiveCPUUsage() < 0) {
      String msg =
          "Initialization failure for CPU circuit breaker. Unable to get 'systemCpuLoad', not supported by the JVM?";
      if (log.isErrorEnabled()) {
        log.error(msg);
      }
      enabled = false;
    } else {
      enabled = true;
    }
  }
}
