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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import java.lang.invoke.MethodHandles;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrMetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks current CPU usage and triggers if the specified threshold is breached.
 *
 * <p>This circuit breaker gets the recent average CPU usage and uses that data to take a decision.
 * We depend on OperatingSystemMXBean which does not allow a configurable interval of collection of
 * data.
 */
public class CPUCircuitBreaker extends CircuitBreaker {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private boolean enabled = true;
  private double cpuUsageThreshold;
  private final SolrCore core;

  private static final ThreadLocal<Double> seenCPUUsage = ThreadLocal.withInitial(() -> 0.0);

  private static final ThreadLocal<Double> allowedCPUUsage = ThreadLocal.withInitial(() -> 0.0);

  public CPUCircuitBreaker(SolrCore core) {
    super();
    this.core = core;
  }

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    double localSeenCPUUsage = calculateLiveCPUUsage();

    if (localSeenCPUUsage < 0) {
      String msg =
          "Initialization failure for CPU circuit breaker. Unable to get 'systemCpuLoad', not supported by the JVM?";
      if (log.isErrorEnabled()) {
        log.error(msg);
      }
      enabled = false;
    }
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

  public void setThreshold(double thresholdValueInPercentage) {
    if (thresholdValueInPercentage > 100) {
      throw new IllegalArgumentException("Invalid Invalid threshold value.");
    }

    if (thresholdValueInPercentage <= 0) {
      throw new IllegalStateException("Threshold cannot be less than or equal to zero");
    }
    cpuUsageThreshold = thresholdValueInPercentage;
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
    // TODO: Use Codahale Meter to calculate the value
    Metric metric =
        this.core
            .getCoreContainer()
            .getMetricManager()
            .registry("solr.jvm")
            .getMetrics()
            .get("os.systemCpuLoad");

    if (metric == null) {
      return -1.0;
    }

    if (metric instanceof Gauge) {
      @SuppressWarnings({"rawtypes"})
      Gauge gauge = (Gauge) metric;
      // unwrap if needed
      if (gauge instanceof SolrMetricManager.GaugeWrapper) {
        gauge = ((SolrMetricManager.GaugeWrapper) gauge).getGauge();
      }
      return (Double) gauge.getValue() * 100;
    }

    return -1.0; // Unable to unpack metric
  }
}
