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

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.metrics.ObservableLongGauge;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.instrumentation.runtimemetrics.java17.RuntimeMetrics;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.metrics.otel.OtelUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages JVM metrics collection using OpenTelemetry Runtime Metrics with JFR features */
public class OtelRuntimeJvmMetrics {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private RuntimeMetrics runtimeMetrics;
  private ObservableLongGauge systemMemoryTotalGauge;
  private ObservableLongGauge systemMemoryFreeGauge;
  private boolean isInitialized = false;

  // Main feature flag to enable/disable all JVM metrics
  public static boolean isJvmMetricsEnabled() {
    return EnvUtils.getPropertyAsBool("solr.metrics.jvm.enabled", true);
  }

  @SuppressForbidden(
      reason =
          "com.sun.management.OperatingSystemMXBean is used intentionally for physical memory"
              + " gauges; guarded by instanceof check so gracefully absent on non-HotSpot JVMs")
  public OtelRuntimeJvmMetrics initialize(
      SolrMetricManager solrMetricManager, String registryName) {
    if (!isJvmMetricsEnabled()) return this;

    // a dummy instance; we only care to provide the MeterProvider
    var otel =
        new OpenTelemetry() {
          @Override
          public MeterProvider getMeterProvider() {
            return solrMetricManager.meterProvider(registryName);
          }

          @Override
          public TracerProvider getTracerProvider() {
            return OpenTelemetry.noop().getTracerProvider();
          }

          @Override
          public ContextPropagators getPropagators() {
            return OpenTelemetry.noop().getPropagators();
          }
        };
    this.runtimeMetrics =
        RuntimeMetrics.builder(otel)
            // TODO: We should have this configurable to enable/disable specific JVM metrics
            .enableAllFeatures()
            .build();
    java.lang.management.OperatingSystemMXBean osMxBean =
        ManagementFactory.getOperatingSystemMXBean();
    if (osMxBean instanceof com.sun.management.OperatingSystemMXBean extOsMxBean) {
      systemMemoryTotalGauge =
          solrMetricManager.observableLongGauge(
              registryName,
              "jvm.system.memory",
              "Total physical memory of the host or container in bytes."
                  + " On Linux with cgroup limits, reflects the container memory limit.",
              measurement -> {
                long total = extOsMxBean.getTotalMemorySize();
                if (total > 0) measurement.record(total);
              },
              OtelUnit.BYTES);
      systemMemoryFreeGauge =
          solrMetricManager.observableLongGauge(
              registryName,
              "jvm.system.memory.free",
              "Free (unused) physical memory of the host or container in bytes.",
              measurement -> {
                long free = extOsMxBean.getFreeMemorySize();
                if (free > 0) measurement.record(free);
              },
              OtelUnit.BYTES);
      log.info("Physical memory metrics enabled");
    } else {
      log.info(
          "Physical memory metrics unavailable:"
              + " com.sun.management.OperatingSystemMXBean not present on this JVM");
    }
    isInitialized = true;
    log.info("JVM metrics collection successfully initialized");
    return this;
  }

  public void close() {
    if (runtimeMetrics != null && isInitialized) {
      try {
        runtimeMetrics.close();
        if (systemMemoryTotalGauge != null) {
          systemMemoryTotalGauge.close();
          systemMemoryTotalGauge = null;
        }
        if (systemMemoryFreeGauge != null) {
          systemMemoryFreeGauge.close();
          systemMemoryFreeGauge = null;
        }
      } catch (Exception e) {
        log.error("Failed to close JVM metrics collection", e);
      } finally {
        runtimeMetrics = null;
        isInitialized = false;
      }
    }
  }

  public boolean isInitialized() {
    return isInitialized && runtimeMetrics != null;
  }
}
