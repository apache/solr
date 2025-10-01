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

import io.opentelemetry.instrumentation.runtimemetrics.java17.RuntimeMetrics;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import java.lang.invoke.MethodHandles;
import org.apache.solr.common.util.EnvUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages JVM metrics collection using OpenTelemetry Runtime Metrics with JFR features */
public class OtelRuntimeJvmMetrics {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private RuntimeMetrics runtimeMetrics;
  private boolean isInitialized = false;

  // Main feature flag to enable/disable all JVM metrics
  public static boolean isJvmMetricsEnabled() {
    return EnvUtils.getPropertyAsBool("solr.metrics.jvm.enabled", true);
  }

  public OtelRuntimeJvmMetrics initialize(
      SolrMetricManager solrMetricManager, String registryName) {
    if (!isJvmMetricsEnabled()) return this;

    this.runtimeMetrics =
        RuntimeMetrics.builder(
                OpenTelemetrySdk.builder()
                    .setMeterProvider(solrMetricManager.meterProvider(registryName))
                    .build())
            // TODO: We should have this configurable to enable/disable specific JVM metrics
            .enableAllFeatures()
            .build();
    isInitialized = true;
    log.info("JVM metrics collection successfully initialized");
    return this;
  }

  public void close() {
    if (runtimeMetrics != null && isInitialized) {
      try {
        runtimeMetrics.close();
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
