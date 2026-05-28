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
package org.apache.solr.security;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.function.Consumer;
import org.apache.solr.metrics.SolrMetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers the security-agent violation counter with {@link SolrMetricManager} using a proper OTel
 * label, keeping all {@code io.opentelemetry} types inside {@code solr:core} where they belong.
 *
 * <p>The agent JAR ({@code solr:agent-sm}) lives in the bootstrap classloader and has no OTel
 * compile dependency. This class bridges the gap: it reads the raw {@code long} counts from {@code
 * ViolationMetricsReporter} reflectively (trivial — only {@code long} primitives cross the
 * boundary), and builds the full {@link Consumer}{@code <}{@link ObservableLongMeasurement}{@code
 * >} callback using OTel types natively.
 *
 * <p>A single OTel observable counter named {@code solr.security.agent.violations} is registered,
 * with label {@code type=file|network|exit|exec}. In Prometheus format:
 *
 * <pre>
 *   solr_security_agent_violations_total{type="file"}    N
 *   solr_security_agent_violations_total{type="network"} N
 *   solr_security_agent_violations_total{type="exit"}    N
 *   solr_security_agent_violations_total{type="exec"}    N
 * </pre>
 */
public final class AgentViolationMetrics {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String METRIC_NAME = "solr.security.agent.violations";
  private static final AttributeKey<String> TYPE_KEY = AttributeKey.stringKey("type");

  private AgentViolationMetrics() {}

  /**
   * Registers the violation counter. No-op if the agent JAR is not present.
   *
   * @param metricManager the live {@link SolrMetricManager}
   * @param registryName the target registry (e.g. {@code "solr.node"})
   */
  public static void register(SolrMetricManager metricManager, String registryName) {
    try {
      // ViolationMetricsReporter is in the bootstrap classloader via Boot-Class-Path.
      Class<?> reporter =
          Class.forName("org.apache.solr.security.agent.ViolationMetricsReporter", false, null);
      Method fileCount = reporter.getMethod("fileCount");
      Method networkCount = reporter.getMethod("networkCount");
      Method exitCount = reporter.getMethod("exitCount");
      Method execCount = reporter.getMethod("execCount");

      // Pre-build Attributes once — these are stable for the lifetime of the process.
      Attributes fileAttrs = Attributes.of(TYPE_KEY, "file");
      Attributes networkAttrs = Attributes.of(TYPE_KEY, "network");
      Attributes exitAttrs = Attributes.of(TYPE_KEY, "exit");
      Attributes execAttrs = Attributes.of(TYPE_KEY, "exec");

      Consumer<ObservableLongMeasurement> callback =
          measurement -> {
            try {
              measurement.record((long) fileCount.invoke(null), fileAttrs);
              measurement.record((long) networkCount.invoke(null), networkAttrs);
              measurement.record((long) exitCount.invoke(null), exitAttrs);
              measurement.record((long) execCount.invoke(null), execAttrs);
            } catch (ReflectiveOperationException ignored) {
              // Should never happen — these are simple no-arg static methods.
            }
          };

      metricManager.observableLongCounter(
          registryName,
          METRIC_NAME,
          "Security agent violation count by type (file, network, exit, exec).",
          callback,
          null);

      log.debug("Security agent violation metrics registered under registry '{}'", registryName);
    } catch (ClassNotFoundException ignored) {
      // Agent JAR not loaded (e.g. SOLR_SECURITY_AGENT_SKIP=true) — nothing to register.
    } catch (Exception e) {
      log.warn("Failed to register security agent violation metrics", e);
    }
  }
}
