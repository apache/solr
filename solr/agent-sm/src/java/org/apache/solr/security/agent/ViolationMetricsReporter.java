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
package org.apache.solr.security.agent;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Maintains per-type violation counters and registers them with Solr's metrics registry once it
 * becomes available.
 *
 * <h2>Deferred registration pattern</h2>
 *
 * The agent starts (via {@code premain()}) before Solr's {@code SolrMetricManager} is initialized.
 * Counters are maintained from the very first violation using {@link LongAdder}s. When {@code
 * CoreContainer} initializes {@code SolrMetricManager}, it calls {@link
 * #registerWithSolrMetrics(Object, String)} reflectively (to avoid a compile-time dependency on
 * {@code solr:agent-sm} from {@code solr:core}). At that point the accumulated counts are already
 * in the counters and the registration just wires them to the metrics registry.
 *
 * <h2>Metric names</h2>
 *
 * <ul>
 *   <li>{@code solr.security.agent.violations.file}
 *   <li>{@code solr.security.agent.violations.network}
 *   <li>{@code solr.security.agent.violations.exit}
 *   <li>{@code solr.security.agent.violations.exec}
 * </ul>
 *
 * <p>In Prometheus format these appear as {@code solr_security_agent_violations_file_total}, etc.
 */
public final class ViolationMetricsReporter {

  // Per-type counters — incremented atomically from interceptor hot paths.
  private static final LongAdder FILE_COUNTER = new LongAdder();
  private static final LongAdder NETWORK_COUNTER = new LongAdder();
  private static final LongAdder EXIT_COUNTER = new LongAdder();
  private static final LongAdder EXEC_COUNTER = new LongAdder();

  // Metric names exposed in the Solr metrics registry.
  public static final String METRIC_FILE = "solr.security.agent.violations.file";
  public static final String METRIC_NETWORK = "solr.security.agent.violations.network";
  public static final String METRIC_EXIT = "solr.security.agent.violations.exit";
  public static final String METRIC_EXEC = "solr.security.agent.violations.exec";

  private ViolationMetricsReporter() {}

  // ---------------------------------------------------------------------------
  // Counter increment API (called by interceptors)
  // ---------------------------------------------------------------------------

  /** Increments the file-access violation counter. */
  public static void incrementFile() {
    FILE_COUNTER.increment();
  }

  /** Increments the network-connection violation counter. */
  public static void incrementNetwork() {
    NETWORK_COUNTER.increment();
  }

  /** Increments the System.exit() violation counter. */
  public static void incrementExit() {
    EXIT_COUNTER.increment();
  }

  /** Increments the process-exec violation counter. */
  public static void incrementExec() {
    EXEC_COUNTER.increment();
  }

  // ---------------------------------------------------------------------------
  // Counter read API (used by tests)
  // ---------------------------------------------------------------------------

  /** Returns the current file-access violation count. */
  public static long fileCount() {
    return FILE_COUNTER.sum();
  }

  /** Returns the current network-connection violation count. */
  public static long networkCount() {
    return NETWORK_COUNTER.sum();
  }

  /** Returns the current System.exit() violation count. */
  public static long exitCount() {
    return EXIT_COUNTER.sum();
  }

  /** Returns the current process-exec violation count. */
  public static long execCount() {
    return EXEC_COUNTER.sum();
  }

  // ---------------------------------------------------------------------------
  // Deferred metrics registration (called reflectively from CoreContainer)
  // ---------------------------------------------------------------------------

  /**
   * Registers the four violation counters with the given {@code SolrMetricManager} in the specified
   * registry.
   *
   * <p>This method is called reflectively from {@code CoreContainer} to avoid a compile-time
   * dependency between {@code solr:core} and {@code solr:agent-sm}. The signature must match what
   * CoreContainer expects:
   *
   * <pre>{@code
   * Class.forName("org.apache.solr.security.agent.ViolationMetricsReporter", false, null)
   *      .getMethod("registerWithSolrMetrics", Object.class, String.class)
   *      .invoke(null, metricManager, "solr.node");
   * }</pre>
   *
   * <p>Because this module has no compile-time dependency on {@code solr:core}, the parameter type
   * is declared as {@link Object}; the reflective call site in {@code CoreContainer} passes the
   * real {@code SolrMetricManager} instance.
   *
   * <p>Metrics are registered as OTel observable counters via {@code
   * SolrMetricManager.observableLongCounter()}. In Prometheus format they appear as {@code
   * solr_security_agent_violations_file_total} etc.
   *
   * @param metricManager the {@code SolrMetricManager} instance (type-erased to {@link Object} to
   *     avoid a compile-time dependency on solr:core)
   * @param registryName the target metrics registry name (e.g. {@code "solr.node"})
   */
  public static void registerWithSolrMetrics(Object metricManager, String registryName) {
    try {
      Class<?> mmClass = metricManager.getClass();
      // SolrMetricManager.observableLongCounter(String registry, String name, String description,
      //     Consumer<ObservableLongMeasurement> callback, OtelUnit unit)
      // SolrMetricManager.observableLongCounter(String, String, String, Consumer, OtelUnit)
      Method counterMethod =
          findMethod(
              mmClass,
              "observableLongCounter",
              "String",
              "String",
              "String",
              "Consumer",
              "OtelUnit");
      if (counterMethod == null) {
        agentErr(
            "[Solr SecurityAgent] SolrMetricManager.observableLongCounter not found"
                + " — violation metrics will not be registered in /admin/metrics");
        return;
      }
      registerCounter(
          counterMethod,
          metricManager,
          registryName,
          METRIC_FILE,
          "Security agent file-access violation count",
          FILE_COUNTER::sum);
      registerCounter(
          counterMethod,
          metricManager,
          registryName,
          METRIC_NETWORK,
          "Security agent network-connection violation count",
          NETWORK_COUNTER::sum);
      registerCounter(
          counterMethod,
          metricManager,
          registryName,
          METRIC_EXIT,
          "Security agent JVM-exit violation count",
          EXIT_COUNTER::sum);
      registerCounter(
          counterMethod,
          metricManager,
          registryName,
          METRIC_EXEC,
          "Security agent process-exec violation count",
          EXEC_COUNTER::sum);
    } catch (Exception e) {
      // Log to stderr — SLF4J may not be reachable from bootstrap context during premain.
      agentErr("[Solr SecurityAgent] Failed to register violation metrics: " + e);
    }
  }

  /**
   * Finds a public method on {@code cls} by name and parameter type simple-names. Using simple
   * names avoids a compile-time dependency on {@code solr:core} types (e.g. {@code OtelUnit}).
   */
  private static Method findMethod(Class<?> cls, String name, String... paramTypeSimpleNames) {
    for (Method m : cls.getMethods()) {
      if (!name.equals(m.getName())) continue;
      Class<?>[] params = m.getParameterTypes();
      if (params.length != paramTypeSimpleNames.length) continue;
      boolean match = true;
      for (int i = 0; i < params.length; i++) {
        if (!params[i].getSimpleName().equals(paramTypeSimpleNames[i])) {
          match = false;
          break;
        }
      }
      if (match) return m;
    }
    return null;
  }

  private static void registerCounter(
      Method counterMethod,
      Object mm,
      String registry,
      String name,
      String description,
      Supplier<Long> valueSupplier)
      throws Exception {
    // Consumer<ObservableLongMeasurement> — type-erased to Consumer at runtime.
    // Called by the OTel SDK at each metric collection cycle.
    Consumer<Object> callback =
        measurement -> {
          try {
            Method record = measurement.getClass().getMethod("record", long.class);
            record.invoke(measurement, valueSupplier.get());
          } catch (ReflectiveOperationException ignored) {
            // Silently skip if ObservableLongMeasurement.record() is unavailable
          }
        };
    // observableLongCounter(registry, name, description, callback, unit=null)
    counterMethod.invoke(mm, registry, name, description, callback, null);
  }

  @SuppressForbidden(
      reason =
          "System.err is the only output channel available during premain/agent bootstrap, "
              + "before SLF4J is reachable from the bootstrap classloader.")
  private static void agentErr(String msg) {
    System.err.println(msg);
  }
}
