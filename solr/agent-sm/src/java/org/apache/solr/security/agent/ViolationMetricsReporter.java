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
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.LongAdder;
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
 *   <li>{@code security.agent.violations.file}
 *   <li>{@code security.agent.violations.network}
 *   <li>{@code security.agent.violations.exit}
 *   <li>{@code security.agent.violations.exec}
 * </ul>
 */
public final class ViolationMetricsReporter {

  // Per-type counters — incremented atomically from interceptor hot paths.
  private static final LongAdder FILE_COUNTER = new LongAdder();
  private static final LongAdder NETWORK_COUNTER = new LongAdder();
  private static final LongAdder EXIT_COUNTER = new LongAdder();
  private static final LongAdder EXEC_COUNTER = new LongAdder();

  // Metric names exposed in the Solr metrics registry.
  public static final String METRIC_FILE = "security.agent.violations.file";
  public static final String METRIC_NETWORK = "security.agent.violations.network";
  public static final String METRIC_EXIT = "security.agent.violations.exit";
  public static final String METRIC_EXEC = "security.agent.violations.exec";

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
   *      .getMethod("registerWithSolrMetrics", SolrMetricManager.class, String.class)
   *      .invoke(null, metricManager, "solr.jvm");
   * }</pre>
   *
   * <p>Because this module has no compile-time dependency on {@code solr:core}, the parameter type
   * is declared as {@link Object}; the reflective call site in {@code CoreContainer} passes the
   * real {@code SolrMetricManager} instance.
   *
   * @param metricManager the {@code SolrMetricManager} instance (type-erased to {@link Object} to
   *     avoid a compile-time dependency on solr:core)
   * @param registryName the target metrics registry name (e.g. {@code "solr.jvm"})
   */
  public static void registerWithSolrMetrics(Object metricManager, String registryName) {
    // Reflectively call SolrMetricManager.registerGauge(...) for each counter.
    // We use a Supplier<Long> so the gauge always returns the current counter value.
    try {
      Class<?> mmClass = metricManager.getClass();
      // SolrMetricManager.registerGauge(SolrInfoBean reporter, String registry,
      //     Gauge<?> gauge, String scope, boolean force, String... path)
      // We use the simpler overload that accepts a Supplier directly where available.
      // Fall back to the Gauge overload if the Supplier overload is absent.
      registerGauge(mmClass, metricManager, registryName, METRIC_FILE, FILE_COUNTER::sum);
      registerGauge(mmClass, metricManager, registryName, METRIC_NETWORK, NETWORK_COUNTER::sum);
      registerGauge(mmClass, metricManager, registryName, METRIC_EXIT, EXIT_COUNTER::sum);
      registerGauge(mmClass, metricManager, registryName, METRIC_EXEC, EXEC_COUNTER::sum);
    } catch (Exception e) {
      // Log to stderr — SLF4J may not be reachable from bootstrap context during premain.
      agentErr("[Solr SecurityAgent] Failed to register violation metrics: " + e);
    }
  }

  private static void registerGauge(
      Class<?> mmClass, Object mm, String registry, String metricName, Supplier<Long> valueSupplier)
      throws Exception {
    // Look for registerGauge(SolrInfoBean, String, Gauge, boolean, String, String...)
    // The gauge is a lambda; metrics names are split as scope + path segments.
    // We use the most compatible call: registerGauge(null, registry, gauge, false, metricName)
    Method registerMethod = null;
    for (Method m : mmClass.getMethods()) {
      if ("registerGauge".equals(m.getName())) {
        registerMethod = m;
        break;
      }
    }
    if (registerMethod == null) {
      agentErr("[Solr SecurityAgent] SolrMetricManager.registerGauge not found");
      return;
    }
    // Build a com.codahale.metrics.Gauge lambda via a proxy or cast.
    // Since we can't import codahale types here, create an anonymous class via reflection.
    // Most robust: use the Gauge<Long> functional interface via dynamic proxy.
    Object gauge = buildGauge(valueSupplier);
    // Invoke: registerGauge(SolrInfoBean=null, String registry, Gauge gauge, boolean force, String
    // name, String... path)
    registerMethod.invoke(mm, null, registry, gauge, false, metricName, new String[0]);
  }

  @SuppressForbidden(
      reason =
          "System.err is the only output channel available during premain/agent bootstrap, "
              + "before SLF4J is reachable from the bootstrap classloader.")
  private static void agentErr(String msg) {
    System.err.println(msg);
  }

  @SuppressForbidden(
      reason =
          "Thread.getContextClassLoader() is required here to locate com.codahale.metrics.Gauge "
              + "from the application classloader when this agent class lives in the bootstrap loader.")
  private static Object buildGauge(Supplier<Long> supplier) throws Exception {
    // com.codahale.metrics.Gauge is a single-method interface (functional).
    // Create a dynamic proxy implementing Gauge<Long>.
    Class<?> gaugeInterface =
        Class.forName(
            "com.codahale.metrics.Gauge", true, Thread.currentThread().getContextClassLoader());
    return Proxy.newProxyInstance(
        gaugeInterface.getClassLoader(),
        new Class<?>[] {gaugeInterface},
        (proxy, method, args) -> {
          if ("getValue".equals(method.getName())) return supplier.get();
          if ("equals".equals(method.getName())) return proxy == args[0];
          if ("hashCode".equals(method.getName())) return System.identityHashCode(proxy);
          return null;
        });
  }
}
