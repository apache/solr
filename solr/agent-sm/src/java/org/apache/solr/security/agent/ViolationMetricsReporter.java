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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

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
 * <h2>Metric name and label</h2>
 *
 * A single OTel observable counter {@value #METRIC_NAME} is registered with the label key {@value
 * #LABEL_TYPE}. The label takes one of four values: {@code file}, {@code network}, {@code exit}, or
 * {@code exec}. In Prometheus format the counter appears as:
 *
 * <pre>
 *   solr_security_agent_violations_total{type="file"}    N
 *   solr_security_agent_violations_total{type="network"} N
 *   solr_security_agent_violations_total{type="exit"}    N
 *   solr_security_agent_violations_total{type="exec"}    N
 * </pre>
 */
public final class ViolationMetricsReporter {

  // Per-type counters — incremented atomically from interceptor hot paths.
  private static final LongAdder FILE_COUNTER = new LongAdder();
  private static final LongAdder NETWORK_COUNTER = new LongAdder();
  private static final LongAdder EXIT_COUNTER = new LongAdder();
  private static final LongAdder EXEC_COUNTER = new LongAdder();

  /** OTel metric name for the single labeled violation counter. */
  public static final String METRIC_NAME = "solr.security.agent.violations";

  /** OTel label key used to distinguish violation types. */
  public static final String LABEL_TYPE = "type";

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
   * Registers a single labeled violation counter with the given {@code SolrMetricManager}.
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
   * <p>One OTel observable counter named {@value #METRIC_NAME} is registered. The callback records
   * four labeled values (one per violation type) using {@code
   * ObservableLongMeasurement.record(long, Attributes)}. {@code io.opentelemetry.api.common
   * .Attributes} instances are created reflectively via the app classloader because this class
   * lives in the bootstrap classloader and has no OTel dependency.
   *
   * @param metricManager the {@code SolrMetricManager} instance (type-erased to {@link Object})
   * @param registryName the target metrics registry name (e.g. {@code "solr.node"})
   */
  public static void registerWithSolrMetrics(Object metricManager, String registryName) {
    Class<?> mmClass = metricManager.getClass();
    // SolrMetricManager.observableLongCounter(String registry, String name, String description,
    //     Consumer<ObservableLongMeasurement> callback, OtelUnit unit)
    Method counterMethod =
        findMethod(
            mmClass, "observableLongCounter", "String", "String", "String", "Consumer", "OtelUnit");
    if (counterMethod == null) {
      // Throw so that CoreContainer's reflective call site logs this at WARN.  A missing method
      // almost certainly means the SolrMetricManager API changed without updating this class.
      throw new IllegalStateException(
          "SolrMetricManager.observableLongCounter(String,String,String,Consumer,OtelUnit)"
              + " not found on "
              + mmClass.getName()
              + " — violation metrics cannot be registered in /admin/metrics."
              + " This likely indicates a SolrMetricManager API change.");
    }
    try {
      // Build Attributes objects reflectively — this class is in the bootstrap classloader with no
      // OTel on its classpath. Use the app classloader (reachable via metricManager) to access
      // io.opentelemetry.api.common.{AttributeKey, Attributes} at registration time.
      ClassLoader cl = metricManager.getClass().getClassLoader();
      Class<?> attrKeyClass = Class.forName("io.opentelemetry.api.common.AttributeKey", false, cl);
      Class<?> attrsClass = Class.forName("io.opentelemetry.api.common.Attributes", false, cl);
      // AttributeKey.stringKey("type") — produces AttributeKey<String>
      Object typeKey = attrKeyClass.getMethod("stringKey", String.class).invoke(null, LABEL_TYPE);
      // Attributes.of(AttributeKey<T>, T) — after type erasure: (AttributeKey, Object)
      Method attrOf = attrsClass.getMethod("of", attrKeyClass, Object.class);
      final Object fileAttrs = attrOf.invoke(null, typeKey, "file");
      final Object networkAttrs = attrOf.invoke(null, typeKey, "network");
      final Object exitAttrs = attrOf.invoke(null, typeKey, "exit");
      final Object execAttrs = attrOf.invoke(null, typeKey, "exec");

      // Cache the two-arg record(long, Attributes) method, found lazily on first callback
      // invocation.  AtomicReference is used because lambda captures must be effectively final.
      final AtomicReference<Method> recordRef = new AtomicReference<>();

      Consumer<Object> callback =
          measurement -> {
            try {
              Method record = recordRef.get();
              if (record == null) {
                // Locate record(long, <Attributes>) on the concrete ObservableLongMeasurement impl.
                // The second parameter type is Attributes, erased to Object at the call site —
                // scan by arity and first-param type to avoid a hard dependency on
                // Attributes.class.
                for (Method m : measurement.getClass().getMethods()) {
                  if ("record".equals(m.getName())
                      && m.getParameterCount() == 2
                      && m.getParameterTypes()[0] == long.class) {
                    recordRef.compareAndSet(null, m);
                    break;
                  }
                }
                record = recordRef.get();
              }
              if (record != null) {
                record.invoke(measurement, FILE_COUNTER.sum(), fileAttrs);
                record.invoke(measurement, NETWORK_COUNTER.sum(), networkAttrs);
                record.invoke(measurement, EXIT_COUNTER.sum(), exitAttrs);
                record.invoke(measurement, EXEC_COUNTER.sum(), execAttrs);
              }
            } catch (ReflectiveOperationException ignored) {
              // Silently skip if record(long, Attributes) is unavailable
            }
          };

      counterMethod.invoke(
          metricManager,
          registryName,
          METRIC_NAME,
          "Security agent violation count by type (file, network, exit, exec).",
          callback,
          null);
    } catch (Exception e) {
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

  @SuppressForbidden(
      reason =
          "System.err is the only output channel available during premain/agent bootstrap, "
              + "before SLF4J is reachable from the bootstrap classloader.")
  private static void agentErr(String msg) {
    System.err.println(msg);
  }
}
