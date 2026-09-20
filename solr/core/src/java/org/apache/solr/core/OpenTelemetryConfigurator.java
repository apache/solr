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

package org.apache.solr.core;

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import java.lang.invoke.MethodHandles;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.apache.solr.util.tracing.SimplePropagator;
import org.apache.solr.util.tracing.TraceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Configures and sets {@link GlobalOpenTelemetry}. */
public abstract class OpenTelemetryConfigurator implements NamedListInitializedPlugin {

  public static final boolean TRACE_ID_GEN_ENABLED =
      EnvUtils.getPropertyAsBool("solr.tracing.always.on.enabled", true);

  private static final String DEFAULT_CLASS_NAME =
      EnvUtils.getProperty(
          "solr.otelDefaultConfigurator", "org.apache.solr.opentelemetry.OtelTracerConfigurator");

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static volatile boolean loaded = false;

  /**
   * Initializes {@link io.opentelemetry.api.GlobalOpenTelemetry} from a custom plugin,
   * auto-configuration, or simple trace ID propagation. Does nothing if the OpenTelemetry Java
   * agent is present, since it has already done this.
   */
  public static synchronized void initializeOpenTelemetrySdk(
      NodeConfig cfg, SolrResourceLoader loader) {
    // synchronized & "loaded" to avoid races in tests starting Solr nodes concurrently
    if (loaded) return;
    loaded = true;

    if (TraceUtils.OTEL_AGENT_PRESENT) {
      log.info("OpenTelemetry Java agent is installed; using the OpenTelemetry it registered.");
    } else {
      PluginInfo info = (cfg != null) ? cfg.getTracerConfiguratorPluginInfo() : null;
      OpenTelemetry otel = null;
      if (info != null && info.isEnabled()) {
        OpenTelemetryConfigurator configurator =
            loader.newInstance(info.className, OpenTelemetryConfigurator.class);
        configurator.init(info.initArgs);
        otel = configurator.createOpenTelemetry();
      } else if (shouldAutoConfigOTEL()) {
        otel = autoConfigOTEL(loader); // null if it failed to load
      }
      if (otel == null && TRACE_ID_GEN_ENABLED) {
        otel = OpenTelemetry.propagating(ContextPropagators.create(SimplePropagator.getInstance()));
        log.info("OpenTelemetry loaded with simple propagation only.");
      }
      boolean isNoop = otel == null;

      try {
        // throws IllegalStateException if already set
        GlobalOpenTelemetry.set(isNoop ? OpenTelemetry.noop() : otel);
        if (isNoop) {
          return; // no point in the thread local provider below
        }
      } catch (IllegalStateException e) {
        log.info("GlobalOpenTelemetry was already initialized by something else; using that.");
      }
    }

    ExecutorUtil.addThreadLocalProvider(new ContextThreadLocalProvider());
  }

  private static OpenTelemetry autoConfigOTEL(SolrResourceLoader loader) {
    try {
      OpenTelemetryConfigurator configurator =
          loader.newInstance(DEFAULT_CLASS_NAME, OpenTelemetryConfigurator.class);
      configurator.init(new NamedList<>());
      log.info("OpenTelemetry loaded via auto configuration.");
      return configurator.createOpenTelemetry();
    } catch (SolrException e) {
      log.error(
          "Unable to auto-config OpenTelemetry with class {}. Make sure you have enabled the 'opentelemetry' module",
          DEFAULT_CLASS_NAME,
          e);
      return null;
    }
  }

  /**
   * Creates the {@link OpenTelemetry} to install as {@link GlobalOpenTelemetry}; called after
   * {@link #init(NamedList)}. Implementations must not set {@link GlobalOpenTelemetry} themselves.
   */
  protected abstract OpenTelemetry createOpenTelemetry();

  private static class ContextThreadLocalProvider
      implements ExecutorUtil.InheritableThreadLocalProvider {

    @Override
    public void store(AtomicReference<Object> ctx) {
      ctx.set(Context.current());
    }

    @Override
    public void set(AtomicReference<Object> ctx) {
      var traceContext = (Context) ctx.get();
      var scope = traceContext.makeCurrent();
      ctx.set(scope);
    }

    @Override
    public void clean(AtomicReference<Object> ctx) {
      var scope = (Scope) ctx.get();
      scope.close();
    }
  }

  /**
   * Best effort way to determine if we should attempt to init OTEL from system properties.
   *
   * @return true if OTEL should be init
   */
  static boolean shouldAutoConfigOTEL() {
    var env = System.getenv();
    boolean isSdkDisabled = Boolean.parseBoolean(getConfig("OTEL_SDK_DISABLED", env));
    if (isSdkDisabled) {
      return false;
    }
    return getConfig("OTEL_SERVICE_NAME", env) != null;
  }

  /**
   * Returns system property if found, else returns environment variable, or null if none found.
   *
   * @param envName the environment variable to look for
   * @param env current env
   * @return the resolved value
   */
  protected static String getConfig(String envName, Map<String, String> env) {
    String sysName = envNameToSyspropName(envName);
    String sysValue = EnvUtils.getProperty(sysName);
    String envValue = env.get(envName);
    return sysValue != null ? sysValue : envValue;
  }

  /**
   * In OTEL Java SDK there is a convention that the java property name for OTEL_FOO_BAR is
   * otel.foo.bar
   *
   * @param envName the environmnet name to convert
   * @return the corresponding sysprop name
   */
  protected static String envNameToSyspropName(String envName) {
    return envName.toLowerCase(Locale.ROOT).replace("_", ".");
  }

  @VisibleForTesting
  public static void resetForTest() {
    loaded = false;
    GlobalOpenTelemetry.resetForTest();
  }
}
