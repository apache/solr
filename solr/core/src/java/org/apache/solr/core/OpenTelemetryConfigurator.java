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
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Configures and loads/sets {@link GlobalOpenTelemetry} from a {@link OpenTelemetrySdk}. */
public abstract class OpenTelemetryConfigurator implements NamedListInitializedPlugin {

  public static final boolean TRACE_ID_GEN_ENABLED =
      Boolean.parseBoolean(EnvUtils.getProperty("solr.alwaysOnTraceId", "true"));

  private static final String DEFAULT_CLASS_NAME =
      EnvUtils.getProperty(
          "solr.otelDefaultConfigurator", "org.apache.solr.opentelemetry.OtelTracerConfigurator");

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static volatile boolean loaded = false;

  /**
   * Initializes the {@link io.opentelemetry.api.GlobalOpenTelemetry} instance by configuring the
   * {@link io.opentelemetry.sdk.OpenTelemetrySdk} through custom plugin, auto-configure or default
   * SDK.
   */
  public static synchronized void initializeOpenTelemetrySdk(
      NodeConfig cfg, SolrResourceLoader loader) {
    PluginInfo info = (cfg != null) ? cfg.getTracerConfiguratorPluginInfo() : null;

    if (info != null && info.isEnabled()) {
      OpenTelemetryConfigurator.configureCustomOpenTelemetrySdk(
          loader, cfg.getTracerConfiguratorPluginInfo());
    } else if (OpenTelemetryConfigurator.shouldAutoConfigOTEL()) {
      OpenTelemetryConfigurator.autoConfigureOpenTelemetrySdk(loader);
    } else {
      OpenTelemetryConfigurator.configureOpenTelemetrySdk();
    }
  }

  private static void configureOpenTelemetrySdk() {
    if (loaded) return;

    if (TRACE_ID_GEN_ENABLED) ExecutorUtil.addThreadLocalProvider(new ContextThreadLocalProvider());

    OpenTelemetry otel =
        OpenTelemetry.propagating(ContextPropagators.create(SimplePropagator.getInstance()));
    GlobalOpenTelemetry.set(otel);
    loaded = true;
  }

  private static void autoConfigureOpenTelemetrySdk(SolrResourceLoader loader) {
    if (loaded) return;
    try {
      OpenTelemetryConfigurator configurator =
          loader.newInstance(DEFAULT_CLASS_NAME, OpenTelemetryConfigurator.class);
      configurator.init(new NamedList<>());
      ExecutorUtil.addThreadLocalProvider(new ContextThreadLocalProvider());
      loaded = true;
    } catch (SolrException e) {
      log.error(
          "Unable to auto-config OpenTelemetry with class {}. Make sure you have enabled the 'opentelemetry' module",
          DEFAULT_CLASS_NAME,
          e);
    }
  }

  private static void configureCustomOpenTelemetrySdk(SolrResourceLoader loader, PluginInfo info) {
    if (loaded) return;

    OpenTelemetryConfigurator configurator =
        loader.newInstance(info.className, OpenTelemetryConfigurator.class);
    configurator.init(info.initArgs);
    ExecutorUtil.addThreadLocalProvider(new ContextThreadLocalProvider());
    loaded = true;
  }

  protected abstract Tracer getTracer();

  protected abstract OpenTelemetrySdk getOpenTelemetrySdk();

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
    String sysValue = System.getProperty(sysName);
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
