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

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import java.lang.invoke.MethodHandles;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.apache.solr.util.tracing.SimplePropagator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Produces an OpenTracing {@link Tracer} from configuration. */
public abstract class TracerConfigurator implements NamedListInitializedPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final boolean TRACE_ID_GEN_ENABLED =
      Boolean.parseBoolean(System.getProperty("solr.alwaysOnTraceId", "true"));

  private static final String DEFAULT_CLASS_NAME =
      System.getProperty(
          "solr.otelDefaultConfigurator", "org.apache.solr.opentelemetry.OtelTracerConfigurator");

  public abstract Tracer getTracer();

  public static Tracer loadTracer(SolrResourceLoader loader, PluginInfo info) {
    // In "normal" Solr operation, loadTracer is called once in Solr's lifetime.
    // In test mode we run many test suites, sometimes multiple servers per test suite.
    // It's important that the tracing config not change throughout a test suite because of the
    //   static singleton pattern and assumptions based on this.

    if (info != null && info.isEnabled()) {
      GlobalTracer.registerIfAbsent(
          () -> {
            TracerConfigurator configurator =
                loader.newInstance(info.className, TracerConfigurator.class);
            configurator.init(info.initArgs);
            return configurator.getTracer();
          });
    } else if (shouldAutoConfigOTEL()) {
      GlobalTracer.registerIfAbsent(() -> autoConfigOTEL(loader));
    } else if (TRACE_ID_GEN_ENABLED) {
      SimplePropagator.load();
    }
    if (GlobalTracer.isRegistered()) {
      // ideally we would furthermore check that it's not a no-op impl either but
      //  GlobalTracer.get() always returns a GlobalTracer implementing Tracer that delegates
      //  to the real Tracer (that may or may not be a No-Op impl).
      ExecutorUtil.addThreadLocalProvider(new TracerConfigurator.SpanThreadLocalProvider());
    }

    return GlobalTracer.get();
  }

  /**
   * Propagate the active span across threads. New spans are not created, which means that we're
   * possibly exposing a Span to a thread that may find that it has already finished, depending on
   * how the instrumented thread pool is used. It's probably fine to create new spans related to a
   * finished span? It's not okay to otherwise touch a finished span (e.g. to log or tag).
   *
   * <p>This strategy is also used by {@code
   * brave.propagation.CurrentTraceContext#wrap(java.lang.Runnable)}.
   */
  private static class SpanThreadLocalProvider
      implements ExecutorUtil.InheritableThreadLocalProvider {
    private final Tracer tracer = GlobalTracer.get();

    @Override
    public void store(AtomicReference<Object> ctx) {
      assert tracer == GlobalTracer.get() : "Tracer changed; not supported!";
      ctx.set(tracer.scopeManager().activeSpan());
    }

    @Override
    public void set(AtomicReference<Object> ctx) {
      final Span span = (Span) ctx.get();
      if (span != null) {
        log.trace("Thread received span to do async work: {}", span);
        final Scope scope = tracer.scopeManager().activate(span);
        ctx.set(scope);
      }
    }

    @Override
    public void clean(AtomicReference<Object> ctx) {
      Scope scope = (Scope) ctx.get();
      if (scope != null) {
        scope.close();
      }
    }
  }

  private static Tracer autoConfigOTEL(SolrResourceLoader loader) {
    try {
      TracerConfigurator configurator =
          loader.newInstance(DEFAULT_CLASS_NAME, TracerConfigurator.class);
      configurator.init(new NamedList<>());
      return configurator.getTracer();
    } catch (SolrException e) {
      log.error(
          "Unable to auto-config OpenTelemetry with class {}. Make sure you have enabled the 'opentelemetry' module",
          DEFAULT_CLASS_NAME,
          e);
    }
    return GlobalTracer.get();
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
}
