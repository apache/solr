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
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import java.lang.invoke.MethodHandles;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.URLUtil;
import org.apache.solr.logging.DeprecationLog;
import org.apache.solr.util.AddressUtils;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.apache.solr.util.tracing.SimplePropagator;
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

  private static final AttributeKey<String> SERVICE_NAME = AttributeKey.stringKey("service.name");
  private static final AttributeKey<String> SERVICE_INSTANCE_ID =
      AttributeKey.stringKey("service.instance.id");
  private static final AttributeKey<String> HOST_NAME = AttributeKey.stringKey("host.name");

  /** What the SDK uses for {@code service.name} when nothing configures it. */
  private static final String UNKNOWN_SERVICE = "unknown_service:java";

  /**
   * {@code OTEL_METRICS_EXPORTER} as the operator configured it. Captured at class initialization,
   * which happens before the {@code opentelemetry} module overwrites the system property with
   * {@code none} to stop the autoconfigured SDK exporting metrics Solr already owns.
   */
  private static final String CONFIGURED_METRICS_EXPORTER =
      getConfig("OTEL_METRICS_EXPORTER", System.getenv());

  private static volatile boolean loaded = false;

  /**
   * Resource attributes describing this node, shared by every signal. Resolved once by {@link
   * #initializeOpenTelemetrySdk}; until then, and in processes that never call it, the SDK default.
   */
  private static volatile Resource telemetryResource = Resource.getDefault();

  /** Is the OpenTelemetry Java agent present? */
  protected static final boolean OTEL_AGENT_PRESENT;

  static {
    // https://github.com/open-telemetry/opentelemetry-java-instrumentation/discussions/9173
    boolean b;
    try {
      Class.forName("io.opentelemetry.javaagent.OpenTelemetryAgent");
      b = true;
    } catch (ClassNotFoundException e) {
      b = false;
    }
    OTEL_AGENT_PRESENT = b;
  }

  /**
   * Initializes {@link io.opentelemetry.api.GlobalOpenTelemetry} from a custom plugin,
   * auto-configuration, or simple trace ID propagation. Does nothing if the OpenTelemetry Java
   * agent is present or {@link GlobalOpenTelemetry} is already set (e.g. by tests).
   */
  public static synchronized void initializeOpenTelemetrySdk(
      NodeConfig cfg, SolrResourceLoader loader) {
    // synchronized & "loaded" to avoid races in tests starting Solr nodes concurrently
    if (loaded) return;
    loaded = true;

    // Publish Solr's identity through OTEL_RESOURCE_ATTRIBUTES before any configurator reads it, so
    // an autoconfigured SDK and Solr's own meter providers resolve the same resource. Deliberately
    // not otel.service.name: that one doubles as the module's auto-activation switch.
    setDefaultResourceAttributes(cfg, System.getenv());
    telemetryResource = resourceFromConfig(System.getenv());

    // Check for the agent explicitly; older agents don't instrument isSet(), so it returns false
    if (OTEL_AGENT_PRESENT) {
      log.info("OpenTelemetry Java agent is installed; using the OpenTelemetry it registered.");
    } else if (GlobalOpenTelemetry.isSet()) {
      // e.g. a 3rd party agent, or an early GlobalOpenTelemetry.get() call which sets a no-op
      log.info("GlobalOpenTelemetry was already initialized by something else; using that.");
    } else {
      PluginInfo info = (cfg != null) ? cfg.getTracerConfiguratorPluginInfo() : null;
      OpenTelemetry otel = null;
      if (info != null && info.isEnabled()) {
        OpenTelemetryConfigurator configurator =
            loader.newInstance(info.className, OpenTelemetryConfigurator.class);
        configurator.init(info.initArgs);
        otel = configurator.createOpenTelemetry();
        adoptConfiguredResource(configurator);
        log.info("OpenTelemetry loaded via {}", info.className);
      } else if (shouldAutoConfigOTEL()) {
        otel = autoConfigOTEL(loader); // null if it failed to load
      }
      if (otel == null && TRACE_ID_GEN_ENABLED) {
        otel = OpenTelemetry.propagating(ContextPropagators.create(SimplePropagator.getInstance()));
        log.info("OpenTelemetry loaded with simple propagation only.");
      }

      try {
        // throws IllegalStateException if already set
        GlobalOpenTelemetry.set(otel == null ? OpenTelemetry.noop() : otel);
        if (otel == null) {
          return; // no point in the thread local provider below
        }
      } catch (IllegalStateException e) { // deprecated; remove in Solr 11.
        // e.g. a legacy configurator that set it in init()
        log.info("GlobalOpenTelemetry was already initialized by the configurator; using that.");
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
      OpenTelemetry otel = configurator.createOpenTelemetry();
      adoptConfiguredResource(configurator);
      return otel;
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
   * Subclasses should override this; the default returns {@link #getOpenTelemetrySdk()} for
   * backwards compatibility.
   */
  protected OpenTelemetry createOpenTelemetry() {
    DeprecationLog.log(
        "createOpenTelemetry", getClass() + " should implement createOpenTelemetry()");
    return getOpenTelemetrySdk();
  }

  /**
   * The resource the configurator resolved for the OpenTelemetry it created, or {@code null} to let
   * Solr derive one from the standard {@code OTEL_*} configuration. Overriding this is what keeps
   * traces and metrics on an identical resource, including attributes contributed by {@code
   * ResourceProvider} implementations that only the SDK knows about.
   */
  protected Resource getResource() {
    return null;
  }

  /**
   * Resource attributes describing this node, for any signal Solr exports itself. Reflects {@code
   * OTEL_SERVICE_NAME} and {@code OTEL_RESOURCE_ATTRIBUTES} (or their {@code otel.*} system
   * property equivalents), with Solr supplying {@code service.name}, {@code service.instance.id}
   * and {@code host.name} where they are not configured.
   */
  public static Resource getTelemetryResource() {
    return telemetryResource;
  }

  /**
   * The {@code OTEL_METRICS_EXPORTER} the operator configured, or {@code null}. Solr's own OTLP
   * metric exporter honours it; see {@code org.apache.solr.metrics.otel.MetricExporterFactory}.
   */
  public static String getConfiguredMetricsExporter() {
    return CONFIGURED_METRICS_EXPORTER;
  }

  private static void adoptConfiguredResource(OpenTelemetryConfigurator configurator) {
    Resource configured = configurator.getResource();
    if (configured != null) {
      telemetryResource = withSolrDefaults(configured);
    }
  }

  /**
   * Builds the resource from the standard OTEL configuration. Used when no configurator supplies
   * one -- no {@code opentelemetry} module, or a Java agent owning tracing -- so that the metrics
   * endpoints still report who they belong to.
   */
  private static Resource resourceFromConfig(Map<String, String> env) {
    AttributesBuilder attrs = Attributes.builder();
    parseResourceAttributes(getConfig("OTEL_RESOURCE_ATTRIBUTES", env)).forEach(attrs::put);
    // OTEL_SERVICE_NAME wins over a service.name given among the resource attributes, per spec.
    String serviceName = getConfig("OTEL_SERVICE_NAME", env);
    if (serviceName != null) {
      attrs.put(SERVICE_NAME, serviceName);
    }
    return withSolrDefaults(Resource.getDefault().merge(Resource.create(attrs.build())));
  }

  /**
   * Names the service {@code solr} unless that is already configured. {@code host.name} and {@code
   * service.instance.id} need no equivalent here: {@link #setDefaultResourceAttributes} publishes
   * them through {@code otel.resource.attributes}, which every path reads.
   */
  private static Resource withSolrDefaults(Resource base) {
    String serviceName = base.getAttribute(SERVICE_NAME);
    if (serviceName != null && !UNKNOWN_SERVICE.equals(serviceName)) {
      return base;
    }
    return base.merge(Resource.create(Attributes.of(SERVICE_NAME, "solr")));
  }

  /**
   * Merges Solr's resource attribute defaults into {@code otel.resource.attributes}. Values already
   * configured by the operator, in either the environment or a system property, take precedence.
   */
  private static void setDefaultResourceAttributes(NodeConfig cfg, Map<String, String> env) {
    Map<String, String> attrs = new LinkedHashMap<>();
    String host = nodeHost(cfg);
    String port = nodePort(cfg);
    if (host != null) {
      attrs.put(HOST_NAME.getKey(), host);
      if (port != null) {
        attrs.put(SERVICE_INSTANCE_ID.getKey(), host + ":" + port);
      }
    }
    attrs.putAll(parseResourceAttributes(getConfig("OTEL_RESOURCE_ATTRIBUTES", env)));
    if (attrs.isEmpty()) return;
    EnvUtils.setProperty(
        "otel.resource.attributes",
        attrs.entrySet().stream()
            .map(e -> e.getKey() + "=" + e.getValue())
            .collect(Collectors.joining(",")));
  }

  /** Parses the comma separated {@code key=value} form, skipping entries that lack a value. */
  private static Map<String, String> parseResourceAttributes(String commaSeparated) {
    Map<String, String> parsed = new LinkedHashMap<>();
    if (commaSeparated == null) return parsed;
    for (String entry : commaSeparated.split(",")) {
      int eq = entry.indexOf('=');
      if (eq > 0) {
        parsed.put(entry.substring(0, eq).strip(), entry.substring(eq + 1).strip());
      }
    }
    return parsed;
  }

  private static String nodeHost(NodeConfig cfg) {
    String host =
        (cfg != null && cfg.getCloudConfig() != null) ? cfg.getCloudConfig().getHost() : null;
    if (host == null || host.isBlank()) {
      host = EnvUtils.getProperty("solr.host.advertise");
    }
    if (host == null || host.isBlank()) {
      host = AddressUtils.getHostToAdvertise();
    }
    if (host == null || host.isBlank()) return null;
    return URLUtil.hasScheme(host) ? URLUtil.removeScheme(host) : host;
  }

  private static String nodePort(NodeConfig cfg) {
    if (cfg != null && cfg.getCloudConfig() != null && cfg.getCloudConfig().getSolrHostPort() > 0) {
      return Integer.toString(cfg.getCloudConfig().getSolrHostPort());
    }
    Integer listen = EnvUtils.getPropertyAsInteger("solr.port.listen", null);
    return listen != null ? Integer.toString(listen) : null;
  }

  /**
   * @deprecated Not used by Solr; override {@link #createOpenTelemetry()} instead.
   */
  @Deprecated(since = "10.2")
  protected Tracer getTracer() {
    return null;
  }

  /**
   * @deprecated override {@link #createOpenTelemetry()} instead.
   */
  @Deprecated(since = "10.2")
  protected OpenTelemetrySdk getOpenTelemetrySdk() {
    return null;
  }

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
    telemetryResource = Resource.getDefault();
    GlobalOpenTelemetry.resetForTest();
  }
}
