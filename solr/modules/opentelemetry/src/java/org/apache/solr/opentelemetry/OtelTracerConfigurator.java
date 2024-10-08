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
package org.apache.solr.opentelemetry;

import io.opentelemetry.opentracingshim.OpenTracingShim;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.opentracing.Tracer;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.TracerConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OpenTracing TracerConfigurator implementation which exports spans to OpenTelemetry in OTLP
 * format. This impl re-uses the existing OpenTracing instrumentation through a shim, and takes care
 * of properly closing the backing Tracer when Solr shuts down.
 */
public class OtelTracerConfigurator extends TracerConfigurator {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  // Copy of environment. Can be overridden by tests
  Map<String, String> currentEnv = System.getenv();

  @Override
  public void init(NamedList<?> args) {
    injectPluginSettingsIfNotConfigured(args);
  }

  @Override
  public Tracer getTracer() {
    setDefaultIfNotConfigured("OTEL_SERVICE_NAME", "solr");
    setDefaultIfNotConfigured("OTEL_TRACES_EXPORTER", "otlp");
    setDefaultIfNotConfigured("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc");
    setDefaultIfNotConfigured("OTEL_TRACES_SAMPLER", "parentbased_always_on");
    if (System.getProperty("host") != null) {
      addOtelResourceAttributes(Map.of("host.name", System.getProperty("host")));
    }

    final String currentConfig = getCurrentOtelConfigAsString();
    log.info("OpenTelemetry tracer enabled with configuration: {}", currentConfig);

    // Need to disable the exporters for metrics and logs
    String metricsExporter = getEnvOrSysprop("OTEL_METRICS_EXPORTER");
    String logsExporter = getEnvOrSysprop("OTEL_LOGS_EXPORTER");
    if ((metricsExporter != null && !Objects.equals(metricsExporter, "none"))
        || (logsExporter != null && !Objects.equals(logsExporter, "none"))) {
      log.warn(
          "The opentelemetry module does not support METRICS or LOGS. Ignoring faulty environment setting");
    }
    System.setProperty("otel.metrics.exporter", "none");
    System.setProperty("otel.logs.exporter", "none");

    OpenTelemetrySdk otelSdk = AutoConfiguredOpenTelemetrySdk.initialize().getOpenTelemetrySdk();
    Tracer shim = OpenTracingShim.createTracerShim(otelSdk);
    return new ClosableTracerShim(shim, otelSdk.getSdkTracerProvider());
  }

  /**
   * Will inject plugin configuration values into system properties if not already setup (existing
   * system properties take precedence)
   */
  private void injectPluginSettingsIfNotConfigured(NamedList<?> args) {
    args.forEach(
        (k, v) -> {
          var asSysName = envNameToSyspropName(k);
          if (asSysName.startsWith("otel.")) {
            setDefaultIfNotConfigured(asSysName, v.toString());
          }
        });
  }

  /**
   * Add explicit tags statically to all traces, independent of request. Attributes with same name
   * supplied in ENV or SysProp will take precedence over attributes added in code.
   */
  void addOtelResourceAttributes(Map<String, String> attrsToAdd) {
    String commaSepAttrs = getEnvOrSysprop("OTEL_RESOURCE_ATTRIBUTES");
    Map<String, String> attrs = new HashMap<>(attrsToAdd);
    if (commaSepAttrs != null) {
      attrs.putAll(
          Arrays.stream(commaSepAttrs.split(","))
              .filter(e -> e.contains("="))
              .map(e -> e.strip().split("="))
              .collect(Collectors.toMap(kv -> kv[0], kv -> kv[1])));
    }
    System.setProperty(
        envNameToSyspropName("OTEL_RESOURCE_ATTRIBUTES"),
        attrs.entrySet().stream()
            .map(e -> e.getKey() + "=" + e.getValue())
            .collect(Collectors.joining(",")));
  }

  /** Prepares a string with all configuration K/V pairs sorted and semicolon separated */
  String getCurrentOtelConfigAsString() {
    return getCurrentOtelConfig().entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(e -> e.getKey() + "=" + e.getValue())
        .collect(Collectors.joining("; "));
  }

  /**
   * Finds all configuration based on environment with prefix <code>OTEL_</code> or System
   * Properties with prefix <code>otel.</code>.
   *
   * @return a unified map of config, using the ENV_VAR as keys, even if the config was pulled from
   *     a system property.
   */
  Map<String, String> getCurrentOtelConfig() {
    HashMap<String, String> currentConfig = new HashMap<>();
    currentEnv.entrySet().stream()
        .filter(e -> e.getKey().startsWith("OTEL_"))
        .forEach(entry -> currentConfig.put(entry.getKey(), entry.getValue()));
    System.getProperties().entrySet().stream()
        .filter(e -> e.getKey().toString().startsWith("otel."))
        .forEach(
            entry -> {
              String key = entry.getKey().toString();
              String envKey = key.toUpperCase(Locale.ROOT).replace('.', '_');
              String value = entry.getValue().toString();
              currentConfig.put(envKey, value);
            });
    return currentConfig;
  }

  /**
   * Returns system property if found, else returns environment variable, or null if none found.
   *
   * @param envName the environment variable to look for
   * @return the resolved value
   */
  String getEnvOrSysprop(String envName) {
    return getConfig(envName, currentEnv);
  }

  /**
   * First checks if the property is defined in environment or properties. If not, we set it as a
   * property.
   *
   * @param envName environment variable name, should start with OTEL_
   * @param defaultValue the value to set if not already configured
   */
  void setDefaultIfNotConfigured(String envName, String defaultValue) {
    String incomingValue = getEnvOrSysprop(envName);
    if (incomingValue == null) {
      System.setProperty(envNameToSyspropName(envName), defaultValue);
      if (log.isDebugEnabled()) {
        log.debug("Using default setting {}={}", envName, getEnvOrSysprop(envName));
      }
    }
  }
}
