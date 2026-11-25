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
package org.apache.solr.metrics.otel;

import java.lang.reflect.Method;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.SuppressForbidden;
import org.junit.Before;
import org.junit.Test;

/**
 * Precedence of Solr's OTLP metric exporter settings over the standard OTEL ones. The constants are
 * resolved once per JVM, so this calls the resolvers directly.
 */
public class MetricExporterFactoryTest extends SolrTestCase {

  @Before
  public void requireCleanEnv() {
    for (String env :
        new String[] {
          "OTEL_METRICS_EXPORTER",
          "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
          "OTEL_EXPORTER_OTLP_PROTOCOL",
          "OTEL_METRIC_EXPORT_INTERVAL",
          "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
          "OTEL_EXPORTER_OTLP_ENDPOINT"
        }) {
      assumeTrue(env + " is set in the environment", System.getenv(env) == null);
    }
  }

  @Test
  public void testEnabled() throws Exception {
    assertEquals(false, invoke("resolveEnabled"));
    System.setProperty("otel.metrics.exporter", "otlp");
    assertEquals(true, invoke("resolveEnabled"));
    System.setProperty("otel.metrics.exporter", "prometheus,otlp");
    assertEquals(true, invoke("resolveEnabled"));
    System.setProperty("otel.metrics.exporter", "prometheus");
    assertEquals(false, invoke("resolveEnabled"));
  }

  @Test
  public void testExplicitSolrDisableWins() throws Exception {
    System.setProperty("otel.metrics.exporter", "otlp");
    System.setProperty("solr.metrics.otlpExporterEnabled", "false");
    assertEquals(false, invoke("resolveEnabled"));
  }

  @Test
  public void testProtocol() throws Exception {
    assertEquals("http", invoke("resolveProtocol"));
    System.setProperty("otel.exporter.otlp.protocol", "grpc");
    assertEquals("grpc", invoke("resolveProtocol"));
    System.setProperty("otel.exporter.otlp.metrics.protocol", "http/protobuf");
    assertEquals("http", invoke("resolveProtocol"));
    // Solr sends protobuf only, so this must reach the exporter factory, which rejects it
    System.setProperty("otel.exporter.otlp.metrics.protocol", "http/json");
    assertEquals("http/json", invoke("resolveProtocol"));
    System.setProperty("solr.metrics.otlpExporterProtocol", "grpc");
    assertEquals("grpc", invoke("resolveProtocol"));
  }

  @Test
  public void testInterval() throws Exception {
    assertEquals(60000, invoke("resolveInterval"));
    System.setProperty("otel.metric.export.interval", "15000");
    assertEquals(15000, invoke("resolveInterval"));
    System.setProperty("solr.metrics.otlpExporterInterval", "5000");
    assertEquals(5000, invoke("resolveInterval"));
  }

  @Test
  public void testIntervalUnits() throws Exception {
    for (Map.Entry<String, Integer> e :
        Map.of("250ms", 250, "30s", 30_000, " 2 m ", 120_000, "1h", 3_600_000, "1d", 86_400_000)
            .entrySet()) {
      System.setProperty("otel.metric.export.interval", e.getKey());
      assertEquals(e.getKey(), e.getValue(), invoke("resolveInterval"));
    }
  }

  /** A bad interval must not stop the node from starting. */
  @Test
  public void testInvalidIntervalFallsBackToDefault() throws Exception {
    for (String invalid : new String[] {"abc", "30x", "0", "-5", "s", "99999999999999999999"}) {
      System.setProperty("otel.metric.export.interval", invalid);
      assertEquals(invalid, 60000, invoke("resolveInterval"));
    }
  }

  /** The SDK reads a blank variable as unset. */
  @Test
  public void testBlankOtelValuesAreUnset() throws Exception {
    System.setProperty("otel.metric.export.interval", "");
    System.setProperty("otel.exporter.otlp.protocol", " ");
    System.setProperty("otel.exporter.otlp.endpoint", "");
    System.setProperty("otel.exporter.otlp.metrics.endpoint", "");
    System.setProperty("otel.metrics.exporter", "");
    assertEquals(60000, invoke("resolveInterval"));
    assertEquals("http", invoke("resolveProtocol"));
    assertEquals("http://localhost:4318/v1/metrics", httpEndpoint());
    assertEquals(false, invoke("resolveEnabled"));
  }

  @Test
  public void testEndpoints() throws Exception {
    assertEquals("http://localhost:4318/v1/metrics", httpEndpoint());
    assertEquals("http://localhost:4317", grpcEndpoint());

    System.setProperty("otel.exporter.otlp.endpoint", "http://collector:4318/");
    assertEquals("http://collector:4318/v1/metrics", httpEndpoint());
    assertEquals("http://collector:4318/", grpcEndpoint());

    System.setProperty("otel.exporter.otlp.metrics.endpoint", "http://metrics:9000/custom");
    assertEquals("http://metrics:9000/custom", httpEndpoint());

    System.setProperty("solr.metrics.otlpHttpExporterEndpoint", "http://solr:1234/x");
    assertEquals("http://solr:1234/x", httpEndpoint());
  }

  private static String httpEndpoint() throws Exception {
    return (String)
        invoke(
            "resolveEndpoint",
            new Class<?>[] {String.class, boolean.class},
            "solr.metrics.otlpHttpExporterEndpoint",
            true);
  }

  private static String grpcEndpoint() throws Exception {
    return (String)
        invoke(
            "resolveEndpoint",
            new Class<?>[] {String.class, boolean.class},
            "solr.metrics.otlpGrpcExporterEndpoint",
            false);
  }

  private static Object invoke(String name) throws Exception {
    return invoke(name, new Class<?>[0]);
  }

  @SuppressForbidden(reason = "The resolvers are private interface methods")
  private static Object invoke(String name, Class<?>[] types, Object... args) throws Exception {
    Method m = MetricExporterFactory.class.getDeclaredMethod(name, types);
    m.setAccessible(true);
    return m.invoke(null, args);
  }
}
