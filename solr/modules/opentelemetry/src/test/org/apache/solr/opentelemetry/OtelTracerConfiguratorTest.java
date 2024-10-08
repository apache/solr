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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import io.opentracing.util.GlobalTracer;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@ThreadLeakLingering(linger = 10000)
public class OtelTracerConfiguratorTest extends SolrTestCaseJ4 {
  private OtelTracerConfigurator instance;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    instance = new OtelTracerConfigurator();
    instance.currentEnv =
        Map.of(
            "OTELNOTHERE", "foo",
            "OTEL_K1", "env-k1",
            "OTEL_K2", "env-k2");
    System.setProperty("otelnothere", "bar");
    System.setProperty("otel.k1", "prop-k1");
    System.setProperty("otel.k3", "prop-k3");
    NamedList<String> conf = new NamedList<>();
    conf.add("OTEL_K1", "conf-k1");
    conf.add("otel.k7", "conf-k7");
    instance.init(conf);

    // to be safe because this test tests tracing.
    resetGlobalTracer();
    ExecutorUtil.resetThreadLocalProviders();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    System.clearProperty("otelnothere");
    System.clearProperty("otel.k1");
    System.clearProperty("otel.k3");
    System.clearProperty("otel.bsp.export.timeout");
  }

  @Test
  public void testGetCurrentOtelConfig() {
    Map<String, String> expected =
        Map.of(
            "OTEL_K1", "prop-k1",
            "OTEL_K2", "env-k2",
            "OTEL_K3", "prop-k3",
            "OTEL_K7", "conf-k7");
    assertEquals(expected, instance.getCurrentOtelConfig());
  }

  @Test
  public void testGetCurrentOtelConfigAsString() {
    assertEquals(
        "OTEL_K1=prop-k1; OTEL_K2=env-k2; OTEL_K3=prop-k3; OTEL_K7=conf-k7",
        instance.getCurrentOtelConfigAsString());
  }

  @Test
  public void testGetEnvOrSysprop() {
    assertEquals("prop-k1", instance.getEnvOrSysprop("OTEL_K1"));
    assertEquals("env-k2", instance.getEnvOrSysprop("OTEL_K2"));
    assertNull(instance.getEnvOrSysprop("NOTEXIST"));
  }

  @Test
  public void testSetDefaultIfNotConfigured() {
    instance.setDefaultIfNotConfigured("OTEL_K2", "default");
    instance.setDefaultIfNotConfigured("OTEL_YEY", "default");
    assertEquals("default", instance.getCurrentOtelConfig().get("OTEL_YEY"));
    assertEquals("prop-k1", instance.getCurrentOtelConfig().get("OTEL_K1"));
  }

  @Test
  public void testInjected() throws Exception {
    System.setProperty("otel.resource.attributes", "foo=bar,ILLEGAL-LACKS-VALUE,");
    System.setProperty("host", "my.solr.host");
    // Make sure the batch exporter times out before our thread lingering time of 10s
    instance.setDefaultIfNotConfigured("OTEL_BSP_SCHEDULE_DELAY", "1000");
    instance.setDefaultIfNotConfigured("OTEL_BSP_EXPORT_TIMEOUT", "2000");
    MiniSolrCloudCluster cluster =
        new MiniSolrCloudCluster.Builder(2, createTempDir())
            .addConfig("config", TEST_PATH().resolve("collection1").resolve("conf"))
            .withSolrXml(getFile("solr/solr.xml").toPath())
            .withTraceIdGenerationDisabled()
            .build();
    try {
      assertTrue(
          "Tracer shim not registered with GlobalTracer",
          GlobalTracer.get().toString().contains("ClosableTracerShim"));
      assertEquals(
          List.of("host.name=my.solr.host", "foo=bar"),
          List.of(System.getProperty("otel.resource.attributes").split(",")));
    } finally {
      cluster.shutdown();
      System.clearProperty("otel.resource.attributes");
      System.clearProperty("host");
    }
  }
}
