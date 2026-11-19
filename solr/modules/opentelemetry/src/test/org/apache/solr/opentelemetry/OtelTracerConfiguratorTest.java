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

import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.junit.Before;
import org.junit.Test;

public class OtelTracerConfiguratorTest extends SolrTestCaseJ4 {
  private OtelTracerConfigurator instance;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    Map<String, String> currentEnv =
        Map.of(
            "OTELNOTHERE", "foo",
            "OTEL_K1", "env-k1",
            "OTEL_K2", "env-k2");
    System.clearProperty("otel.service.name");
    System.clearProperty("otel.resource.attributes");
    System.setProperty("otelnothere", "bar");
    System.setProperty("otel.k1", "prop-k1");
    System.setProperty("otel.k3", "prop-k3");
    instance = new OtelTracerConfigurator(currentEnv);
  }

  @Test
  public void testGetCurrentOtelConfig() {
    Map<String, String> expected =
        Map.of(
            "OTEL_K1", "prop-k1",
            "OTEL_K2", "env-k2",
            "OTEL_K3", "prop-k3");
    assertEquals(expected, instance.getCurrentOtelConfig());
  }

  @Test
  public void testGetCurrentOtelConfigAsString() {
    assertEquals(
        "OTEL_K1=prop-k1; OTEL_K2=env-k2; OTEL_K3=prop-k3",
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
  public void testServiceNameDefault() {
    instance.prepareConfiguration(new NamedList<>());
    assertEquals("solr", instance.getCurrentOtelConfig().get("OTEL_SERVICE_NAME"));
  }

  /** The SDK ranks OTEL_SERVICE_NAME above this attribute, so the default must stand down. */
  @Test
  public void testServiceNameFromResourceAttributes() {
    System.setProperty("otel.resource.attributes", "service.name=search,foo=bar");
    instance.prepareConfiguration(new NamedList<>());
    assertNull(instance.getCurrentOtelConfig().get("OTEL_SERVICE_NAME"));
  }

  @Test
  public void testServiceNamePropertyOutranksResourceAttribute() {
    System.setProperty("otel.service.name", "configured");
    System.setProperty("otel.resource.attributes", "service.name=search");
    instance.prepareConfiguration(new NamedList<>());
    assertEquals("configured", instance.getCurrentOtelConfig().get("OTEL_SERVICE_NAME"));
  }

  @Test
  public void testPluginConfig() throws Exception {
    NamedList<String> conf = new NamedList<>();
    conf.add("OTEL_K1", "conf-k1"); // will be replaced by sys prop
    conf.add("otel.k7", "conf-k7"); // will be kept
    instance.prepareConfiguration(conf);
    assertEquals("prop-k1", instance.getCurrentOtelConfig().get("OTEL_K1"));
    assertEquals("conf-k7", instance.getCurrentOtelConfig().get("OTEL_K7"));
  }
}
