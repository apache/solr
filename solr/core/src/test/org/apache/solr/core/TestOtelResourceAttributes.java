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

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.resources.Resource;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Resource attributes Solr resolves for the telemetry it exports itself. */
public class TestOtelResourceAttributes extends SolrTestCase {

  private static final AttributeKey<String> SERVICE_NAME = AttributeKey.stringKey("service.name");
  private static final AttributeKey<String> SERVICE_INSTANCE_ID =
      AttributeKey.stringKey("service.instance.id");
  private static final AttributeKey<String> HOST_NAME = AttributeKey.stringKey("host.name");

  @Before
  @After
  public void clearOtelConfig() {
    System.clearProperty("otel.service.name");
    System.clearProperty("otel.resource.attributes");
    OpenTelemetryConfigurator.resetForTest();
  }

  private Resource initialize(NodeConfig cfg) {
    OpenTelemetryConfigurator.initializeOpenTelemetrySdk(
        cfg, new SolrResourceLoader(createTempDir()));
    return OpenTelemetryConfigurator.getTelemetryResource();
  }

  private NodeConfig nodeConfig(String host, int port) {
    return new NodeConfig.NodeConfigBuilder("testNode", createTempDir())
        .setCloudConfig(new CloudConfig.CloudConfigBuilder(host, port).build())
        .build();
  }

  @Test
  public void solrSuppliesDefaults() {
    Resource resource = initialize(nodeConfig("solr-host-1", 8983));
    assertEquals("solr", resource.getAttribute(SERVICE_NAME));
    assertEquals("solr-host-1", resource.getAttribute(HOST_NAME));
    assertEquals("solr-host-1:8983", resource.getAttribute(SERVICE_INSTANCE_ID));
  }

  /** Without a cloud config there is no advertised port, but the node still names itself. */
  @Test
  public void defaultsWithoutCloudConfig() {
    Resource resource = initialize(null);
    assertEquals("solr", resource.getAttribute(SERVICE_NAME));
    assertNotNull(resource.getAttribute(HOST_NAME));
  }

  @Test
  public void configuredAttributesWin() {
    System.setProperty("otel.service.name", "my-solr");
    System.setProperty(
        "otel.resource.attributes",
        "service.instance.id=node-9,host.name=my.host,deployment.environment.name=staging");

    Resource resource = initialize(nodeConfig("solr-host-1", 8983));
    assertEquals("my-solr", resource.getAttribute(SERVICE_NAME));
    assertEquals("node-9", resource.getAttribute(SERVICE_INSTANCE_ID));
    assertEquals("my.host", resource.getAttribute(HOST_NAME));
    assertEquals(
        "staging", resource.getAttribute(AttributeKey.stringKey("deployment.environment.name")));
  }

  /** {@code OTEL_SERVICE_NAME} outranks a {@code service.name} among the resource attributes. */
  @Test
  public void serviceNamePropertyOutranksResourceAttribute() {
    System.setProperty("otel.service.name", "from-service-name");
    System.setProperty("otel.resource.attributes", "service.name=from-attributes");
    assertEquals("from-service-name", initialize(nodeConfig("h", 8983)).getAttribute(SERVICE_NAME));
  }

  /** Entries without a value are skipped rather than failing the whole resource. */
  @Test
  public void malformedAttributesAreSkipped() {
    System.setProperty("otel.resource.attributes", "foo=bar,ILLEGAL-LACKS-VALUE,");
    Resource resource = initialize(nodeConfig("h", 8983));
    assertEquals("bar", resource.getAttribute(AttributeKey.stringKey("foo")));
    assertEquals("solr", resource.getAttribute(SERVICE_NAME));
  }

  /** A configurator that resolves its own resource owns it, so both signals share one. */
  @Test
  public void configuratorResourceIsAdopted() {
    NodeConfig cfg =
        new NodeConfig.NodeConfigBuilder("testNode", createTempDir())
            .setTracerConfig(
                new PluginInfo("tracerConfig", Map.of("class", WithResource.class.getName())))
            .build();
    Resource resource = initialize(cfg);
    assertEquals("from-sdk", resource.getAttribute(SERVICE_NAME));
    assertEquals("sdk-detected", resource.getAttribute(HOST_NAME));
  }

  /** Even then Solr names the service, since the SDK's own fallback is not a useful job label. */
  @Test
  public void configuratorResourceStillGetsServiceName() {
    NodeConfig cfg =
        new NodeConfig.NodeConfigBuilder("testNode", createTempDir())
            .setTracerConfig(
                new PluginInfo(
                    "tracerConfig", Map.of("class", WithDefaultResource.class.getName())))
            .build();
    assertEquals("solr", initialize(cfg).getAttribute(SERVICE_NAME));
  }

  public static class WithResource extends OpenTelemetryConfigurator {
    @Override
    protected OpenTelemetry createOpenTelemetry() {
      return OpenTelemetry.propagating(
          ContextPropagators.create(W3CTraceContextPropagator.getInstance()));
    }

    @Override
    protected Resource getResource() {
      return Resource.create(Attributes.of(SERVICE_NAME, "from-sdk", HOST_NAME, "sdk-detected"));
    }
  }

  public static class WithDefaultResource extends WithResource {
    @Override
    protected Resource getResource() {
      return Resource.getDefault();
    }
  }
}
