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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.sdk.testing.junit4.OpenTelemetryRule;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.Utils;
import org.apache.solr.security.BasicAuthPlugin;
import org.apache.solr.util.SecurityJson;
import org.apache.solr.util.tracing.TraceUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class BasicAuthIntegrationTracingTest extends SolrCloudTestCase {

  private static final String COLLECTION = "collection1";

  @ClassRule public static OpenTelemetryRule otelRule = OpenTelemetryRule.create();

  @BeforeClass
  public static void setupCluster() throws Exception {
    TraceUtils.resetRecordingFlag(); // ensure spans are "recorded"

    configureCluster(4)
        .addConfig("config", TEST_PATH().resolve("collection1").resolve("conf"))
        .withSolrXml(TEST_PATH().resolve("solr.xml"))
        .withSecurityJson(SecurityJson.SIMPLE)
        .configure();

    assertNotEquals(
        "Expecting active otel, not noop impl",
        TracerProvider.noop(),
        GlobalOpenTelemetry.get().getTracerProvider());

    CollectionAdminRequest.createCollection(COLLECTION, "config", 2, 2)
        .setBasicAuthCredentials(SecurityJson.USER, SecurityJson.PASS)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 2, 4);
  }

  /** See SOLR-16955 */
  @Test
  public void testSetupBasicAuth() throws Exception {
    getAndClearSpans(); // reset

    CloudSolrClient cloudClient = cluster.getSolrClient();
    Map<String, Object> ops =
        Map.of(
            "set-user", Map.of("harry", "HarryIsCool"),
            "set-property", Map.of("blockUnknown", true));
    V2Request req =
        new V2Request.Builder("/cluster/security/authentication")
            .withMethod(SolrRequest.METHOD.POST)
            .withPayload(Utils.toJSONString(ops))
            .build();
    req.setBasicAuthCredentials(SecurityJson.USER, SecurityJson.PASS);
    assertEquals(0, req.process(cloudClient, COLLECTION).getStatus());

    var finishedSpans = getAndClearSpans();
    assertEquals(1, finishedSpans.size());
    var span = finishedSpans.get(0);
    assertEquals("post:/cluster/security/authentication", span.getName());
    assertEquals("solr", span.getAttributes().get(TraceUtils.TAG_USER));
    assertEquals(
        BasicAuthPlugin.class.getSimpleName(), span.getAttributes().get(TraceUtils.TAG_CLASS));
    assertEquals(List.copyOf(ops.keySet()), span.getAttributes().get(TraceUtils.TAG_OPS));
  }

  // code duplication here...
  static List<SpanData> getAndClearSpans() {
    List<SpanData> result = new ArrayList<>(otelRule.getSpans());
    Collections.reverse(result); // nicer to see spans chronologically
    otelRule.clearSpans();
    return result;
  }
}
