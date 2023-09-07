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

package org.apache.solr.util.tracing;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.solr.security.Sha256AuthenticationProvider.getSaltedHashedValue;

import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.GlobalTracer;
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
import org.apache.solr.security.RuleBasedAuthorizationPlugin;
import org.apache.solr.util.LogLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@LogLevel("org.apache.solr.core.TracerConfigurator=trace")
public class BasicAuthIntegrationTracingTest extends SolrCloudTestCase {

  static MockTracer tracer;

  private static final String COLLECTION = "collection1";
  private static final String USER = "solr";
  private static final String PASS = "SolrRocksAgain";
  private static final String SECURITY_JSON =
      Utils.toJSONString(
          Map.of(
              "authorization",
              Map.of(
                  "class",
                  RuleBasedAuthorizationPlugin.class.getName(),
                  "user-role",
                  singletonMap(USER, "admin"),
                  "permissions",
                  singletonList(Map.of("name", "all", "role", "admin"))),
              "authentication",
              Map.of(
                  "class",
                  BasicAuthPlugin.class.getName(),
                  "blockUnknown",
                  true,
                  "credentials",
                  singletonMap(USER, getSaltedHashedValue(PASS)))));

  @BeforeClass
  public static void beforeTest() throws Exception {
    tracer = new MockTracer();
    assertTrue(GlobalTracer.registerIfAbsent(tracer));

    configureCluster(4)
        .addConfig(
            "config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .withSecurityJson(SECURITY_JSON)
        .withTraceIdGenerationDisabled()
        .configure();
    CollectionAdminRequest.createCollection(COLLECTION, "config", 2, 2)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .setBasicAuthCredentials(USER, PASS)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 2, 4);
  }

  @AfterClass
  public static void afterTest() {
    tracer = null;
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
    req.setBasicAuthCredentials(USER, PASS);
    assertEquals(0, req.process(cloudClient, COLLECTION).getStatus());

    var finishedSpans = getAndClearSpans();
    assertEquals(1, finishedSpans.size());
    var span = finishedSpans.get(0);
    assertEquals("post:/cluster/security/authentication", span.operationName());
    assertEquals("solr", span.tags().get("db.user"));
    assertEquals(BasicAuthPlugin.class.getSimpleName(), span.tags().get("class"));
    assertEquals(String.join(",", ops.keySet()), span.tags().get("ops"));
  }

  private List<MockSpan> getAndClearSpans() {
    List<MockSpan> result = tracer.finishedSpans(); // returns a mutable copy
    Collections.reverse(result); // nicer to see spans chronologically
    tracer.reset();
    return result;
  }
}
