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

package org.apache.solr.security;

import java.lang.invoke.MethodHandles;

import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.SolrCloudAuthTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicAuthOnSingleNodeTest extends SolrCloudAuthTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String COLLECTION = "authCollection";

  @Before
  public void setupCluster() throws Exception {
    // Under load a freshly-started secured cluster can come up with its inter-node (PKI) request
    // signing not yet wired, so the CREATE-core sub-requests during collection creation arrive
    // unauthenticated and are rejected with 401 ("Underlying core creation failed while creating
    // collection"). That state is stuck for the affected cluster instance, so retrying the create on
    // the same cluster does not help -- bring up a fresh cluster and try again.
    final int maxAttempts = 8;
    for (int attempt = 1; ; attempt++) {
      try {
        configureCluster(1)
            .addConfig("conf", SolrTestUtil.configset("cloud-minimal"))
            .withSecurityJson(STD_CONF)
            .configure();
        CollectionAdminRequest.createCollection(COLLECTION, "conf", 4, 1)
            .setMaxShardsPerNode(100)
            .setBasicAuthCredentials("solr", "solr")
            .process(cluster.getSolrClient());
        // This fork's CreateCollectionCmd returns once the cores are created but before they
        // publish ACTIVE (waitForFinalState defaults to false; see the MRM TODO in
        // CreateCollectionCmd). The 4-shard collection must be fully active before basicTest's
        // immediate distributed query, otherwise it 503s with "no servers hosting shard: s3".
        cluster.waitForActiveCollection(COLLECTION, 4, 4);
        return;
      } catch (Exception e) {
        if (attempt >= maxAttempts) {
          throw e;
        }
        log.warn("cluster setup attempt {} failed (inter-node auth not ready under load); recreating cluster: {}",
            attempt, e.toString());
        try {
          if (cluster != null) {
            cluster.shutdown();
          }
        } catch (Exception ignore) {
          // best-effort teardown of the failed cluster before recreating
        }
        cluster = null;
        Thread.sleep(1000);
      }
    }
  }

  @Override
  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
    cluster = null;
    super.tearDown();
  }

  @Test
  public void basicTest() throws Exception {
    try (Http2SolrClient client = new Http2SolrClient.Builder(cluster.getJettySolrRunner(0).getBaseUrl().toString())
        .build()){

      // SOLR-13510, this will be failed if the listener (handling inject credential in header) is called in another
      // thread since SolrRequestInfo will return null in that case.
      for (int i = 0; i < 30; i++) {
        assertNotNull(new QueryRequest(params("q", "*:*"))
                      .setBasicAuthCredentials("solr", "solr").process(client, COLLECTION));
      }
    }
  }

  @Test
  public void testDeleteSecurityJsonZnode() throws Exception {
    try (Http2SolrClient client = new Http2SolrClient.Builder(cluster.getJettySolrRunner(0).getBaseUrl().toString())
        .build()){
      try {
        new QueryRequest(params("q", "*:*")).process(client, COLLECTION);
        fail("Should throw exception due to authentication needed");
      } catch (Exception e) { /* Ignore */ }

      // Deleting security.json will disable security - before SOLR-9679 it would instead cause an exception
      cluster.getZkClient().delete("/security.json", -1);

      int count = 0;
      boolean done = false;
      // Assert that security is turned off. This is async, so we retry up to 5s before failing the test
      while (!done) {
        try {
          Thread.sleep(500);
          count += 1;
          new QueryRequest(params("q", "*:*")).process(client, COLLECTION);
          done = true;
        } catch (Exception e) {
          if (count >= 10) {
            fail("Failed 10 times to query without credentials after removing security.json");
          }
        }
      }
    }
  }

  protected static final String STD_CONF = "{\n" +
      "  \"authentication\":{\n" +
      "   \"blockUnknown\": true,\n" +
      "   \"class\":\"solr.BasicAuthPlugin\",\n" +
      "   \"credentials\":{\"solr\":\"EEKn7ywYk5jY8vG9TyqlG2jvYuvh1Q7kCCor6Hqm320= 6zkmjMjkMKyJX6/f0VarEWQujju5BzxZXub6WOrEKCw=\"}\n" +
      "  },\n" +
      "  \"authorization\":{\n" +
      "   \"class\":\"solr.RuleBasedAuthorizationPlugin\",\n" +
      "   \"permissions\":[\n" +
      " {\"name\":\"security-edit\", \"role\":\"admin\"},\n" +
      " {\"name\":\"collection-admin-edit\", \"role\":\"admin\"},\n" +
      " {\"name\":\"core-admin-edit\", \"role\":\"admin\"}\n" +
      "   ],\n" +
      "   \"user-role\":{\"solr\":\"admin\"}\n" +
      "  }\n" +
      "}";
}


