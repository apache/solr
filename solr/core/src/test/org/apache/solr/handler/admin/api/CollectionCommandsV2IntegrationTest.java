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

package org.apache.solr.handler.admin.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.client.solrj.response.json.JsonMapResponseParser;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.RoutingRule;
import org.apache.solr.common.cloud.Slice;
import org.junit.BeforeClass;
import org.junit.Test;

/** Drives the JAX-RS collection command APIs over HTTP against a running SolrCloud cluster. */
public class CollectionCommandsV2IntegrationTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @Test
  public void testRebalanceLeaders() throws Exception {
    final String collection = "v2RebalanceLeaders";
    CollectionAdminRequest.createCollection(collection, "conf", 2, 2)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 2, 4);

    final Map<String, String> preferredLeaders = new HashMap<>();
    for (Slice slice : getCollectionState(collection).getSlices()) {
      final Replica preferred =
          slice.getReplicas().stream()
              .filter(replica -> !replica.getName().equals(slice.getLeader().getName()))
              .findFirst()
              .orElseThrow();
      preferredLeaders.put(slice.getName(), preferred.getName());
      CollectionAdminRequest.addReplicaProperty(
              collection, slice.getName(), preferred.getName(), "preferredLeader", "true")
          .process(cluster.getSolrClient());
    }
    waitForState(
        "Expected preferred leader properties before rebalancing",
        collection,
        c ->
            c != null
                && preferredLeaders.entrySet().stream()
                    .allMatch(
                        entry ->
                            c.getSlice(entry.getKey())
                                .getReplica(entry.getValue())
                                .getBool("property.preferredleader", false)));

    V2Response response =
        postV2(
            "/collections/" + collection + "/rebalance-leaders",
            "{\"maxAtOnce\": 2, \"maxWaitSeconds\": 30}");

    assertEquals(
        Map.of("Success", "All active replicas with the preferredLeader property set are leaders"),
        response.getResponse().get("Summary"));
    assertNotNull(response.getResponse().get("successes"));
    assertEquals(
        preferredLeaders.keySet(),
        ((Map<?, ?>) response.getResponse().get("successes")).keySet());
    waitForState(
        "Expected every preferred replica to become leader",
        collection,
        c ->
            c != null
                && preferredLeaders.entrySet().stream()
                    .allMatch(
                        entry -> {
                          final Replica leader = c.getSlice(entry.getKey()).getLeader();
                          return leader != null && entry.getValue().equals(leader.getName());
                        }));
  }

  @Test
  public void testMigrateDocs() throws Exception {
    final String source = "v2MigrateSource";
    final String target = "v2MigrateTarget";
    createMigrationCollections(source, target);

    postV2(
        "/collections/" + source + "/migrate",
        "{\"target\": \"" + target + "\", \"splitKey\": \"a!\", \"forwardTimeout\": 45}");

    assertMigrationResult(source, target);
  }

  @Test
  public void testMigrateDocsAsyncWithAliases() throws Exception {
    final String source = "v2MigrateAsyncSource";
    final String target = "v2MigrateAsyncTarget";
    final String sourceAlias = "v2MigrateSourceAlias";
    final String targetAlias = "v2MigrateTargetAlias";
    final String asyncId = "v2MigrateAsyncId";
    createMigrationCollections(source, target);
    CollectionAdminRequest.createAlias(sourceAlias, source).process(cluster.getSolrClient());
    CollectionAdminRequest.createAlias(targetAlias, target).process(cluster.getSolrClient());

    final var response =
        postV2(
            "/collections/" + sourceAlias + "/migrate",
            "{\"target\": \""
                + targetAlias
                + "\", \"splitKey\": \"a!\", \"forwardTimeout\": 45,"
                + " \"followAliases\": true, \"async\": \""
                + asyncId
                + "\"}");

    assertEquals(asyncId, response.getResponse().get("requestid"));
    assertEquals(
        RequestStatusState.COMPLETED,
        CollectionAdminRequest.requestStatus(asyncId).waitFor(cluster.getSolrClient(), 120));
    assertMigrationResult(source, target);
  }

  private void createMigrationCollections(String source, String target) throws Exception {
    CollectionAdminRequest.createCollection(source, "conf", 1, 1).process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection(target, "conf", 1, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection(source, 1, 1);
    cluster.waitForActiveCollection(target, 1, 1);
    cluster.getSolrClient().add(source, new SolrInputDocument("id", "a!1"));
    cluster.getSolrClient().add(source, new SolrInputDocument("id", "b!1"));
    cluster.getSolrClient().add(target, new SolrInputDocument("id", "existing!1"));
    cluster.getSolrClient().commit(source);
    cluster.getSolrClient().commit(target);
  }

  private void assertMigrationResult(String source, String target) throws Exception {
    waitForState(
        "Expected a routing rule for split key a!",
        source,
        c -> {
          if (c == null) {
            return false;
          }
          Map<String, RoutingRule> rules = c.getSlice("shard1").getRoutingRules();
          return rules != null && rules.containsKey("a!");
        });

    assertEquals(
        target,
        getCollectionState(source)
            .getSlice("shard1")
            .getRoutingRules()
            .get("a!")
            .getTargetCollectionName());
    cluster.getSolrClient().commit(target);
    final var documents =
        cluster
            .getSolrClient()
            .query(target, new SolrQuery("*:*").setFields("id"))
            .getResults();
    assertEquals(2, documents.getNumFound());
    assertEquals(
        Set.of("a!1", "existing!1"),
        documents.stream().map(document -> document.getFieldValue("id")).collect(Collectors.toSet()));
  }

  private static V2Response postV2(String path, String payload) throws Exception {
    V2Request request =
        new V2Request.Builder(path)
            .withMethod(SolrRequest.METHOD.POST)
            .withPayload(payload)
            .build();
    request.setResponseParser(new JsonMapResponseParser());
    V2Response response = request.process(cluster.getSolrClient());
    assertEquals(0, response.getStatus());
    return response;
  }
}
