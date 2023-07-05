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
package org.apache.solr.search.join;

import static java.util.Collections.singletonMap;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.cluster.placement.plugins.AffinityPlacementConfig;
import org.apache.solr.cluster.placement.plugins.StubShardAffinityPlacementFactory;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests using fromIndex that points to a collection in SolrCloud mode. */
// @LogLevel("org.apache.solr.schema.IndexSchema=TRACE")
public class ShardToShardJoinAbstract extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String[] scoreModes = {"avg", "max", "min", "total"};

  //    resetExceptionIgnores();
  protected static String toColl = "parent";
  protected static String fromColl = "children";

  @BeforeClass
  public static void setPropos() {
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
  }

  public static void setupCluster(
      Consumer<CollectionAdminRequest.Create> fromDecorator,
      Consumer<CollectionAdminRequest.Create> parentDecorator,
      Function<String, SolrInputDocument> parentDocFactory,
      BiFunction<String, String, SolrInputDocument> childDocFactory)
      throws Exception {
    final Path configDir = TEST_COLL1_CONF();

    String configName = "_default"; // "solrCloudCollectionConfig";
    int nodeCount = 5;
    final MiniSolrCloudCluster cloudCluster =
        configureCluster(nodeCount) // .addConfig(configName, configDir)
            .configure();

    PluginMeta plugin = new PluginMeta();
    plugin.name = PlacementPluginFactory.PLUGIN_NAME;
    plugin.klass = StubShardAffinityPlacementFactory.class.getName();
    plugin.config =
        new AffinityPlacementConfig(
            1, 2, Collections.emptyMap(), Map.of(toColl, fromColl), Map.of());

    V2Request req =
        new V2Request.Builder("/cluster/plugin")
            .forceV2(true)
            .POST()
            .withPayload(singletonMap("add", plugin))
            .build();
    req.process(cluster.getSolrClient());
    // TODO await completion
    // version = phaser.awaitAdvanceInterruptibly(version, 10, TimeUnit.SECONDS);

    Map<String, String> collectionProperties = new HashMap<>();

    // create a collection holding data for the "to" side of the JOIN

    final CollectionAdminRequest.Create createChildCollReq =
        CollectionAdminRequest.createCollection(fromColl, configName, 3, 2)
            .setProperties(collectionProperties);
    fromDecorator.accept(createChildCollReq);
    createChildCollReq.process(cluster.getSolrClient());

    // collectionProperties.put("router.field", "id");
    final CollectionAdminRequest.Create parentReq =
        CollectionAdminRequest.createCollection(toColl, configName, 3, 2)
            .setProperties(collectionProperties);
    parentDecorator.accept(parentReq);
    parentReq.process(cluster.getSolrClient());
    // TODO await completion

    List<SolrInputDocument> parents = new ArrayList<>();
    List<SolrInputDocument> children = new ArrayList<>();
    for (int parent = 1000; parent <= 10 * 1000; parent += 1000) {
      int firstChild = parent + (parent / 100) + 1;
      for (int child = firstChild; child - firstChild <= 10; child += 1) {
        SolrInputDocument childDoc = childDocFactory.apply("" + child, "" + parent);
        childDoc.setField("name_sI", "" + child); // search by
        childDoc.setField("parent_id_s", "" + parent); // join by
        children.add(childDoc);
      }
      SolrInputDocument parentDoc = parentDocFactory.apply("" + parent);
      parentDoc.setField("id", "" + parent);
      parents.add(parentDoc);
    }
    UpdateRequest upChild = new UpdateRequest();
    upChild.add(children);
    upChild.commit(cluster.getSolrClient(), fromColl);

    UpdateRequest upParent = new UpdateRequest();
    upParent.add(parents);
    upParent.commit(cluster.getSolrClient(), toColl);

    final CollectionAdminResponse process =
        CollectionAdminRequest.getClusterStatus().process(cluster.getSolrClient());
  }

  @AfterClass
  public static void shutdown() {
    System.clearProperty("solr.test.sys.prop1");
    System.clearProperty("solr.test.sys.prop2");
    log.info("logic complete ... deleting the {} and {} collections", toColl, fromColl);

    // try to clean up
    for (String c : new String[] {toColl, fromColl}) {
      try {
        CollectionAdminRequest.Delete req = CollectionAdminRequest.deleteCollection(c);
        req.process(cluster.getSolrClient());
      } catch (Exception e) {
        // don't fail the test
        log.warn("Could not delete collection {} after test completed due to:", c, e);
      }
    }

    log.info("succeeded ... shutting down now!");
  }

  protected void testJoins(
      String toColl, String fromColl, String qLocalParams, boolean isScoresTest)
      throws SolrServerException, IOException {
    // verify the join with fromIndex works

    CloudSolrClient client = cluster.getSolrClient();
    List<Map.Entry<Integer, Integer>> parentToChild = new ArrayList<>();
    {
      for (int parent = 1000; parent <= 10 * 1000; parent += 1000) {
        int firstChild = parent + (parent / 100) + 1;
        for (int child = firstChild + random().nextInt(3);
            child - firstChild <= 10;
            child += 1 + random().nextInt(3)) {
          {
            final String fromQ = "name_sI:" + child;
            final String joinQ =
                "{!join "
                    + anyScoreMode(isScoresTest)
                    + "from=parent_id_s fromIndex="
                    + fromColl
                    + " "
                    + qLocalParams
                    + " to=id}"
                    + fromQ;
            QueryRequest qr = new QueryRequest(params("collection", toColl, "q", joinQ, "fl", "*"));
            QueryResponse rsp = new QueryResponse(client.request(qr), client);
            SolrDocumentList hits = rsp.getResults();
            assertEquals("Expected 1 doc, got " + hits, 1, hits.getNumFound());
            SolrDocument doc = hits.get(0);
            assertEquals("" + parent, doc.getFirstValue("id"));
          }
          parentToChild.add(new AbstractMap.SimpleEntry<>(parent, child));
          // let's join from child to parent
          {
            final String fromQ = "id:" + parent;
            final String joinQ =
                "{!join "
                    + anyScoreMode(isScoresTest)
                    + "to=parent_id_s fromIndex="
                    + toColl
                    + " "
                    + qLocalParams
                    + " from=id}"
                    + fromQ;
            QueryRequest qr =
                new QueryRequest(
                    params("collection", fromColl, "q", joinQ, "fl", "*", "rows", "20"));
            QueryResponse rsp = new QueryResponse(client.request(qr), client);
            SolrDocumentList hits = rsp.getResults();
            assertEquals("Expected 11 doc, got " + hits, 10 + 1, hits.getNumFound());
            for (SolrDocument doc : hits) {
              assertEquals("" + parent, doc.getFirstValue("parent_id_s"));
            }
          }
        }
      }
    }
    for (int pass = 0; pass < 5; pass++) {
      // pick two children, join'em to two or single parent
      Collections.shuffle(parentToChild, random());

      final String fromQ =
          "name_sI:("
              + parentToChild.get(0).getValue()
              + " "
              + parentToChild.get(1).getValue()
              + ")";
      final String joinQ =
          "{!join "
              + anyScoreMode(isScoresTest)
              + "from=parent_id_s fromIndex="
              + fromColl
              + " "
              + qLocalParams
              + " to=id}"
              + fromQ;
      QueryRequest qr = new QueryRequest(params("collection", toColl, "q", joinQ, "fl", "*"));
      QueryResponse rsp = new QueryResponse(client.request(qr), client);
      SolrDocumentList hits = rsp.getResults();
      final Set<String> expect =
          new HashSet<>(
              Arrays.asList(
                  parentToChild.get(0).getKey().toString(),
                  parentToChild.get(1).getKey().toString()));
      final Set<Object> act =
          hits.stream().map(doc -> doc.getFirstValue("id")).collect(Collectors.toSet());
      assertEquals(expect, act);
    }
  }

  private String anyScoreMode(boolean isScoresTest) {
    return isScoresTest ? "score=" + (scoreModes[random().nextInt(scoreModes.length)]) + " " : "";
  }
}
