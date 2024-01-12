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

package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class NestedShardedAtomicUpdateTest extends SolrCloudTestCase {
  private static final String DEBUG_LABEL = MethodHandles.lookup().lookupClass().getName();
  private static final String DEFAULT_COLLECTION = DEBUG_LABEL + "_collection";

  private static CloudSolrClient cloudClient;
  private static List<SolrClient> clients; // not CloudSolrClient

  @BeforeClass
  public static void beforeClass() throws Exception {
    final String configName = DEBUG_LABEL + "_config-set";
    final Path configDir = TEST_COLL1_CONF();

    configureCluster(1).addConfig(configName, configDir).configure();

    cloudClient = cluster.getSolrClient(DEFAULT_COLLECTION);

    CollectionAdminRequest.createCollection(DEFAULT_COLLECTION, configName, 4, 1)
        .withProperty("config", "solrconfig-tlog.xml")
        .withProperty("schema", "schema-nest.xml")
        .process(cloudClient);

    cluster.waitForActiveCollection(DEFAULT_COLLECTION, 4, 4);

    clients = new ArrayList<>();
    ClusterState clusterState = cloudClient.getClusterState();
    for (Replica replica : clusterState.getCollection(DEFAULT_COLLECTION).getReplicas()) {
      clients.add(getHttpSolrClient(replica.getCoreUrl()));
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    IOUtils.close(clients);
  }

  @Test
  public void doRootShardRoutingTest() throws Exception {
    assertEquals(
        4, cloudClient.getClusterState().getCollection(DEFAULT_COLLECTION).getSlices().size());
    final String[] ids = {"3", "4", "5", "6"};

    assertEquals(
        "size of ids to index should be the same as the number of clients",
        clients.size(),
        ids.length);
    // for now,  we know how ranges will be distributed to shards.
    // may have to look it up in clusterstate if that assumption changes.

    SolrInputDocument doc = sdoc("id", "1", "_root_", "1", "level_s", "root");

    int which = (doc.get("_root_").hashCode() & 0x7fffffff) % clients.size();
    SolrClient aClient = clients.get(which);

    indexDoc(aClient, null, doc);

    doc =
        sdoc(
            "id",
            "1",
            "_root_",
            "1",
            "children",
            map("add", sdocs(sdoc("id", "2", "level_s", "child"))));

    indexDoc(aClient, null, doc);

    for (int idIndex = 0; idIndex < ids.length; ++idIndex) {

      doc =
          sdoc(
              "id",
              "2",
              "_root_",
              "1",
              "grandChildren",
              map("add", sdocs(sdoc("id", ids[idIndex], "level_s", "grand_child"))));

      indexDocAndRandomlyCommit(getRandomSolrClient(), null, doc);

      doc = sdoc("id", "3", "_root_", "1", "inplace_updatable_int", map("inc", "1"));

      indexDocAndRandomlyCommit(getRandomSolrClient(), null, doc);

      // assert RTG request respects _route_ param
      QueryResponse routeRsp =
          getRandomSolrClient().query(params("qt", "/get", "id", "2", "_route_", "1"));
      SolrDocument results = (SolrDocument) routeRsp.getResponse().get("doc");
      assertNotNull(
          "RTG should find doc because _route_ was set to the root documents' ID", results);
      assertEquals("2", results.getFieldValue("id"));

      // assert all docs are indexed under the same root
      getRandomSolrClient().commit();
      assertEquals(0, getRandomSolrClient().query(params("q", "-_root_:1")).getResults().size());

      // assert all docs are indexed inside the same block
      QueryResponse rsp =
          getRandomSolrClient().query(params("qt", "/get", "id", "1", "fl", "*, [child]"));
      SolrDocument val = (SolrDocument) rsp.getResponse().get("doc");
      assertEquals("1", val.getFieldValue("id"));
      @SuppressWarnings({"unchecked"})
      List<SolrDocument> children = (List) val.getFieldValues("children");
      assertEquals(1, children.size());
      SolrDocument childDoc = children.get(0);
      assertEquals("2", childDoc.getFieldValue("id"));
      @SuppressWarnings({"unchecked"})
      List<SolrDocument> grandChildren = (List) childDoc.getFieldValues("grandChildren");
      assertEquals(idIndex + 1, grandChildren.size());
      SolrDocument grandChild = grandChildren.get(0);
      assertEquals("3", grandChild.getFieldValue("id"));
      assertEquals(idIndex + 1, grandChild.getFirstValue("inplace_updatable_int"));
    }
  }

  @Test
  public void doNestedInplaceUpdateTest() throws Exception {
    assertEquals(
        4, cloudClient.getClusterState().getCollection(DEFAULT_COLLECTION).getSlices().size());
    final String[] ids = {"3", "4", "5", "6"};

    assertEquals(
        "size of ids to index should be the same as the number of clients",
        clients.size(),
        ids.length);
    // for now,  we know how ranges will be distributed to shards.
    // may have to look it up in clusterstate if that assumption changes.

    SolrInputDocument doc = sdoc("id", "1", "_root_", "1", "level_s", "root");

    int which = (doc.get("_root_").hashCode() & 0x7fffffff) % clients.size();
    SolrClient aClient = clients.get(which);

    indexDocAndRandomlyCommit(aClient, null, doc);

    doc =
        sdoc(
            "id",
            "1",
            "_root_",
            "1",
            "children",
            map("add", sdocs(sdoc("id", "2", "level_s", "child"))));

    indexDocAndRandomlyCommit(aClient, null, doc);

    doc =
        sdoc(
            "id",
            "2",
            "_root_",
            "1",
            "grandChildren",
            map("add", sdocs(sdoc("id", ids[0], "level_s", "grand_child"))));

    indexDocAndRandomlyCommit(aClient, null, doc);

    int id1InPlaceCounter = 0;
    int id2InPlaceCounter = 0;
    int id3InPlaceCounter = 0;
    for (int fieldValue = 1; fieldValue < 5; ++fieldValue) {
      // randomly increment a field on a root, middle, and leaf doc
      if (random().nextBoolean()) {
        id1InPlaceCounter++;
        indexDoc(
            getRandomSolrClient(),
            null,
            sdoc("id", "1", "_root_", "1", "inplace_updatable_int", map("inc", "1")));
      }
      if (random().nextBoolean()) {
        id2InPlaceCounter++;
        indexDoc(
            getRandomSolrClient(),
            null,
            sdoc("id", "2", "_root_", "1", "inplace_updatable_int", map("inc", "1")));
      }
      if (random().nextBoolean()) { // TODO consider removing this now-useless block?
        id3InPlaceCounter++;
        indexDoc(
            getRandomSolrClient(),
            null,
            sdoc("id", "3", "_root_", "1", "inplace_updatable_int", map("inc", "1")));
      }
      if (random().nextBoolean()) {
        getRandomSolrClient().commit();
      }

      if (random().nextBoolean()) {
        // assert RTG request respects _route_ param
        QueryResponse routeRsp =
            getRandomSolrClient().query(params("qt", "/get", "id", "2", "_route_", "1"));
        SolrDocument results = (SolrDocument) routeRsp.getResponse().get("doc");
        assertNotNull(
            "RTG should find doc because _route_ was set to the root documents' ID", results);
        assertEquals("2", results.getFieldValue("id"));
      }

      if (random().nextBoolean()) {
        // assert all docs are indexed under the same root
        assertEquals(0, getRandomSolrClient().query(params("q", "-_root_:1")).getResults().size());
      }

      if (random().nextBoolean()) {
        // assert all docs are indexed inside the same block
        QueryResponse rsp =
            getRandomSolrClient().query(params("qt", "/get", "id", "1", "fl", "*, [child]"));
        SolrDocument val = (SolrDocument) rsp.getResponse().get("doc");
        assertEquals("1", val.getFieldValue("id"));
        assertInplaceCounter(id1InPlaceCounter, val);
        @SuppressWarnings({"unchecked"})
        List<SolrDocument> children = (List) val.getFieldValues("children");
        assertEquals(1, children.size());
        SolrDocument childDoc = children.get(0);
        assertEquals("2", childDoc.getFieldValue("id"));
        assertInplaceCounter(id2InPlaceCounter, childDoc);
        @SuppressWarnings({"unchecked"})
        List<SolrDocument> grandChildren = (List) childDoc.getFieldValues("grandChildren");
        assertEquals(1, grandChildren.size());
        SolrDocument grandChild = grandChildren.get(0);
        assertEquals("3", grandChild.getFieldValue("id"));
        assertInplaceCounter(id3InPlaceCounter, grandChild);
      }
    }
  }

  private void assertInplaceCounter(int expected, SolrDocument val) {
    Number result = (Number) val.getFirstValue("inplace_updatable_int");
    if (expected == 0) {
      assertNull(val.toString(), result);
    } else {
      assertNotNull(val.toString(), result);
      assertEquals(expected, result.intValue());
    }
  }

  @Test
  public void sendWrongRouteParam() throws Exception {
    assertEquals(
        4, cloudClient.getClusterState().getCollection(DEFAULT_COLLECTION).getSlices().size());
    final String rootId = "1";

    SolrInputDocument doc = sdoc("id", rootId, "level_s", "root");

    final SolrParams wrongRouteParams = params("_route_", "c");
    final SolrParams rightParams = params("_route_", rootId);

    int which = (rootId.hashCode() & 0x7fffffff) % clients.size();
    SolrClient aClient = clients.get(which);

    indexDocAndRandomlyCommit(aClient, null, doc);

    final SolrInputDocument childDoc =
        sdoc("id", rootId, "children", map("add", sdocs(sdoc("id", "2", "level_s", "child"))));

    indexDocAndRandomlyCommit(aClient, rightParams, childDoc);

    final SolrInputDocument grandChildDoc =
        sdoc(
            "id",
            "2",
            "_root_",
            rootId,
            "grandChildren",
            map("add", sdocs(sdoc("id", "3", "level_s", "grandChild"))));

    // despite the wrong param, it'll be routed correctly; we can find the doc after.
    //   An error would have been okay too but routing correctly is also fine.
    indexDoc(aClient, wrongRouteParams, grandChildDoc);
    aClient.commit();

    assertEquals(
        1, aClient.query(params("_route_", rootId, "q", "id:3")).getResults().getNumFound());
  }

  private void indexDocAndRandomlyCommit(
      SolrClient client, SolrParams params, SolrInputDocument sdoc)
      throws IOException, SolrServerException {
    indexDoc(client, params, sdoc);
    // randomly commit docs
    if (random().nextBoolean()) {
      client.commit();
    }
  }

  private void indexDoc(SolrClient client, SolrParams params, SolrInputDocument sdoc)
      throws IOException, SolrServerException {
    final UpdateRequest updateRequest = new UpdateRequest();
    updateRequest.add(sdoc);
    updateRequest.setParams(new ModifiableSolrParams(params));
    updateRequest.process(client, null);
  }

  private SolrClient getRandomSolrClient() {
    // randomly return one of these clients, to include the cloudClient
    final int index = random().nextInt(clients.size() + 1);
    return index == clients.size() ? cloudClient : clients.get(index);
  }
}
