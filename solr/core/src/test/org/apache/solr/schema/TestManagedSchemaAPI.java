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

package org.apache.solr.schema;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.core.SolrCore;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestManagedSchemaAPI extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void createCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "true");
    configureCluster(2)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-managed").resolve("conf"))
        .configure();
  }

  @Test
  public void test() throws Exception {
    String collection = "testschemaapi";
    CollectionAdminRequest.createCollection(collection, "conf1", 1, 2)
        .process(cluster.getSolrClient());
    testModifyField(collection);
    testReloadAndAddSimple(collection);
    testAddFieldAndDocument(collection);
  }

  @Test
  public void testAddFieldDoesNotReloadCore() throws Exception {
    String collection = "addfield_noreload";
    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(collection, "conf1", 1, 2).process(client);
    cluster.waitForActiveCollection(collection, 1, 2);
    Map<String, UUID> coreIdsBefore = snapshotCoreIds(collection);

    addStringField("live_field", collection, client);
    // a reload creates a new SolrCore with a new uniqueId
    assertEquals(coreIdsBefore, snapshotCoreIds(collection));
    assertEveryReplicaLatestSchema(collection, s -> s.getFieldOrNull("live_field") != null);

    client.add(collection, sdoc("id", "live-1", "live_field", "v"));
    client.commit(collection);
    assertEquals(
        1, client.query(collection, new SolrQuery("live_field:v")).getResults().getNumFound());
  }

  @Test
  public void testAddDynamicFieldAndCopyFieldDoNotReloadCore() throws Exception {
    String collection = "adddynamic_copy_noreload";
    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(collection, "conf1", 1, 2).process(client);
    cluster.waitForActiveCollection(collection, 1, 2);
    Map<String, UUID> coreIdsBefore = snapshotCoreIds(collection);

    assertSchemaUpdateSucceeded(
        new SchemaRequest.AddDynamicField(Map.of("name", "*_live", "type", "string"))
            .process(client, collection));
    assertEquals(coreIdsBefore, snapshotCoreIds(collection));
    assertEveryReplicaLatestSchema(collection, s -> s.getFieldOrNull("color_live") != null);

    addStringField("copy_src", collection, client);
    addStringField("copy_dst", collection, client);
    assertSchemaUpdateSucceeded(
        new SchemaRequest.AddCopyField("copy_src", List.of("copy_dst"))
            .process(client, collection));
    assertEquals(coreIdsBefore, snapshotCoreIds(collection));
    assertEveryReplicaLatestSchema(
        collection,
        s ->
            s.getCopyFieldsList("copy_src").stream()
                .anyMatch(cf -> cf.getDestination().getName().equals("copy_dst")));

    client.add(collection, sdoc("id", "live-2", "color_live", "red", "copy_src", "blue"));
    client.commit(collection);
    assertEquals(
        1, client.query(collection, new SolrQuery("color_live:red")).getResults().getNumFound());
    assertEquals(
        1, client.query(collection, new SolrQuery("copy_dst:blue")).getResults().getNumFound());
  }

  @Test
  public void testDeleteFieldStillReloadsCore() throws Exception {
    String collection = "deletefield_reload";
    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(collection, "conf1", 1, 2).process(client);
    cluster.waitForActiveCollection(collection, 1, 2);
    addStringField("doomed_field", collection, client);
    Map<String, UUID> coreIdsBefore = snapshotCoreIds(collection);

    assertSchemaUpdateSucceeded(
        new SchemaRequest.DeleteField("doomed_field").process(client, collection));

    // The core handling the request reloads before responding. Whether other replicas also reload
    // depends on whether their schema watch fires before the conf dir listener.
    Map<String, UUID> coreIdsAfter = snapshotCoreIds(collection);
    assertTrue(
        coreIdsBefore.keySet().stream()
            .anyMatch(name -> !coreIdsBefore.get(name).equals(coreIdsAfter.get(name))));
    assertEveryReplicaLatestSchema(collection, s -> s.getFieldOrNull("doomed_field") == null);
  }

  private void assertEveryReplicaLatestSchema(String collection, Predicate<IndexSchema> expectation)
      throws IOException {
    for (Replica replica : getCollectionState(collection).replicaStream().toList()) {
      try (SolrCore core =
          cluster.getReplicaJetty(replica).getCoreContainer().getCore(replica.getCoreName())) {
        assertNotNull(core);
        assertTrue(replica.getName(), expectation.test(core.getLatestSchema()));
      }
    }
  }

  private static void assertSchemaUpdateSucceeded(SchemaResponse.UpdateResponse rsp) {
    assertEquals(0, rsp.getStatus());
    assertNull(rsp.getResponse().get("errors"));
  }

  private Map<String, UUID> snapshotCoreIds(String collection) {
    Map<String, UUID> ids = new HashMap<>();
    for (Replica replica : getCollectionState(collection).replicaStream().toList()) {
      try (SolrCore core =
          cluster.getReplicaJetty(replica).getCoreContainer().getCore(replica.getCoreName())) {
        assertNotNull(core);
        ids.put(replica.getName(), core.uniqueId);
      }
    }
    return ids;
  }

  private void testReloadAndAddSimple(String collection) throws IOException, SolrServerException {
    CloudSolrClient cloudClient = cluster.getSolrClient();

    String fieldName = "myNewField";
    addStringField(fieldName, collection, cloudClient);

    CollectionAdminRequest.Reload reloadRequest =
        CollectionAdminRequest.reloadCollection(collection);
    CollectionAdminResponse response = reloadRequest.process(cloudClient);
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "1");
    doc.addField(fieldName, "val");
    UpdateRequest ureq = new UpdateRequest().add(doc);
    cloudClient.request(ureq, collection);
  }

  private void testAddFieldAndDocument(String collection) throws IOException, SolrServerException {
    CloudSolrClient cloudClient = cluster.getSolrClient();

    String fieldName = "myNewField1";
    addStringField(fieldName, collection, cloudClient);

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "2");
    doc.addField(fieldName, "val1");
    UpdateRequest ureq = new UpdateRequest().add(doc);
    cloudClient.request(ureq, collection);
  }

  private void addStringField(String fieldName, String collection, CloudSolrClient cloudClient)
      throws IOException, SolrServerException {
    Map<String, Object> fieldAttributes = new LinkedHashMap<>();
    fieldAttributes.put("name", fieldName);
    fieldAttributes.put("type", "string");
    SchemaRequest.AddField addFieldUpdateSchemaRequest =
        new SchemaRequest.AddField(fieldAttributes);
    SchemaResponse.UpdateResponse addFieldResponse =
        addFieldUpdateSchemaRequest.process(cloudClient, collection);
    assertEquals(0, addFieldResponse.getStatus());
    assertNull(addFieldResponse.getResponse().get("errors"));

    log.info("added new field={}", fieldName);
  }

  private void testModifyField(String collection) throws Exception {
    CloudSolrClient cloudClient = cluster.getSolrClient();
    // assumption: in this schema, the id field is docValues=false, uninvertible=true

    SolrInputDocument doc = new SolrInputDocument("id", "3");
    cloudClient.add(collection, doc);
    cloudClient.commit(collection);
    cloudClient.query(collection, params("q", "*:*", "sort", "id asc")); // does not throw

    String fieldName = "id";
    SchemaRequest.Field getFieldRequest = new SchemaRequest.Field(fieldName);
    SchemaResponse.FieldResponse getFieldResponse =
        getFieldRequest.process(cloudClient, collection);
    Map<String, Object> field = getFieldResponse.getField();
    field.put("uninvertible", false); // and because this field does not have docValues, can't sort.
    SchemaRequest.ReplaceField replaceRequest = new SchemaRequest.ReplaceField(field);
    SchemaResponse.UpdateResponse replaceResponse = replaceRequest.process(cloudClient, collection);
    assertNull(replaceResponse.getResponse().get("errors"));
    CollectionAdminRequest.Reload reloadRequest =
        CollectionAdminRequest.reloadCollection(collection);
    CollectionAdminResponse response = reloadRequest.process(cloudClient);
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());

    Exception e =
        expectThrows(
            Exception.class,
            () -> {
              cloudClient.query(collection, params("q", "*:*", "sort", "id asc"));
            });
    assertTrue("Should fail because needs docValues", e.getMessage().contains("docValues"));
  }
}
