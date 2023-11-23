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
package org.apache.solr.update;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class UuidAtomicUpdateTest extends SolrCloudTestCase {
  private static final String COMMITTED_DOC_ID = "1";
  private static final String UNCOMMITTED_DOC_ID = "2";
  private static final String COLLECTION = "collection1";
  private static final int NUM_SHARDS = 1;
  private static final int NUM_REPLICAS = 2; // bug occurs only with replica

  private static final String committedUuidAfter = UUID.randomUUID().toString();
  private static final String uncommittedUuidAfter = UUID.randomUUID().toString();

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-dynamic")).configure();

    CollectionAdminRequest.createCollection(COLLECTION, "conf", NUM_SHARDS, NUM_REPLICAS)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(COLLECTION, NUM_SHARDS, NUM_REPLICAS * NUM_SHARDS);
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    // wipe
    new UpdateRequest().deleteByQuery("*:*").process(cluster.getSolrClient(), COLLECTION);
    String committedUuidBefore = UUID.randomUUID().toString();
    final SolrInputDocument committedDoc =
        sdoc("id", COMMITTED_DOC_ID, "title_s", "title_1", "uuid", committedUuidBefore);
    new UpdateRequest().add(committedDoc).commit(cluster.getSolrClient(), COLLECTION);

    String uncommittedUuidBefore = UUID.randomUUID().toString();
    final SolrInputDocument uncommittedDoc =
        sdoc("id", UNCOMMITTED_DOC_ID, "title_s", "title_2", "uuid", uncommittedUuidBefore);
    new UpdateRequest().add(uncommittedDoc).process(cluster.getSolrClient(), COLLECTION);
  }

  @Test
  public void testUpdateCommittedTextField() throws Exception {
    atomicSetValue(COMMITTED_DOC_ID, "title_s", "CHANGED");
    ensureFieldHasValues(COMMITTED_DOC_ID, "title_s", "CHANGED");
    commit();
    ensureFieldHasValues(COMMITTED_DOC_ID, "title_s", "CHANGED");
  }

  @Test
  public void testUpdateUncommittedTextField() throws Exception {
    atomicSetValue(UNCOMMITTED_DOC_ID, "title_s", "CHANGED");
    ensureFieldHasValues(UNCOMMITTED_DOC_ID, "title_s", "CHANGED");
    commit();
    ensureFieldHasValues(UNCOMMITTED_DOC_ID, "title_s", "CHANGED");
  }

  @Test
  public void testUpdateCommittedUuidField() throws Exception {
    atomicSetValue(COMMITTED_DOC_ID, "uuid", committedUuidAfter);
    ensureFieldHasValues(COMMITTED_DOC_ID, "uuid", committedUuidAfter);
    commit();
    ensureFieldHasValues(COMMITTED_DOC_ID, "uuid", committedUuidAfter);
  }

  @Test
  public void testUpdateUncommittedUuidField() throws Exception {
    atomicSetValue(UNCOMMITTED_DOC_ID, "uuid", uncommittedUuidAfter);
    ensureFieldHasValues(UNCOMMITTED_DOC_ID, "uuid", uncommittedUuidAfter);
    commit();
    ensureFieldHasValues(UNCOMMITTED_DOC_ID, "uuid", uncommittedUuidAfter);
  }

  private static void commit() throws IOException, SolrServerException {
    new UpdateRequest().commit(cluster.getSolrClient(), COLLECTION);
  }

  private static void atomicSetValue(String docId, String fieldName, Object value)
      throws Exception {
    final SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", docId);
    doc.setField(fieldName, Map.of("set", value));

    UpdateResponse updateResponse = cluster.getSolrClient().add(COLLECTION, doc);
    assertEquals(updateResponse.toString(), 0, updateResponse.getStatus());
    assertEquals(
        updateResponse.toString(), NUM_REPLICAS, updateResponse.getResponseHeader().get("rf"));
  }

  private static void ensureFieldHasValues(
      String identifyingDocId, String fieldName, Object... expectedValues) throws Exception {
    for (Boolean tf : new Boolean[] {true, false}) {
      final ModifiableSolrParams solrParams = new ModifiableSolrParams();
      solrParams.set("id", identifyingDocId);
      solrParams.set("shards.preference", "replica.leader:" + tf);
      QueryRequest request = new QueryRequest(solrParams);
      request.setPath("/get");
      final QueryResponse response = request.process(cluster.getSolrClient(), COLLECTION);

      final NamedList<Object> rawResponse = response.getResponse();
      assertNotNull(rawResponse.get("doc"));
      assertTrue(rawResponse.get("doc") instanceof SolrDocument);
      final SolrDocument doc = (SolrDocument) rawResponse.get("doc");
      final Collection<Object> valuesAfterUpdate = doc.getFieldValues(fieldName);
      assertEquals(
          "Expected field to have "
              + expectedValues.length
              + " values, but found "
              + valuesAfterUpdate.size(),
          expectedValues.length,
          valuesAfterUpdate.size());
      for (Object expectedValue : expectedValues) {
        assertTrue(
            "Expected value ["
                + expectedValue
                + "] was not found in field, but "
                + valuesAfterUpdate,
            valuesAfterUpdate.contains(expectedValue));
      }
    }
  }
}
