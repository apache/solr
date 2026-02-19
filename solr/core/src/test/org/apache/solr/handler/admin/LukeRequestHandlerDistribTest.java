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
package org.apache.solr.handler.admin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class LukeRequestHandlerDistribTest extends SolrCloudTestCase {

  private static final String COLLECTION = "lukeDistribTest";
  private static final int NUM_DOCS = 20;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-dynamic")).configure();

    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1)
        .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    cluster.waitForActiveCollection(COLLECTION, 2, 2);

    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 0; i < NUM_DOCS; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", String.valueOf(i));
      doc.addField("name", "name_" + i);
      doc.addField("subject", "subject value " + (i % 5));
      docs.add(doc);
    }
    cluster.getSolrClient().add(COLLECTION, docs);
    cluster.getSolrClient().commit(COLLECTION);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    shutdownCluster();
  }

  /** Sends a luke request and wraps the raw response in a typed {@link LukeResponse}. */
  private LukeResponse requestLuke(String collection, ModifiableSolrParams extra) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/admin/luke");
    params.set("numTerms", "0");
    if (extra != null) {
      for (Map.Entry<String, String[]> entry : extra.getMap().entrySet()) {
        params.set(entry.getKey(), entry.getValue());
      }
    }
    QueryRequest req = new QueryRequest(params);
    NamedList<Object> raw = cluster.getSolrClient().request(req, collection);
    LukeResponse rsp = new LukeResponse();
    rsp.setResponse(raw);
    return rsp;
  }

  @Test
  public void testDistributedMerge() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("distrib", "true");

    LukeResponse rsp = requestLuke(COLLECTION, params);

    assertEquals(
        "merged numDocs should equal total docs", NUM_DOCS, rsp.getNumDocsAsLong().longValue());
    assertTrue("merged maxDoc should be > 0", rsp.getMaxDoc() > 0);
    assertNotNull("deletedDocs should be present", rsp.getDeletedDocsAsLong());

    Map<String, LukeResponse> shards = rsp.getShardResponses();
    assertNotNull("shards section should be present", shards);
    assertEquals("should have 2 shard entries", 2, shards.size());

    // Each shard should have its own index info; per-shard numDocs should sum to total
    long sumShardDocs = 0;
    for (Map.Entry<String, LukeResponse> entry : shards.entrySet()) {
      LukeResponse shardLuke = entry.getValue();
      assertNotNull("each shard should have numDocs", shardLuke.getNumDocsAsLong());
      assertNotNull("each shard should have maxDoc", shardLuke.getMaxDoc());
      sumShardDocs += shardLuke.getNumDocsAsLong();
    }
    assertEquals(
        "sum of per-shard numDocs should equal merged numDocs",
        rsp.getNumDocsAsLong().longValue(),
        sumShardDocs);
  }

  @Test
  public void testDistributedFieldsMerge() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("distrib", "true");

    LukeResponse rsp = requestLuke(COLLECTION, params);

    Map<String, LukeResponse.FieldInfo> fields = rsp.getFieldInfo();
    assertNotNull("fields should be present", fields);

    LukeResponse.FieldInfo nameField = fields.get("name");
    assertNotNull("'name' field should be present", nameField);
    assertNotNull("field type should be present", nameField.getType());
    assertNotNull("schema flags should be present", nameField.getSchema());
    assertEquals(
        "merged docs count for 'name' should equal total docs",
        NUM_DOCS,
        nameField.getDocsAsLong().longValue());

    LukeResponse.FieldInfo idField = fields.get("id");
    assertNotNull("'id' field should be present", idField);
    assertEquals("id field type should be string", "string", idField.getType());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDetailedFieldStatsPerShard() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("distrib", "true");
    params.set("fl", "name");
    params.set("numTerms", "5");

    LukeResponse rsp = requestLuke(COLLECTION, params);

    // Top-level fields should NOT have topTerms, distinct, histogram
    LukeResponse.FieldInfo nameField = rsp.getFieldInfo().get("name");
    assertNotNull("'name' field should be present", nameField);
    assertNull("topTerms should NOT be in top-level fields", nameField.getTopTerms());
    assertEquals("distinct should NOT be in top-level fields", 0, nameField.getDistinct());

    // Per-shard entries should have detailed stats
    Map<String, LukeResponse> shards = rsp.getShardResponses();
    assertNotNull("shards section should be present", shards);

    boolean foundDetailedStats = false;
    for (Map.Entry<String, LukeResponse> entry : shards.entrySet()) {
      LukeResponse shardLuke = entry.getValue();
      // Access the raw shard entry for per-shard fields
      NamedList<Object> shardRaw = shardLuke.getResponse();
      NamedList<Object> shardFields = (NamedList<Object>) shardRaw.get("fields");
      if (shardFields != null) {
        NamedList<Object> shardNameField = (NamedList<Object>) shardFields.get("name");
        if (shardNameField != null) {
          foundDetailedStats = true;
          assertTrue(
              "per-shard field should have topTerms, distinct, or histogram",
              shardNameField.get("topTerms") != null
                  || shardNameField.get("distinct") != null
                  || shardNameField.get("histogram") != null);
        }
      }
    }
    assertTrue("at least one shard should have detailed field stats", foundDetailedStats);
  }

  @Test
  public void testLocalModeDefault() throws Exception {
    LukeResponse rsp = requestLuke(COLLECTION, null);

    assertNotNull("index info should be present", rsp.getIndexInfo());
    assertNull("shards should NOT be present in local mode", rsp.getShardResponses());
  }

  @Test
  public void testExplicitDistribFalse() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("distrib", "false");

    LukeResponse rsp = requestLuke(COLLECTION, params);

    assertNotNull("index info should be present", rsp.getIndexInfo());
    assertNull("shards should NOT be present with distrib=false", rsp.getShardResponses());
  }

  /**
   * 12 shards, 1 document: only one shard has data, the other 11 are empty. Verifies that
   * schema-derived attributes (type, schema flags, dynamicBase) merge correctly when most shards
   * have no documents, and that index-derived attributes (index flags, docs count) degrade
   * gracefully.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testSparseShards() throws Exception {
    String collection = "lukeSparse12";
    CollectionAdminRequest.createCollection(collection, "conf", 12, 1)
        .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    cluster.waitForActiveCollection(collection, 12, 12);

    try {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "only-one");
      doc.addField("name", "sparse test");
      doc.addField("subject", "subject value");
      doc.addField("cat_s", "category");
      cluster.getSolrClient().add(collection, doc);
      cluster.getSolrClient().commit(collection);

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("distrib", "true");

      LukeResponse rsp = requestLuke(collection, params);

      // Index-level stats
      assertEquals("numDocs should be 1", 1, rsp.getNumDocsAsLong().longValue());
      assertTrue("maxDoc should be > 0", rsp.getMaxDoc() > 0);
      assertEquals("deletedDocs should be 0", 0, rsp.getDeletedDocsAsLong().longValue());

      Map<String, LukeResponse> shards = rsp.getShardResponses();
      assertNotNull("shards section should be present", shards);
      assertEquals("should have 12 shard entries", 12, shards.size());

      // Exactly one shard should have numDocs=1
      long sumShardDocs = 0;
      for (Map.Entry<String, LukeResponse> entry : shards.entrySet()) {
        LukeResponse shardLuke = entry.getValue();
        assertNotNull("each shard should have numDocs", shardLuke.getNumDocsAsLong());
        sumShardDocs += shardLuke.getNumDocsAsLong();
      }
      assertEquals("sum of per-shard numDocs should be 1", 1, sumShardDocs);

      // Field-level checks
      Map<String, LukeResponse.FieldInfo> fields = rsp.getFieldInfo();
      assertNotNull("fields should be present", fields);

      // Schema-derived attrs should be present for all fields, even with 11 empty shards
      LukeResponse.FieldInfo idField = fields.get("id");
      assertNotNull("'id' field should be present", idField);
      assertEquals("id type", "string", idField.getType());
      assertNotNull("id schema flags", idField.getSchema());

      LukeResponse.FieldInfo nameField = fields.get("name");
      assertNotNull("'name' field should be present", nameField);
      assertNotNull("name type", nameField.getType());
      assertNotNull("name schema flags", nameField.getSchema());
      assertEquals("name docs should be 1", 1, nameField.getDocsAsLong().longValue());

      // Dynamic field — should have dynamicBase in extras
      LukeResponse.FieldInfo catField = fields.get("cat_s");
      assertNotNull("'cat_s' field should be present", catField);
      assertNotNull("cat_s type", catField.getType());
      assertNotNull("cat_s dynamicBase", catField.getExtras().get("dynamicBase"));

      // Verify index flags are present (from the one shard that has the document).
      // Fields that are indexed and have a live doc should get index flags via the merge's
      // computeIfAbsent (take-first-non-null) logic.
      NamedList<Object> mergedFields = (NamedList<Object>) rsp.getResponse().get("fields");
      assertNotNull("merged fields NamedList should be present", mergedFields);
      NamedList<Object> rawNameField = (NamedList<Object>) mergedFields.get("name");
      assertNotNull("raw 'name' field should be in merged fields", rawNameField);
      // The index flags key may or may not be present depending on whether the field is indexed
      // and stored — but if present, it should be a non-empty string
      Object indexFlags = rawNameField.get("index");
      if (indexFlags != null) {
        assertTrue("index flags should be a non-empty string", indexFlags.toString().length() > 0);
      }
    } finally {
      CollectionAdminRequest.deleteCollection(collection)
          .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    }
  }

  @Test
  public void testDistribTrueOnSingleShardFallsBackToLocal() throws Exception {
    String singleShardCollection = "lukeSingleShard";
    CollectionAdminRequest.createCollection(singleShardCollection, "conf", 1, 1)
        .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    cluster.waitForActiveCollection(singleShardCollection, 1, 1);

    try {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "single-1");
      doc.addField("name", "test_name");
      cluster.getSolrClient().add(singleShardCollection, doc);
      cluster.getSolrClient().commit(singleShardCollection);

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("distrib", "true");

      LukeResponse rsp = requestLuke(singleShardCollection, params);

      assertNotNull(
          "index info should be present even with distrib=true on single shard",
          rsp.getIndexInfo());
      assertEquals("should see the 1 doc we indexed", 1, rsp.getNumDocsAsLong().longValue());
      assertNull(
          "shards should NOT be present when falling back to local", rsp.getShardResponses());
    } finally {
      CollectionAdminRequest.deleteCollection(singleShardCollection)
          .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    }
  }
}
