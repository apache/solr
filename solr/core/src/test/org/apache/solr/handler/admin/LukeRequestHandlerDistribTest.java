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

import java.util.Map;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.request.LukeRequest;
import org.apache.solr.client.solrj.response.LukeResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.junit.Test;

public class LukeRequestHandlerDistribTest extends BaseDistributedSearchTestCase {

  private static final Long NUM_DOCS = 20L;

  public LukeRequestHandlerDistribTest() {
    fixShardCount(2);
  }

  private LukeResponse requestLuke() throws Exception {
    return requestLuke(new ModifiableSolrParams());
  }

  private LukeResponse requestLuke(ModifiableSolrParams extra) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/admin/luke");
    params.set("numTerms", "0");
    params.set("shards.info", "true");
    params.add(extra);

    // query() sends to control and a random shard with shards param, compares responses
    handle.put("QTime", SKIPVAL);
    handle.put(LukeRequestHandler.RSP_INDEX, SKIP);
    handle.put(LukeRequestHandler.RSP_SHARDS, SKIP);
    // Detailed per-field stats (distinct, topTerms, histogram) are kept per-shard in
    // distributed mode and intentionally excluded from the aggregated top-level fields.
    // Local mode includes them inline, so skip them in the comparison.
    handle.put(LukeRequestHandler.KEY_DISTINCT, SKIP);
    handle.put(LukeRequestHandler.KEY_TOP_TERMS, SKIP);
    handle.put(LukeRequestHandler.KEY_HISTOGRAM, SKIP);
    QueryResponse qr = query(params);
    LukeResponse rsp = new LukeResponse();
    rsp.setResponse(qr.getResponse());

    return rsp;
  }

  private void assertLukeXPath(ModifiableSolrParams extra, String... xpaths) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("shards", shards);
    params.add(extra);
    LukeTestUtil.assertLukeXPath(controlClient, null, params, xpaths);
  }

  private void indexTestData() throws Exception {
    for (int i = 0; i < NUM_DOCS; i++) {
      index("id", String.valueOf(i), "name", "name_" + i, "subject", "subject value " + (i % 5));
    }
    commit();
  }

  @Test
  @ShardsFixed(num = 2)
  public void testDistributedAggregateAndFields() throws Exception {
    indexTestData();

    // --- Aggregate index stats ---
    LukeResponse rsp = requestLuke();

    assertEquals("aggregated numDocs should equal total docs", NUM_DOCS, rsp.getNumDocs());
    assertTrue("aggregated maxDoc should be > 0", rsp.getMaxDoc() > 0);
    assertNotNull("deletedDocs should be present", rsp.getDeletedDocs());

    Map<String, LukeResponse> shardResponses = rsp.getShardResponses();
    assertNotNull("shards section should be present", shardResponses);
    assertEquals("should have 2 shard entries", 2, shardResponses.size());

    Long sumShardDocs = 0L;
    for (Map.Entry<String, LukeResponse> entry : shardResponses.entrySet()) {
      LukeResponse shardLuke = entry.getValue();
      assertNotNull("each shard should have numDocs", shardLuke.getNumDocs());
      assertNotNull("each shard should have maxDoc", shardLuke.getMaxDoc());
      sumShardDocs += shardLuke.getNumDocs();
    }
    assertEquals(
        "sum of per-shard numDocs should equal aggregated numDocs", rsp.getNumDocs(), sumShardDocs);

    // --- Field-level aggregation ---
    Map<String, LukeResponse.FieldInfo> fields = rsp.getFieldInfo();
    assertNotNull("fields should be present", fields);

    LukeResponse.FieldInfo nameField = fields.get("name");
    assertNotNull("'name' field should be present", nameField);
    assertNotNull("field type should be present", nameField.getType());
    assertNotNull("schema flags should be present", nameField.getSchema());
    assertEquals(
        "aggregated docs count for 'name' should equal total docs",
        (long) NUM_DOCS,
        nameField.getDocs());

    LukeResponse.FieldInfo idField = fields.get("id");
    assertNotNull("'id' field should be present", idField);
    assertEquals("id field type should be string", "string", idField.getType());

    assertLukeXPath(
        new ModifiableSolrParams(),
        "//lst[@name='index']/long[@name='numDocs'][.='20']",
        "count(//lst[@name='shards']/lst)=2",
        "//lst[@name='fields']/lst[@name='name']/str[@name='type'][.='nametext']",
        "//lst[@name='fields']/lst[@name='name']/str[@name='schema']",
        "//lst[@name='fields']/lst[@name='name']/str[@name='index']",
        "//lst[@name='fields']/lst[@name='name']/long[@name='docs'][.='20']",
        "//lst[@name='fields']/lst[@name='id']/str[@name='type'][.='string']",
        "//lst[@name='fields']/lst[@name='id']/long[@name='docs'][.='20']");

    // --- Detailed per-shard stats (topTerms, histogram, distinct) ---
    ModifiableSolrParams detailedParams = new ModifiableSolrParams();
    detailedParams.set("fl", "name");
    detailedParams.set("numTerms", "5");

    LukeResponse detailedRsp = requestLuke(detailedParams);

    LukeResponse.FieldInfo detailedNameField = detailedRsp.getFieldInfo().get("name");
    assertNotNull("'name' field should be present", detailedNameField);
    assertNull("topTerms should NOT be in top-level fields", detailedNameField.getTopTerms());
    assertEquals("distinct should NOT be in top-level fields", 0, detailedNameField.getDistinct());

    Map<String, LukeResponse> detailedShardResponses = detailedRsp.getShardResponses();
    assertNotNull("shards section should be present", detailedShardResponses);

    assertLukeXPath(
        detailedParams,
        "/response/lst[@name='fields']/lst[@name='name']/str[@name='type'][.='nametext']",
        "/response/lst[@name='fields']/lst[@name='name']/long[@name='docs'][.='20']",
        "not(/response/lst[@name='fields']/lst[@name='name']/lst[@name='topTerms'])",
        "not(/response/lst[@name='fields']/lst[@name='name']/lst[@name='histogram'])",
        "not(/response/lst[@name='fields']/lst[@name='name']/int[@name='distinct'])",
        "//lst[@name='shards']/lst/lst[@name='fields']/lst[@name='name']/lst[@name='topTerms']",
        "//lst[@name='shards']/lst/lst[@name='fields']/lst[@name='name']/lst[@name='histogram']/int[@name='1']",
        "//lst[@name='shards']/lst/lst[@name='fields']/lst[@name='name']/int[@name='distinct']");

    // --- Doc lookup not found ---
    ModifiableSolrParams notFoundParams = new ModifiableSolrParams();
    notFoundParams.set("id", "999888777");

    LukeResponse notFoundRsp = requestLuke(notFoundParams);

    NamedList<Object> notFoundRaw = notFoundRsp.getResponse();
    assertNull("doc section should NOT be present for missing ID", notFoundRaw.get("doc"));

    assertLukeXPath(notFoundParams, "not(//lst[@name='doc'])");

    // --- Doc lookup found ---
    ModifiableSolrParams foundParams = new ModifiableSolrParams();
    foundParams.set("id", "0");

    assertLukeXPath(
        foundParams,
        "//lst[@name='doc']/int[@name='docId']",
        "//lst[@name='doc']/lst[@name='lucene']/lst[@name='id']/str[@name='type'][.='string']",
        "//lst[@name='doc']/lst[@name='lucene']/lst[@name='id']/str[@name='value'][.='0']",
        "//lst[@name='doc']/lst[@name='lucene']/lst[@name='name']/str[@name='type'][.='nametext']",
        "//lst[@name='doc']/lst[@name='lucene']/lst[@name='name']/str[@name='value'][.='name_0']",
        "//lst[@name='doc']/arr[@name='solr']/str[.='0']",
        "//lst[@name='doc']/arr[@name='solr']/str[.='name_0']",
        "//lst[@name='index']",
        "//lst[@name='info']");

    // --- Schema view ---
    ModifiableSolrParams schemaParams = new ModifiableSolrParams();
    schemaParams.set("show", "schema");

    assertLukeXPath(
        schemaParams,
        "//lst[@name='schema']/lst[@name='fields']/lst[@name='id']/str[@name='type'][.='string']",
        "//lst[@name='schema']/lst[@name='fields']/lst[@name='name']/str[@name='type'][.='nametext']",
        "//lst[@name='schema']/lst[@name='dynamicFields']/lst[@name='*_s']",
        "//lst[@name='schema']/str[@name='uniqueKeyField'][.='id']",
        "//lst[@name='schema']/lst[@name='types']/lst[@name='string']",
        "//lst[@name='schema']/lst[@name='types']/lst[@name='nametext']",
        "//lst[@name='schema']/lst[@name='similarity']",
        "not(/response/lst[@name='fields'])",
        "count(//lst[@name='shards']/lst)=2");
  }

  @Test
  @ShardsFixed(num = 12)
  public void testSparseShardsAndDeferredIndexFlags() throws Exception {
    // Index a single doc on shard 0
    index_specific(
        0, "id", "100", "name", "sparse test", "subject", "subject value", "cat_s", "category");
    commit();

    LukeResponse rsp = requestLuke();

    // Index-level stats
    assertEquals("numDocs should be 1", 1, (long) rsp.getNumDocs());
    assertTrue("maxDoc should be > 0", rsp.getMaxDoc() > 0);
    assertEquals("deletedDocs should be 0", 0L, (long) rsp.getDeletedDocs());

    Map<String, LukeResponse> shardResponses = rsp.getShardResponses();
    assertNotNull("shards section should be present", shardResponses);
    assertEquals("should have 12 shard entries", 12, shardResponses.size());

    long sumShardDocs = 0;
    for (Map.Entry<String, LukeResponse> entry : shardResponses.entrySet()) {
      LukeResponse shardLuke = entry.getValue();
      assertNotNull("each shard should have numDocs", shardLuke.getNumDocs());
      sumShardDocs += shardLuke.getNumDocs();
    }
    assertEquals("sum of per-shard numDocs should be 1", 1, sumShardDocs);

    // Field-level checks
    Map<String, LukeResponse.FieldInfo> fields = rsp.getFieldInfo();
    assertNotNull("fields should be present", fields);

    LukeResponse.FieldInfo idField = fields.get("id");
    assertNotNull("'id' field should be present", idField);
    assertEquals("id type", "string", idField.getType());
    assertNotNull("id schema flags", idField.getSchema());

    LukeResponse.FieldInfo nameField = fields.get("name");
    assertNotNull("'name' field should be present", nameField);
    assertNotNull("name type", nameField.getType());
    assertNotNull("name schema flags", nameField.getSchema());
    assertEquals("name docs should be 1", 1, nameField.getDocs());

    // Dynamic field — should have dynamicBase in extras
    LukeResponse.FieldInfo catField = fields.get("cat_s");
    assertNotNull("'cat_s' field should be present", catField);
    assertNotNull("cat_s type", catField.getType());
    assertNotNull("cat_s dynamicBase", catField.getExtras().get("dynamicBase"));

    assertLukeXPath(
        new ModifiableSolrParams(),
        "//lst[@name='index']/long[@name='numDocs'][.='1']",
        "//lst[@name='index']/long[@name='deletedDocs'][.='0']",
        "count(//lst[@name='shards']/lst)=12",
        "//lst[@name='fields']/lst[@name='name']/str[@name='type'][.='nametext']",
        "//lst[@name='fields']/lst[@name='name']/str[@name='schema']",
        "//lst[@name='fields']/lst[@name='name']/str[@name='index']",
        "//lst[@name='fields']/lst[@name='name']/long[@name='docs'][.='1']",
        "//lst[@name='fields']/lst[@name='cat_s']/str[@name='type'][.='string']",
        "//lst[@name='fields']/lst[@name='cat_s']/str[@name='dynamicBase'][.='*_s']",
        "//lst[@name='fields']/lst[@name='cat_s']/long[@name='docs'][.='1']");

    // Index docs with the target field across shards, plus anchor docs without it.
    // Use numeric IDs (the default test schema copies id to integer fields).
    // Target docs get even IDs starting at 1000, anchor docs get odd IDs.
    for (int i = 0; i < 12 * 4; i++) {
      index("id", String.valueOf(1000 + i * 2), "flag_target_s", "value_" + i);
      index("id", String.valueOf(1001 + i * 2), "name", "anchor");
    }
    commit();

    // Delete all target docs except the first one, using per-shard deletes.
    // Then optimize to force segment merge — expunges soft-deleted docs so
    // Terms.getDocCount() (which backs docs) reflects only live docs.
    for (int i = 0; i < clients.size(); i++) {
      clients.get(i).deleteByQuery("flag_target_s:* AND -id:1000");
      clients.get(i).optimize();
    }
    controlClient.deleteByQuery("flag_target_s:* AND -id:1000");
    controlClient.optimize();

    ModifiableSolrParams flagParams = new ModifiableSolrParams();
    flagParams.set("fl", "flag_target_s");

    LukeResponse flagRsp = requestLuke(flagParams);

    Map<String, LukeResponse.FieldInfo> flagFields = flagRsp.getFieldInfo();
    assertNotNull("fields should be present", flagFields);
    LukeResponse.FieldInfo targetField = flagFields.get("flag_target_s");
    assertNotNull("'flag_target_s' field should be present", targetField);

    assertLukeXPath(
        flagParams,
        "//lst[@name='fields']/lst[@name='flag_target_s']/str[@name='type'][.='string']",
        "//lst[@name='fields']/lst[@name='flag_target_s']/str[@name='dynamicBase'][.='*_s']",
        "//lst[@name='fields']/lst[@name='flag_target_s']/str[@name='index']",
        "//lst[@name='fields']/lst[@name='flag_target_s']/long[@name='docs'][.='1']");
  }

  @Test
  @ShardsFixed(num = 2)
  public void testDistributedDocLookupDuplicateId() throws Exception {
    String dupId = "99999";

    // Write the same document directly to two shard cores via UpdateHandler,
    // completely bypassing the distributed update processor chain.
    for (int i = 0; i < 2; i++) {
      try (SolrCore core = jettys.get(i).getCoreContainer().getCore("collection1")) {
        SolrInputDocument solrDoc = new SolrInputDocument();
        solrDoc.addField("id", dupId);
        solrDoc.addField("name", "dup_copy_" + i);

        AddUpdateCommand addCmd =
            new AddUpdateCommand(new SolrQueryRequestBase(core, new ModifiableSolrParams()) {});
        addCmd.solrDoc = solrDoc;
        core.getUpdateHandler().addDoc(addCmd);

        CommitUpdateCommand commitCmd =
            new CommitUpdateCommand(
                new SolrQueryRequestBase(core, new ModifiableSolrParams()) {}, false);
        commitCmd.waitSearcher = true;
        core.getUpdateHandler().commit(commitCmd);
      }
    }

    // Distributed Luke doc lookup should detect the corruption
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("id", dupId);

    Exception ex = expectThrows(Exception.class, () -> requestLuke(params));
    String fullMessage = SolrException.getRootCause(ex).getMessage();
    assertTrue(
        "exception should mention duplicate/corrupt index: " + fullMessage,
        fullMessage.contains("found on multiple shards"));
  }

  @Test
  @ShardsFixed(num = 1)
  public void testSingleShardViaParamStillDistributes() throws Exception {
    index("id", "500", "name", "test_name");
    commit();

    // Pass the shards param with a single shard — should still fan out to it
    // rather than incorrectly falling through to local mode
    LukeRequest req = new LukeRequest(params("shards", shards, "shards.info", "true"));
    req.setNumTerms(0);
    LukeResponse rsp = req.process(controlClient);

    assertNotNull("index info should be present", rsp.getIndexInfo());
    assertEquals("should see the 1 doc we indexed", 1, (long) rsp.getNumDocs());
    assertNotNull(
        "shards section should be present when shards.info=true", rsp.getShardResponses());
    assertEquals("should have 1 shard entry", 1, rsp.getShardResponses().size());

    // Without shards.info, shards section should be absent
    req = new LukeRequest(params("shards", shards));
    req.setNumTerms(0);
    rsp = req.process(controlClient);
    assertNotNull("index info should be present", rsp.getIndexInfo());
    assertEquals("should see the 1 doc we indexed", 1, (long) rsp.getNumDocs());
    assertNull("shards section should be absent without shards.info", rsp.getShardResponses());
  }
}
