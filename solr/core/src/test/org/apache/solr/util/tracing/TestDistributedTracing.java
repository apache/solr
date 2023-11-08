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

import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.GlobalTracer;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.util.LogLevel;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@LogLevel("org.apache.solr.core.TracerConfigurator=trace")
public class TestDistributedTracing extends SolrCloudTestCase {
  private static final String COLLECTION = "collection1";

  static MockTracer tracer;

  @BeforeClass
  public static void beforeTest() throws Exception {
    tracer = new MockTracer();
    assertTrue(GlobalTracer.registerIfAbsent(tracer));

    configureCluster(4)
        .addConfig(
            "config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .withTraceIdGenerationDisabled()
        .configure();
    CollectionAdminRequest.createCollection(COLLECTION, "config", 2, 2)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 2, 4);
  }

  @Before
  private void resetSpanData() {
    getAndClearSpans();
  }

  @AfterClass
  public static void afterTest() {
    tracer = null;
  }

  @Test
  public void test() throws IOException, SolrServerException {
    // TODO it would be clearer if we could compare the complete Span tree between reality
    //   and what we assert it looks like in a structured visual way.

    CloudSolrClient cloudClient = cluster.getSolrClient();
    List<MockSpan> finishedSpans;

    // Indexing
    cloudClient.add(COLLECTION, sdoc("id", "1"));
    finishedSpans = getAndClearSpans();

    finishedSpans.removeIf(
        x -> x.tags() != null && !x.tags().get("http.url").toString().endsWith("/update"));
    assertEquals(2, finishedSpans.size());
    assertOneSpanIsChildOfAnother(finishedSpans);
    // core because cloudClient routes to core
    assertEquals("post:/{core}/update", finishedSpans.get(0).operationName());
    assertDbInstanceCore(finishedSpans.get(0), COLLECTION);

    cloudClient.add(COLLECTION, sdoc("id", "2"));
    cloudClient.add(COLLECTION, sdoc("id", "3"));
    cloudClient.add(COLLECTION, sdoc("id", "4"));
    cloudClient.commit(COLLECTION);
    getAndClearSpans();

    // Searching
    cloudClient.query(COLLECTION, new SolrQuery("*:*"));
    finishedSpans = getAndClearSpans();
    finishedSpans.removeIf(
        x -> x.tags() != null && !x.tags().get("http.url").toString().endsWith("/select"));
    // one from client to server, 2 for execute query, 2 for fetching documents
    assertEquals(5, finishedSpans.size());
    var parentId = getRootTraceId(finishedSpans);
    for (MockSpan span : finishedSpans) {
      if (span.parentId() != 0 && parentId != span.parentId()) {
        fail("All spans must belong to single span, but:" + finishedSpans);
      }
    }
    assertEquals("get:/{core}/select", finishedSpans.get(0).operationName());
    assertDbInstanceCore(finishedSpans.get(0), COLLECTION);
  }

  @Test
  public void testAdminApi() throws Exception {
    CloudSolrClient cloudClient = cluster.getSolrClient();
    List<MockSpan> finishedSpans;

    // Admin API call
    cloudClient.request(new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/metrics"));
    finishedSpans = getAndClearSpans();
    assertEquals("get:/admin/metrics", finishedSpans.get(0).operationName());

    CollectionAdminRequest.listCollections(cloudClient);
    finishedSpans = getAndClearSpans();
    assertEquals("list:/admin/collections", finishedSpans.get(0).operationName());
  }

  @Test
  public void testV2Api() throws Exception {
    CloudSolrClient cloudClient = cluster.getSolrClient();
    List<MockSpan> finishedSpans;

    new V2Request.Builder("/collections/" + COLLECTION + "/reload")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{}")
        .build()
        .process(cloudClient);
    finishedSpans = getAndClearSpans();
    assertEquals("post:/collections/{collection}/reload", finishedSpans.get(0).operationName());
    assertDbInstanceColl(finishedSpans.get(0), COLLECTION);

    new V2Request.Builder("/c/" + COLLECTION + "/update/json")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{\n" + " \"id\" : \"9\"\n" + "}")
        .withParams(params("commit", "true"))
        .build()
        .process(cloudClient);
    finishedSpans = getAndClearSpans();
    assertEquals("post:/c/{collection}/update/json", finishedSpans.get(0).operationName());
    assertDbInstanceColl(finishedSpans.get(0), COLLECTION);

    final V2Response v2Response =
        new V2Request.Builder("/c/" + COLLECTION + "/select")
            .withMethod(SolrRequest.METHOD.GET)
            .withParams(params("q", "id:9"))
            .build()
            .process(cloudClient);
    finishedSpans = getAndClearSpans();
    assertEquals("get:/c/{collection}/select", finishedSpans.get(0).operationName());
    assertDbInstanceColl(finishedSpans.get(0), COLLECTION);
    assertEquals(1, ((SolrDocumentList) v2Response.getResponse().get("response")).getNumFound());
  }

  /**
   * Best effort test of the apache http client tracing. the test assumes the request uses the http
   * client but there is no way to enforce it, so when the api will be rewritten this test will
   * become obsolete
   */
  @Test
  public void testApacheClient() throws Exception {
    CollectionAdminRequest.ColStatus a1 = CollectionAdminRequest.collectionStatus(COLLECTION);
    CollectionAdminResponse r1 = a1.process(cluster.getSolrClient());
    assertEquals(0, r1.getStatus());
    var finishedSpans = getAndClearSpans();
    var parentTraceId = getRootTraceId(finishedSpans);
    for (var span : finishedSpans) {
      if (span.parentId() == 0) {
        continue;
      }
      assertEquals(span.parentId(), parentTraceId);
    }
  }

  @Test
  public void testInternalCollectionApiCommands() throws Exception {
    String collecton = "testInternalCollectionApiCommands";
    verifyCollectionCreation(collecton);
    verifyCollectionDeletion(collecton);
  }

  private void verifyCollectionCreation(String collection) throws Exception {
    var a1 = CollectionAdminRequest.createCollection(collection, 2, 2);
    CollectionAdminResponse r1 = a1.process(cluster.getSolrClient());
    assertEquals(0, r1.getStatus());

    // Expecting 8 spans:
    // 1. api call 'operationName:"create:/admin/collections"',
    // db.instance=testInternalCollectionApiCommands
    // - unique traceId unrelated to the internal trace id generated for the operation
    // 2. internal CollectionApiCommand 'operationName:"CreateCollectionCmd"'
    // db.instance=testInternalCollectionApiCommands
    // - this will be the parent span, all following spans will have the same traceId
    //
    // 3..6 (4 times) operationName:"post:/admin/cores"
    // db.instance=testInternalCollectionApiCommands_shard1_replica_n2
    // db.instance=testInternalCollectionApiCommands_shard2_replica_n4
    // db.instance=testInternalCollectionApiCommands_shard2_replica_n1
    // db.instance=testInternalCollectionApiCommands_shard1_replica_n6
    //
    // 7..8 (2 times) name=post:/{core}/get
    // db.instance=testInternalCollectionApiCommands_shard2_replica_n4
    // db.instance=testInternalCollectionApiCommands_shard1_replica_n2

    var finishedSpans = getAndClearSpans();
    var s0 = finishedSpans.remove(0);
    assertDbInstanceColl(s0, collection);
    assertEquals("create:/admin/collections", s0.operationName());

    Map<String, Integer> ops = new HashMap<>();
    assertEquals(7, finishedSpans.size());
    System.err.println("finishedSpans " + finishedSpans);

    var parentTraceId = finishedSpans.get(0).context().traceId();
    var parentId = finishedSpans.get(0).context().spanId();
    for (var span : finishedSpans) {
      if (span.context().spanId() == parentId) {
        assertDbInstanceColl(span, collection);
      } else {
        assertDbInstanceCore(span, collection);
      }
      assertEquals(span.context().traceId(), parentTraceId);
      ops.put(span.operationName(), ops.getOrDefault(span.operationName(), 0) + 1);
    }
    var expectedOps =
        Map.of("CreateCollectionCmd", 1, "post:/admin/cores", 4, "post:/{core}/get", 2);
    assertEquals(expectedOps, ops);
  }

  private void verifyCollectionDeletion(String collection) throws Exception {
    var a1 = CollectionAdminRequest.deleteCollection(collection);
    CollectionAdminResponse r1 = a1.process(cluster.getSolrClient());
    assertEquals(0, r1.getStatus());

    // Expecting 6 spans:
    // 1. api call 'operationName:"delete:/admin/collections"',
    // db.instance=testInternalCollectionApiCommands
    // - unique traceId unrelated to the internal trace id generated for the operation
    // 2. internal CollectionApiCommand 'operationName:"DeleteCollectionCmd"'
    // db.instance=testInternalCollectionApiCommands
    // - this will be the parent span, all following spans will have the same traceId
    //
    // 3..6 (4 times) name=post:/admin/cores
    // db.instance=testInternalCollectionApiCommands_shard2_replica_n1
    // db.instance=testInternalCollectionApiCommands_shard1_replica_n2
    // db.instance=testInternalCollectionApiCommands_shard2_replica_n4
    // db.instance=testInternalCollectionApiCommands_shard1_replica_n6

    var finishedSpans = getAndClearSpans();
    var s0 = finishedSpans.remove(0);
    assertDbInstanceColl(s0, collection);
    assertEquals("delete:/admin/collections", s0.operationName());

    Map<String, Integer> ops = new HashMap<>();
    assertEquals(5, finishedSpans.size());
    var parentTraceId = finishedSpans.get(0).context().traceId();
    var parentId = finishedSpans.get(0).context().spanId();
    for (var span : finishedSpans) {
      if (span.context().spanId() == parentId) {
        assertDbInstanceColl(span, collection);
      } else {
        assertDbInstanceCore(span, collection);
      }
      assertEquals(span.context().traceId(), parentTraceId);
      ops.put(span.operationName(), ops.getOrDefault(span.operationName(), 0) + 1);
    }
    var expectedOps = Map.of("DeleteCollectionCmd", 1, "post:/admin/cores", 4);
    assertEquals(expectedOps, ops);
  }

  private static void assertDbInstanceColl(MockSpan mockSpan, String collection) {
    MatcherAssert.assertThat(mockSpan.tags().get("db.instance"), Matchers.equalTo(collection));
  }

  private static void assertDbInstanceCore(MockSpan mockSpan, String collection) {
    MatcherAssert.assertThat(
        (String) mockSpan.tags().get("db.instance"), Matchers.startsWith(collection + "_"));
  }

  private void assertOneSpanIsChildOfAnother(List<MockSpan> finishedSpans) {
    MockSpan child = finishedSpans.get(0);
    MockSpan parent = finishedSpans.get(1);
    if (child.parentId() == 0) {
      MockSpan temp = parent;
      parent = child;
      child = temp;
    }

    assertEquals(child.parentId(), parent.context().spanId());
  }

  private List<MockSpan> getAndClearSpans() {
    List<MockSpan> result = tracer.finishedSpans(); // returns a mutable copy
    Collections.reverse(result); // nicer to see spans chronologically
    tracer.reset();
    return result;
  }

  private long getRootTraceId(List<MockSpan> finishedSpans) {
    assertEquals(1, finishedSpans.stream().filter(s -> s.parentId() == 0).count());
    return finishedSpans.stream()
        .filter(s -> s.parentId() == 0)
        .collect(Collectors.toList())
        .get(0)
        .context()
        .spanId();
  }
}
