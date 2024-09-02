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

package org.apache.solr.opentelemetry;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.sdk.testing.junit4.OpenTelemetryRule;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.solr.util.tracing.TraceUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class TestDistributedTracing extends SolrCloudTestCase {

  private static final String COLLECTION = "collection1";

  @ClassRule public static OpenTelemetryRule otelRule = OpenTelemetryRule.create();

  @BeforeClass
  public static void setupCluster() throws Exception {
    TraceUtils.resetRecordingFlag(); // ensure spans are "recorded"

    configureCluster(4)
        .addConfig("config", TEST_PATH().resolve("collection1").resolve("conf"))
        .withSolrXml(TEST_PATH().resolve("solr.xml"))
        .configure();

    assertNotEquals(
        "Expecting active otel, not noop impl",
        TracerProvider.noop(),
        GlobalOpenTelemetry.get().getTracerProvider());

    CollectionAdminRequest.createCollection(COLLECTION, "config", 2, 2)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 2, 4);
  }

  @Before
  private void resetSpanData() {
    getAndClearSpans();
  }

  @Test
  public void test() throws IOException, SolrServerException {
    // TODO it would be clearer if we could compare the complete Span tree between reality
    // and what we assert it looks like in a structured visual way.
    CloudSolrClient cloudClient = cluster.getSolrClient();

    // Indexing
    cloudClient.add(COLLECTION, sdoc("id", "1"));
    var finishedSpans = getAndClearSpans();
    finishedSpans.removeIf(
        span ->
            span.getAttributes().get(TraceUtils.TAG_HTTP_URL) == null
                || !span.getAttributes().get(TraceUtils.TAG_HTTP_URL).endsWith("/update"));
    assertEquals(2, finishedSpans.size());
    assertOneSpanIsChildOfAnother(finishedSpans);
    // core because cloudClient routes to core
    assertEquals("post:/{core}/update", finishedSpans.get(0).getName());
    assertCoreName(finishedSpans.get(0), COLLECTION);

    cloudClient.add(COLLECTION, sdoc("id", "2"));
    cloudClient.add(COLLECTION, sdoc("id", "3"));
    cloudClient.add(COLLECTION, sdoc("id", "4"));
    cloudClient.commit(COLLECTION);
    getAndClearSpans();

    // Searching
    cloudClient.query(COLLECTION, new SolrQuery("*:*"));
    finishedSpans = getAndClearSpans();
    finishedSpans.removeIf(
        span ->
            span.getAttributes().get(TraceUtils.TAG_HTTP_URL) == null
                || !span.getAttributes().get(TraceUtils.TAG_HTTP_URL).endsWith("/select"));
    // one from client to server, 2 for execute query, 2 for fetching documents
    assertEquals(5, finishedSpans.size());
    var parentTraceId = getRootTraceId(finishedSpans);
    for (var span : finishedSpans) {
      if (isRootSpan(span)) {
        continue;
      }
      assertEquals(span.getParentSpanContext().getTraceId(), parentTraceId);
      assertEquals(span.getTraceId(), parentTraceId);
    }
    assertEquals("get:/{core}/select", finishedSpans.get(0).getName());
    assertCoreName(finishedSpans.get(0), COLLECTION);
  }

  @Test
  public void testAdminApi() throws Exception {
    CloudSolrClient cloudClient = cluster.getSolrClient();

    cloudClient.request(new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/metrics"));
    var finishedSpans = getAndClearSpans();
    assertEquals("get:/admin/metrics", finishedSpans.get(0).getName());

    CollectionAdminRequest.listCollections(cloudClient);
    finishedSpans = getAndClearSpans();
    assertEquals("list:/admin/collections", finishedSpans.get(0).getName());
  }

  @Test
  public void testV2Api() throws Exception {
    CloudSolrClient cloudClient = cluster.getSolrClient();

    new V2Request.Builder("/collections/" + COLLECTION + "/reload")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{}")
        .build()
        .process(cloudClient);
    var finishedSpans = getAndClearSpans();
    assertEquals("post:/collections/{collection}/reload", finishedSpans.get(0).getName());
    assertCollectionName(finishedSpans.get(0), COLLECTION);

    new V2Request.Builder("/c/" + COLLECTION + "/update/json")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{\n" + " \"id\" : \"9\"\n" + "}")
        .withParams(params("commit", "true"))
        .build()
        .process(cloudClient);
    finishedSpans = getAndClearSpans();
    assertEquals("post:/c/{collection}/update/json", finishedSpans.get(0).getName());
    assertCollectionName(finishedSpans.get(0), COLLECTION);

    final V2Response v2Response =
        new V2Request.Builder("/c/" + COLLECTION + "/select")
            .withMethod(SolrRequest.METHOD.GET)
            .withParams(params("q", "id:9"))
            .build()
            .process(cloudClient);
    finishedSpans = getAndClearSpans();
    assertEquals("get:/c/{collection}/select", finishedSpans.get(0).getName());
    assertCollectionName(finishedSpans.get(0), COLLECTION);
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
      if (isRootSpan(span)) {
        continue;
      }
      assertEquals(span.getParentSpanContext().getTraceId(), parentTraceId);
      assertEquals(span.getTraceId(), parentTraceId);
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
    // 1. api call "name=create:/admin/collections". db.instance=testInternalCollectionApiCommands
    // - unique traceId unrelated to the internal trace id generated for the operation
    // 2. internal CollectionApiCommand "name=CreateCollectionCmd"
    // db.instance=testInternalCollectionApiCommands
    // - this will be the parent span, all following spans will have the same traceId
    //
    // 3..6 (4 times) name=post:/admin/cores
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
    assertCollectionName(s0, collection);
    assertEquals("create:/admin/collections", s0.getName());

    Map<String, Integer> ops = new HashMap<>();
    assertEquals(7, finishedSpans.size());
    var parentTraceId = getRootTraceId(finishedSpans);
    for (var span : finishedSpans) {
      if (isRootSpan(span)) {
        assertCollectionName(span, collection);
      } else {
        assertEquals(span.getParentSpanContext().getTraceId(), parentTraceId);
        assertCoreName(span, collection);
      }
      assertEquals(span.getTraceId(), parentTraceId);
      ops.put(span.getName(), ops.getOrDefault(span.getName(), 0) + 1);
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
    // 1. api call "name=delete:/admin/collections". db.instance=testInternalCollectionApiCommands
    // - unique traceId unrelated to the internal trace id generated for the operation
    // 2. internal CollectionApiCommand "name=DeleteCollectionCmd"
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
    assertCollectionName(s0, collection);
    assertEquals("delete:/admin/collections", s0.getName());

    Map<String, Integer> ops = new HashMap<>();
    assertEquals(5, finishedSpans.size());
    var parentTraceId = getRootTraceId(finishedSpans);
    for (var span : finishedSpans) {
      if (isRootSpan(span)) {
        assertCollectionName(span, collection);
      } else {
        assertEquals(span.getParentSpanContext().getTraceId(), parentTraceId);
        assertCoreName(span, collection);
      }
      assertEquals(span.getTraceId(), parentTraceId);
      ops.put(span.getName(), ops.getOrDefault(span.getName(), 0) + 1);
    }
    var expectedOps = Map.of("DeleteCollectionCmd", 1, "post:/admin/cores", 4);
    assertEquals(expectedOps, ops);
  }

  private static boolean isRootSpan(SpanData span) {
    return !span.getParentSpanContext().isValid();
  }

  private static void assertCollectionName(SpanData span, String collection) {
    assertEquals(collection, span.getAttributes().get(TraceUtils.TAG_DB));
  }

  private static void assertCoreName(SpanData span, String collection) {
    assertTrue(span.getAttributes().get(TraceUtils.TAG_DB).startsWith(collection + "_"));
  }

  private void assertOneSpanIsChildOfAnother(List<SpanData> finishedSpans) {
    SpanData child = finishedSpans.get(0);
    SpanData parent = finishedSpans.get(1);
    if (isRootSpan(child)) {
      var temp = parent;
      parent = child;
      child = temp;
    }
    assertEquals(child.getParentSpanContext().getTraceId(), parent.getTraceId());
    assertEquals(child.getTraceId(), parent.getTraceId());
  }

  private static List<SpanData> getAndClearSpans() {
    List<SpanData> result = new ArrayList<>(otelRule.getSpans());
    Collections.reverse(result); // nicer to see spans chronologically
    otelRule.clearSpans();
    return result;
  }

  private String getRootTraceId(List<SpanData> finishedSpans) {
    assertEquals(1, finishedSpans.stream().filter(TestDistributedTracing::isRootSpan).count());
    return finishedSpans.stream()
        .filter(TestDistributedTracing::isRootSpan)
        .findFirst()
        .get()
        .getSpanContext()
        .getTraceId();
  }
}
