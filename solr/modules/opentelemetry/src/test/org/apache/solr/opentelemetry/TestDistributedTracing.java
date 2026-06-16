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

import com.carrotsearch.randomizedtesting.annotations.Seed;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.MetricsRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.RetryUtil;
import org.apache.solr.util.stats.MetricUtils;
import org.apache.solr.util.tracing.TraceUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@Seed("0") // don't want randomization when testing observability
public class TestDistributedTracing extends SolrCloudTestCase {

  private static final String COLLECTION = "collection1";

  @BeforeClass
  public static void setupCluster() throws Exception {
    // force early init
    CustomTestOtelTracerConfigurator.prepareForTest();
    // HTTP 2 clients can do things more asynchronously, leading to less determinism in what we test
    System.setProperty("solr.http1", "true");

    configureCluster(4)
        .addConfig("config", TEST_PATH().resolve("collection1").resolve("conf"))
        .withSolrXml(TEST_PATH().resolve("solr.xml"))
        .withTraceIdGenerationDisabled()
        .withOverseer(true) // some assertions assume overseer
        .configure();

    assertNotEquals(
        "Expecting active otel, not noop impl",
        TracerProvider.noop(),
        GlobalOpenTelemetry.get().getTracerProvider());

    // Create collection with explicit replica placement for deterministic leader assignment.
    // First replica on each shard becomes the leader.
    String node0 = cluster.getJettySolrRunner(0).getNodeName();
    String node1 = cluster.getJettySolrRunner(1).getNodeName();
    String node2 = cluster.getJettySolrRunner(2).getNodeName();
    String node3 = cluster.getJettySolrRunner(3).getNodeName();
    CollectionAdminRequest.createCollection(COLLECTION, "config", 2, 1)
        .setCreateNodeSet("EMPTY")
        .process(cluster.getSolrClient());
    // shard1: node0 = leader, node1 = follower
    CollectionAdminRequest.addReplicaToShard(COLLECTION, "shard1")
        .setNode(node0)
        .process(cluster.getSolrClient());
    CollectionAdminRequest.addReplicaToShard(COLLECTION, "shard1")
        .setNode(node1)
        .process(cluster.getSolrClient());
    // shard2: node2 = leader, node3 = follower
    CollectionAdminRequest.addReplicaToShard(COLLECTION, "shard2")
        .setNode(node2)
        .process(cluster.getSolrClient());
    CollectionAdminRequest.addReplicaToShard(COLLECTION, "shard2")
        .setNode(node3)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 2, 4);
  }

  @AfterClass
  public static void afterClass() {
    CustomTestOtelTracerConfigurator.resetForTest();
  }

  @Before
  private void resetSpanData() {
    getAndClearSpans();
  }

  @Test
  public void test() throws Exception {
    var verifier = new GoldFileTraceVerifier(getClass(), "test");
    // TODO use a CloudSolrClient.  However it's not yet deterministic due to use of random not
    //   aligned to the test seed.
    var client = cluster.getJettySolrRunner(0).getSolrClient();

    // Indexing
    new UpdateRequest()
        .add(List.of(sdoc("id", "1"), sdoc("id", "2"), sdoc("id", "3"), sdoc("id", "4")))
        .commit(client, COLLECTION);
    verifier.verifyPhase();

    // Searching
    client.query(COLLECTION, new SolrQuery("*:*"));
    verifier.verifyPhase();

    verifier.done();
  }

  @Test
  public void testAdminApi() throws Exception {
    var verifier = new GoldFileTraceVerifier(getClass(), "testAdminApi");
    // TODO use a CloudSolrClient.  However it's not yet deterministic due to use of random not
    //   aligned to the test seed.
    var client = cluster.getJettySolrRunner(0).getSolrClient();

    MetricsRequest request = new MetricsRequest();
    request.setResponseParser(new InputStreamResponseParser(MetricUtils.PROMETHEUS_METRICS_WT));
    NamedList<Object> rsp = client.request(request);
    ((InputStream) rsp.get("stream")).close();
    verifier.verifyPhase();

    CollectionAdminRequest.listCollections(client);
    verifier.verifyPhase();

    verifier.done();
  }

  @Test
  public void testV2Api() throws Exception {
    var verifier = new GoldFileTraceVerifier(getClass(), "testV2Api");
    // TODO use a CloudSolrClient.  However it's not yet deterministic due to use of random not
    //   aligned to the test seed.
    var client = cluster.getJettySolrRunner(0).getSolrClient();

    new V2Request.Builder("/collections/" + COLLECTION + "/reload")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{}")
        .build()
        .process(client);
    verifier.verifyPhase();

    new V2Request.Builder("/c/" + COLLECTION + "/update/json")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{\"id\":\"9\"}")
        .withParams(params("commit", "true"))
        .build()
        .process(client);
    verifier.verifyPhase();

    final V2Response v2Response =
        new V2Request.Builder("/c/" + COLLECTION + "/select")
            .withMethod(SolrRequest.METHOD.GET)
            .withParams(params("q", "id:9"))
            .build()
            .process(client);
    verifier.verifyPhase();
    assertEquals(1, ((SolrDocumentList) v2Response.getResponse().get("response")).getNumFound());

    verifier.done();
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
    // 7..8 (2 times) name=post:/{core}/get (FingerPrinting to get versions from non-leaders)
    // db.instance=testInternalCollectionApiCommands_shard2_replica_n1
    // db.instance=testInternalCollectionApiCommands_shard1_replica_n6
    //
    // 9..10 (2 times) name=post:/{core}/get (PeerSync request to non-leaders)
    // db.instance=testInternalCollectionApiCommands_shard2_replica_n1
    // db.instance=testInternalCollectionApiCommands_shard1_replica_n6
    //
    // 11..12 (2 times) name=post:/{core}/get (FingerPrinting to get versions from leaders PeerSync)
    // db.instance=testInternalCollectionApiCommands_shard2_replica_n4
    // db.instance=testInternalCollectionApiCommands_shard1_replica_n2

    var finishedSpans = getAndClearSpans(1);
    var s0 = finishedSpans.remove(0);
    assertCollectionName(s0, collection);
    assertEquals("create:/admin/collections", s0.getName());

    Map<String, Integer> ops = new HashMap<>();
    assertEquals(11, finishedSpans.size());
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
        Map.of("CreateCollectionCmd", 1, "post:/admin/cores", 4, "post:/{core}/get", 6);
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

    var finishedSpans = getAndClearSpans(1);
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


  static List<SpanData> getAndClearSpans() {
    return getAndClearSpans(0);
  }

  static List<SpanData> getAndClearSpans(int minExpected) {
    InMemorySpanExporter exporter = CustomTestOtelTracerConfigurator.getInMemorySpanExporter();
    try {
      RetryUtil.retryUntil(
          "Timed out waiting for " + minExpected + " span(s)",
          250,
          20,
          TimeUnit.MILLISECONDS,
          () -> exporter.getFinishedSpanItems().size() >= minExpected);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    List<SpanData> result = new ArrayList<>(exporter.getFinishedSpanItems());
    Collections.reverse(result); // nicer to see spans chronologically
    exporter.reset();
    return result;
  }

  static String getRootTraceId(List<SpanData> finishedSpans) {
    assertEquals(1, finishedSpans.stream().filter(TestDistributedTracing::isRootSpan).count());
    return finishedSpans.stream()
        .filter(TestDistributedTracing::isRootSpan)
        .findFirst()
        .get()
        .getSpanContext()
        .getTraceId();
  }
}
