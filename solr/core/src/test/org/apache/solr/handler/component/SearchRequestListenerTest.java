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
package org.apache.solr.handler.component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrPing;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.cloud.ConfigRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.CallerSpecificQueryLimit;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.ThreadCpuTimer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SearchRequestListenerTest extends SolrCloudTestCase {
  private static final String COLLECTION = "listener";
  private static final String DISPATCH_COLLECTION = "listener_dispatch";
  private static final int NUM_SHARDS = 2;
  private static final long DELIVERY_TIMEOUT_MS = 30_000;

  private static final long SLOW_LISTENER_MS = 50;

  private final List<SearchRequestListener> listeners = new ArrayList<>();

  private final List<RecordingListener> recorders = new ArrayList<>();

  private static class RecordingListener implements SearchRequestListener {
    final String node;
    final AtomicInteger captures = new AtomicInteger();
    final BlockingQueue<SearchRequestInfo> delivered = new LinkedBlockingQueue<>();

    RecordingListener(String node) {
      this.node = node;
    }

    @Override
    public void onRequestEnd(SearchRequestInfo info) {
      captures.incrementAndGet();
      delivered.add(info);
    }
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .addConfig("dispatch", configset("cloud-minimal"))
        .configure();
    createCollection(COLLECTION);
    createCollection(DISPATCH_COLLECTION, "dispatch");
    // With /select registered for v2 only, EmbeddedSolrServer falls back to the default handler
    cluster
        .getSolrClient()
        .request(
            new ConfigRequest(
                "{'update-requesthandler':"
                    + " {'name': '/select', 'class': 'solr.SearchHandler', 'registerPath': '/v2'},"
                    + " 'add-requesthandler': ["
                    + "{'name': '/v2-search', 'class': 'solr.SearchHandler', 'registerPath': '/v2'},"
                    + "{'name': 'standard', 'class': 'solr.SearchHandler'}]}"),
            DISPATCH_COLLECTION);
  }

  @Before
  public void registerListeners() {
    addListeners(false);
  }

  @After
  public void removeListeners() {
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      listeners.forEach(jetty.getCoreContainer()::removeSearchRequestListener);
    }
  }

  private static void createCollection(String name) throws Exception {
    createCollection(name, "conf");
  }

  private static void createCollection(String name, String config) throws Exception {
    CollectionAdminRequest.createCollection(name, config, NUM_SHARDS, 1)
        .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    cluster.waitForActiveCollection(name, NUM_SHARDS, NUM_SHARDS);
    new UpdateRequest()
        .add("id", "1")
        .add("id", "2")
        .add("id", "3")
        .commit(cluster.getSolrClient(), name);
  }

  private void addListeners(boolean withThrowingListener) {
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      String node = jetty.getNodeName();
      if (withThrowingListener) {
        addListener(
            jetty,
            info -> {
              throw new RuntimeException("listener failure");
            });
      }
      RecordingListener recorder = new RecordingListener(node);
      recorders.add(recorder);
      addListener(jetty, recorder);
    }
  }

  private void resetListeners(boolean withThrowingListener) {
    removeListeners();
    listeners.clear();
    recorders.clear();
    addListeners(withThrowingListener);
  }

  private void addListener(JettySolrRunner jetty, SearchRequestListener listener) {
    listeners.add(listener);
    jetty.getCoreContainer().addSearchRequestListener(listener);
  }

  private static SolrClient nodeClient() {
    return cluster.getJettySolrRunner(0).getSolrClient();
  }

  private static QueryResponse query(String collection, String q) throws Exception {
    return nodeClient().query(collection, new SolrQuery(q));
  }

  private int captures() {
    return recorders.stream().mapToInt(r -> r.captures.get()).sum();
  }

  private RecordingListener captured() {
    assertEquals(1, captures());
    return recorders.stream().filter(r -> r.captures.get() == 1).findFirst().orElseThrow();
  }

  private SearchRequestInfo delivered() throws InterruptedException {
    SearchRequestInfo info = captured().delivered.poll(DELIVERY_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    assertNotNull("no event delivered", info);
    return info;
  }

  @Test
  public void testCalledOnceOnCoordinator() throws Exception {
    query(COLLECTION, "*:*");
    assertEquals(cluster.getJettySolrRunner(0).getNodeName(), captured().node);
  }

  @Test
  public void testDistributedSubQueriesNotReported() throws Exception {
    assertSubQueriesNotReported(true);
  }

  @Test
  public void testLocalSubQueriesNotReported() throws Exception {
    assertSubQueriesNotReported(false);
  }

  private void assertSubQueriesNotReported(boolean distributed) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");
    params.set("distrib", distributed);
    params.set("distrib.singlePass", false);
    params.set("fl", "id,child:[subquery]");
    params.set("child.q", "{!term f=id v=$row.id}");
    params.set("child.fl", "id");
    params.set("child.distrib", false);
    QueryResponse rsp = new QueryRequest(params).process(nodeClient(), COLLECTION);
    assertFalse(rsp.getResults().isEmpty());
    assertEquals(cluster.getJettySolrRunner(0).getNodeName(), captured().node);
  }

  @Test
  public void testTopLevelEmbeddedSearchReported() throws Exception {
    try (SolrCore core = jettyCore(COLLECTION);
        SolrClient client = new EmbeddedSolrServer(core)) {
      new QueryRequest(new SolrQuery("*:*").setParam("distrib", false)).process(client);
    }
    delivered();
  }

  @Test
  public void testRequestInfo() throws Exception {
    QueryResponse rsp = query(COLLECTION, "*:*");
    SearchRequestInfo info = delivered();
    assertEquals(NUM_SHARDS, info.shardCount());
    assertEquals(Long.valueOf(rsp.getResults().getNumFound()), info.hits());
    assertEquals("/select", info.handlerPath());
    assertNotNull(info.rb());
    assertNull(info.failure());
    assertFalse(info.partialResults());
    assertTrue(info.elapsedMs() >= 0);
    assertTrue(info.elapsedMs() <= rsp.getQTime());
  }

  @Test
  public void testFailedSearch() throws Exception {
    SolrException e = expectThrows(SolrException.class, () -> query(COLLECTION, "id:("));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertEquals(
        SolrException.ErrorCode.BAD_REQUEST.code, ((SolrException) delivered().failure()).code());
  }

  @Test
  public void testQTimeMatchesHeader() throws Exception {
    CountDownLatch neverReleased = new CountDownLatch(1);
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      addListener(jetty, info -> awaitQuietly(neverReleased, SLOW_LISTENER_MS));
    }
    assertQTimeMatchesHeader(query(COLLECTION, "*:*"));
    resetListeners(false);
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      addListener(jetty, info -> awaitQuietly(neverReleased, SLOW_LISTENER_MS));
    }
    assertQTimeMatchesHeader(
        new V2Request.Builder("/collections/" + DISPATCH_COLLECTION + "/v2-search")
            .withMethod(SolrRequest.METHOD.GET)
            .withParams(new ModifiableSolrParams().set("q", "*:*").set("distrib", false))
            .build()
            .process(nodeClient()));
  }

  private void assertQTimeMatchesHeader(SolrResponseBase rsp) throws InterruptedException {
    assertEquals(rsp.getQTime(), delivered().qTimeMs());
  }

  private static void awaitQuietly(CountDownLatch latch, long timeoutMs) {
    try {
      latch.await(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Test
  public void testEchoParamsFailureNotReported() {
    SolrException e =
        expectThrows(
            SolrException.class,
            () -> nodeClient().query(COLLECTION, new SolrQuery("*:*").set("echoParams", "bogus")));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertEquals(0, captures());
  }

  @Test
  public void testThrowingListenerDoesNotChangeResponse() throws Exception {
    resetListeners(true);
    QueryResponse rsp = query(COLLECTION, "*:*");
    assertEquals(0, rsp.getStatus());
    assertEquals(3, rsp.getResults().getNumFound());
    delivered();
  }

  @Test
  public void testRealTimeGetNotReported() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("id", "1");
    QueryRequest req = new QueryRequest(params);
    req.setPath("/get");
    req.process(nodeClient(), COLLECTION);
    assertEquals(0, captures());
  }

  @Test
  public void testPingNotReported() throws Exception {
    assertEquals(0, new SolrPing().process(nodeClient(), COLLECTION).getStatus());
    assertEquals(0, captures());
  }

  @Test
  public void testInternalSearchNotReported() throws Exception {
    try (SolrCore core = jettyCore(COLLECTION);
        SolrQueryRequest req =
            new SolrQueryRequestBase(
                core, new ModifiableSolrParams().set("q", "*:*").set("distrib", false)) {}) {
      SolrQueryResponse rsp = new SolrQueryResponse();
      core.execute(core.getRequestHandler("/select"), req, rsp);
      assertNull(rsp.getException());
      assertNotNull(rsp.getValues().get("response"));
    }
    assertEquals(0, captures());
  }

  @Test
  public void testExportNotReported() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");
    params.set("sort", "id asc");
    params.set("fl", "id");
    QueryRequest req = new QueryRequest(params);
    req.setPath("/export");
    req.setResponseParser(new InputStreamResponseParser("json"));
    NamedList<Object> rsp = nodeClient().request(req, COLLECTION);
    String body = InputStreamResponseParser.consumeResponseToString(rsp);
    assertTrue(body, body.contains("\"docs\""));
    assertEquals(0, captures());
  }

  @Test
  public void testPartialResults() throws Exception {
    TestInjection.queryTimeout = new CallerSpecificQueryLimit(Set.of("QueryComponent.process"));
    try {
      query(COLLECTION, "*:*");
    } finally {
      TestInjection.queryTimeout = null;
    }
    SearchRequestInfo info = delivered();
    assertTrue(info.partialResults());
    assertNull(info.failure());
  }

  @Test
  public void testV2Select() throws Exception {
    new V2Request.Builder("/collections/" + COLLECTION + "/select")
        .withMethod(SolrRequest.METHOD.GET)
        .withParams(new ModifiableSolrParams().set("q", "*:*"))
        .build()
        .process(nodeClient());
    SearchRequestInfo info = delivered();
    assertEquals(NUM_SHARDS, info.shardCount());
    assertEquals("/select", info.handlerPath());
  }

  @Test
  public void testV2OnlyHandlerReported() throws Exception {
    new V2Request.Builder("/collections/" + DISPATCH_COLLECTION + "/v2-search")
        .withMethod(SolrRequest.METHOD.GET)
        .withParams(new ModifiableSolrParams().set("q", "*:*").set("distrib", false))
        .build()
        .process(nodeClient());
    SearchRequestInfo info = delivered();
    assertEquals("/v2-search", info.handlerPath());
    assertNull(info.failure());
  }

  @Test
  public void testEmbeddedDefaultHandlerReported() throws Exception {
    try (SolrCore core = jettyCore(DISPATCH_COLLECTION);
        SolrClient client = new EmbeddedSolrServer(core)) {
      new QueryRequest(new SolrQuery("*:*").setParam("distrib", false)).process(client);
    }
    SearchRequestInfo info = delivered();
    assertEquals("/select", info.handlerPath());
    assertNull(info.failure());
  }

  @Test
  public void testCpuTime() throws Exception {
    assumeTrue("thread CPU time unsupported", ThreadCpuTimer.isSupported());
    query(COLLECTION, "*:*");
    assertNull(delivered().cpuTimeMs());

    System.setProperty(ThreadCpuTimer.ENABLE_CPU_TIME, "true");
    String cpuCollection = "listener_cpu";
    createCollection(cpuCollection);
    resetListeners(false);
    QueryResponse rsp = query(cpuCollection, "*:*");
    SearchRequestInfo info = delivered();
    long shardCpuTime = (Long) rsp.getResponseHeader().get(ThreadCpuTimer.CPU_TIME);
    long localCpuTime = (Long) info.rsp().getToLog().get(ThreadCpuTimer.LOCAL_CPU_TIME);
    assertEquals(Long.valueOf(shardCpuTime + localCpuTime), info.cpuTimeMs());
    CollectionAdminRequest.deleteCollection(cpuCollection).process(cluster.getSolrClient());
  }

  @Test
  public void testCpuTimeFromLog() throws Exception {
    try (SolrCore core = jettyCore(COLLECTION);
        SolrQueryRequest req = new SolrQueryRequestBase(core, new ModifiableSolrParams()) {}) {
      SolrQueryResponse failed = new SolrQueryResponse();
      failed.addResponseHeader(new SimpleOrderedMap<>());
      failed.getResponseHeader().add(ThreadCpuTimer.CPU_TIME, 5L);
      failed.addToLog(ThreadCpuTimer.LOCAL_CPU_TIME, 5L);
      assertEquals(Long.valueOf(5), from(req, failed, distributedBuilder(req, failed)).cpuTimeMs());

      SolrQueryResponse succeeded = new SolrQueryResponse();
      succeeded.addResponseHeader(new SimpleOrderedMap<>());
      succeeded.getResponseHeader().add(ThreadCpuTimer.CPU_TIME, 7L);
      succeeded.addToLog(ThreadCpuTimer.CPU_TIME, 7L);
      succeeded.addToLog(ThreadCpuTimer.LOCAL_CPU_TIME, 5L);
      assertEquals(
          Long.valueOf(12), from(req, succeeded, distributedBuilder(req, succeeded)).cpuTimeMs());

      SolrQueryResponse disabled = new SolrQueryResponse();
      assertNull(from(req, disabled, distributedBuilder(req, disabled)).cpuTimeMs());
    }
  }

  @Test
  public void testNoListenersLeavesResponseUnchanged() throws Exception {
    NamedList<?> withListeners = query(COLLECTION, "*:*").getResponseHeader();
    removeListeners();
    NamedList<?> withoutListeners = query(COLLECTION, "*:*").getResponseHeader();
    assertEquals(keys(withListeners), keys(withoutListeners));
    assertEquals(1, captures());
  }

  private static SolrCore jettyCore(String collection) {
    JettySolrRunner jetty = cluster.getJettySolrRunner(0);
    String coreName =
        getCollectionState(collection)
            .replicaStream()
            .filter(replica -> replica.getNodeName().equals(jetty.getNodeName()))
            .map(Replica::getCoreName)
            .findFirst()
            .orElseThrow();
    return jetty.getCoreContainer().getCore(coreName);
  }

  private static ResponseBuilder distributedBuilder(SolrQueryRequest req, SolrQueryResponse rsp) {
    ResponseBuilder rb = new ResponseBuilder(req, rsp, List.of());
    rb.isDistrib = true;
    rb.shards = new String[NUM_SHARDS];
    return rb;
  }

  private static SearchRequestInfo from(
      SolrQueryRequest req, SolrQueryResponse rsp, ResponseBuilder rb) {
    return SearchRequestInfo.from(req, rsp, rb, 0);
  }

  private static List<String> keys(NamedList<?> header) {
    List<String> keys = new ArrayList<>();
    header.forEach((k, v) -> keys.add(k));
    return keys;
  }
}
