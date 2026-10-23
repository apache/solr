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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.ThreadCpuTimer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SearchRequestListenerTest extends SolrCloudTestCase {
  private static final String COLLECTION = "listener";
  private static final int NUM_SHARDS = 2;
  private static final Queue<Event> events = new ConcurrentLinkedQueue<>();

  private static final List<SearchRequestListener> listeners = new ArrayList<>();

  private record Event(String node, SearchRequestInfo info) {}

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
    createCollection(COLLECTION);
  }

  @Before
  public void registerListeners() {
    events.clear();
    addListeners(false);
  }

  @After
  public void removeListeners() {
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      listeners.forEach(jetty.getCoreContainer()::removeSearchRequestListener);
    }
    listeners.clear();
  }

  private static void createCollection(String name) throws Exception {
    CollectionAdminRequest.createCollection(name, "conf", NUM_SHARDS, 1)
        .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);
    cluster.waitForActiveCollection(name, NUM_SHARDS, NUM_SHARDS);
    new UpdateRequest()
        .add("id", "1")
        .add("id", "2")
        .add("id", "3")
        .commit(cluster.getSolrClient(), name);
  }

  private static void addListeners(boolean withThrowingListener) {
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      String node = jetty.getNodeName();
      if (withThrowingListener) {
        addListener(
            jetty,
            info -> {
              throw new RuntimeException("listener failure");
            });
      }
      addListener(jetty, info -> events.add(new Event(node, info)));
    }
  }

  private static void addListener(JettySolrRunner jetty, SearchRequestListener listener) {
    listeners.add(listener);
    jetty.getCoreContainer().addSearchRequestListener(listener);
  }

  private static SolrClient nodeClient() {
    return cluster.getJettySolrRunner(0).getSolrClient();
  }

  private static QueryResponse query(String collection, String q) throws Exception {
    return new QueryRequest(new SolrQuery(q)).process(nodeClient(), collection);
  }

  @Test
  public void testCalledOnceOnCoordinator() throws Exception {
    query(COLLECTION, "*:*");
    assertEquals(1, events.size());
    assertEquals(cluster.getJettySolrRunner(0).getNodeName(), events.peek().node());
  }

  @Test
  public void testRequestInfo() throws Exception {
    QueryResponse rsp = query(COLLECTION, "*:*");
    SearchRequestInfo info = events.peek().info();
    assertEquals(NUM_SHARDS, info.shardCount());
    assertEquals(Long.valueOf(rsp.getResults().getNumFound()), info.hits());
    assertEquals("/select", info.handlerPath());
    assertNotNull(info.rb());
    assertNull(info.failure());
  }

  @Test
  public void testFailedSearch() {
    SolrException e = expectThrows(SolrException.class, () -> query(COLLECTION, "id:("));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertEquals(1, events.size());
    assertEquals(
        SolrException.ErrorCode.BAD_REQUEST.code,
        ((SolrException) events.peek().info().failure()).code());
  }

  @Test
  public void testThrowingListenerDoesNotChangeResponse() throws Exception {
    removeListeners();
    addListeners(true);
    QueryResponse rsp = query(COLLECTION, "*:*");
    assertEquals(0, rsp.getStatus());
    assertEquals(3, rsp.getResults().getNumFound());
    assertEquals(1, events.size());
  }

  @Test
  public void testRealTimeGetNotReported() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("id", "1");
    QueryRequest req = new QueryRequest(params);
    req.setPath("/get");
    req.process(nodeClient(), COLLECTION);
    assertEquals(0, events.size());
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
    assertEquals(0, events.size());
  }

  @Test
  public void testShardsTolerant() throws Exception {
    QueryResponse rsp =
        new QueryRequest(new SolrQuery("*:*").setParam(ShardParams.SHARDS_TOLERANT, true))
            .process(nodeClient(), COLLECTION);
    assertEquals(3, rsp.getResults().getNumFound());
    assertEquals(1, events.size());
    assertNull(events.peek().info().failure());
  }

  @Test
  public void testV2Select() throws Exception {
    new V2Request.Builder("/collections/" + COLLECTION + "/select")
        .withMethod(SolrRequest.METHOD.GET)
        .withParams(new ModifiableSolrParams().set("q", "*:*"))
        .build()
        .process(nodeClient());
    assertEquals(1, events.size());
    assertEquals(NUM_SHARDS, events.peek().info().shardCount());
    assertEquals("/select", events.peek().info().handlerPath());
  }

  @Test
  public void testCpuTime() throws Exception {
    assumeTrue("thread CPU time unsupported", ThreadCpuTimer.isSupported());
    query(COLLECTION, "*:*");
    assertEquals(1, events.size());
    assertNull(events.poll().info().cpuTimeMs());

    System.setProperty(ThreadCpuTimer.ENABLE_CPU_TIME, "true");
    String cpuCollection = "listener_cpu";
    createCollection(cpuCollection);
    events.clear();
    QueryResponse rsp = query(cpuCollection, "*:*");
    assertEquals(1, events.size());
    SearchRequestInfo info = events.poll().info();
    long shardCpuTime = (Long) rsp.getResponseHeader().get(ThreadCpuTimer.CPU_TIME);
    long localCpuTime = (Long) info.rsp().getToLog().get(ThreadCpuTimer.LOCAL_CPU_TIME);
    assertEquals(Long.valueOf(shardCpuTime + localCpuTime), info.cpuTimeMs());
    CollectionAdminRequest.deleteCollection(cpuCollection).process(cluster.getSolrClient());
  }

  @Test
  public void testNoListenersLeavesResponseUnchanged() throws Exception {
    NamedList<?> withListeners = query(COLLECTION, "*:*").getResponseHeader();
    removeListeners();
    events.clear();
    NamedList<?> withoutListeners = query(COLLECTION, "*:*").getResponseHeader();
    assertEquals(keys(withListeners), keys(withoutListeners));
    assertEquals(0, events.size());
  }

  private static List<String> keys(NamedList<?> header) {
    List<String> keys = new ArrayList<>();
    header.forEach((k, v) -> keys.add(k));
    return keys;
  }
}
