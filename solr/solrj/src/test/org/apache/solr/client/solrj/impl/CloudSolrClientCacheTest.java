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

package org.apache.solr.client.solrj.impl;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.http.NoHttpResponseException;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.DelegatingClusterStateProvider;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.junit.BeforeClass;

public class CloudSolrClientCacheTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() {
    assumeWorkingMockito();
  }

  public void testCaching() throws Exception {
    String collName = "gettingstarted";
    Set<String> livenodes = new HashSet<>();
    Map<String, ClusterState.CollectionRef> refs = new HashMap<>();
    Map<String, DocCollection> colls = new HashMap<>();

    class Ref extends ClusterState.CollectionRef {
      private String c;

      public Ref(String c) {
        super(null);
        this.c = c;
      }

      @Override
      public boolean isLazilyLoaded() {
        return true;
      }

      @Override
      public DocCollection get() {
        gets.incrementAndGet();
        return colls.get(c);
      }
    }
    Map<String, Function<?, ?>> responses = new HashMap<>();
    NamedList<Object> okResponse = new NamedList<>();
    okResponse.add("responseHeader", new NamedList<>(Collections.singletonMap("status", 0)));

    LBHttpSolrClient mockLbclient = getMockLbHttpSolrClient(responses);
    AtomicInteger lbhttpRequestCount = new AtomicInteger();
    try (ClusterStateProvider clusterStateProvider = getStateProvider(livenodes, refs);
        CloudSolrClient cloudClient =
            new RandomizingCloudSolrClientBuilder(clusterStateProvider)
                .withLBHttpSolrClient(mockLbclient)
                .build()) {
      livenodes.addAll(Set.of("192.168.1.108:7574_solr", "192.168.1.108:8983_solr"));
      ClusterState cs =
          ClusterState.createFromJson(
              1, COLL1_STATE.getBytes(UTF_8), Collections.emptySet(), Instant.now(), null);
      refs.put(collName, new Ref(collName));
      colls.put(collName, cs.getCollectionOrNull(collName));
      responses.put(
          "request",
          o -> {
            int i = lbhttpRequestCount.incrementAndGet();
            if (i == 1) {
              return new ConnectException("TEST");
            }
            if (i == 2) {
              return new SocketException("TEST");
            }
            if (i == 3) {
              return new NoHttpResponseException("TEST");
            }
            return okResponse;
          });
      UpdateRequest update = new UpdateRequest().add("id", "123", "desc", "Something 0");

      cloudClient.request(update, collName);
      // Async refresh with deduplication means rapid retries can share the same Future.
      // Race: sometimes async completes fast enough for 2 fetches, sometimes only 1.
      int fetchCount = refs.get(collName).getCount();
      assertTrue("Expected 1 or 2 fetches, got " + fetchCount, fetchCount >= 1 && fetchCount <= 2);
    }
  }

  public void testStaleStateRetrySkipsStateVersionBeforeWait() throws Exception {
    String collName = "gettingstarted";
    Set<String> liveNodes = new HashSet<>(Set.of("192.168.1.108:8983_solr"));
    AtomicInteger refGets = new AtomicInteger();
    AtomicReference<DocCollection> currentDoc = new AtomicReference<>(loadCollection(collName, 1));
    Map<String, ClusterState.CollectionRef> refs =
        Map.of(collName, new TestCollectionRef(currentDoc::get, refGets, null, null, -1));
    try (ClusterStateProvider provider = getStateProvider(liveNodes, refs);
        RecordingCloudSolrClient client = new RecordingCloudSolrClient(provider, 3)) {
      client.enqueue(
          (req, cols) -> {
            throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "stale");
          });
      client.enqueue((req, cols) -> null);

      DummyRequest request = new DummyRequest(collName);
      NamedList<Object> resp = client.request(request, collName);
      assertNotNull(resp);

      List<String> history = client.getStateVersionHistory();
      assertEquals(2, history.size());
      assertTrue(history.get(0).startsWith(collName + ":"));
      assertNull("Second attempt should skip _stateVer_", history.get(1));
      assertTrue("Expected refresh to be triggered", refGets.get() >= 1);
    }
  }

  public void testDirectUpdatesToLeadersSkipStateVersionBeforeWait() throws Exception {
    String collName = "gettingstarted";
    Set<String> liveNodes = new HashSet<>(Set.of("192.168.1.108:8983_solr"));
    AtomicInteger refGets = new AtomicInteger();
    AtomicReference<DocCollection> currentDoc = new AtomicReference<>(loadCollection(collName, 1));
    Map<String, ClusterState.CollectionRef> refs =
        Map.of(collName, new TestCollectionRef(currentDoc::get, refGets, null, null, -1));
    try (ClusterStateProvider provider = getStateProvider(liveNodes, refs);
        RecordingCloudSolrClient client =
            new RecordingCloudSolrClient(provider, true, true, true, 3)) {
      client.enqueue(
          (req, cols) -> {
            throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "stale");
          });
      client.enqueue((req, cols) -> null);

      DummyUpdateRequest request = new DummyUpdateRequest(collName);
      NamedList<Object> resp = client.request(request, collName);
      assertNotNull(resp);

      List<String> history = client.getStateVersionHistory();
      assertEquals(2, history.size());
      assertTrue(history.get(0).startsWith(collName + ":"));
      assertNull(history.get(1));
      assertTrue(refGets.get() >= 1);
    }
  }

  public void testStaleStateRetryWaitsAfterSkipFailure() throws Exception {
    String collName = "gettingstarted";
    AtomicReference<DocCollection> currentDoc = new AtomicReference<>(loadCollection(collName, 1));

    // Track when refresh is triggered to ensure the async refresh mechanism is used
    AtomicInteger refGets = new AtomicInteger();
    TestCollectionRef ref = new TestCollectionRef(currentDoc::get, refGets, null, null, -1);
    Map<String, ClusterState.CollectionRef> refs = Map.of(collName, ref);
    Set<String> liveNodes = new HashSet<>(Set.of("192.168.1.108:8983_solr"));

    try (ClusterStateProvider provider = getStateProvider(liveNodes, refs);
        RecordingCloudSolrClient client = new RecordingCloudSolrClient(provider, 2)) {
      // First attempt: returns stale error, triggers skipStateVersion retry
      client.enqueue(
          (req, cols) -> {
            throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "stale-first");
          });
      // Second attempt (skipStateVersion retry): also returns stale error
      client.enqueue(
          (req, cols) -> {
            throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "stale-second");
          });
      // Third attempt (after waiting for refresh): succeeds
      client.enqueue((req, cols) -> null);

      DummyRequest request = new DummyRequest(collName);
      NamedList<Object> resp = client.request(request, collName);
      assertNotNull(resp);

      // Verify the retry sequence:
      // 1. First attempt with state version
      // 2. skipStateVersion retry without state version
      // 3. Final retry with refreshed state version
      List<String> history = client.getStateVersionHistory();
      assertEquals("Should have 3 attempts", 3, history.size());
      assertTrue(
          "First attempt should have state param", history.get(0).startsWith(collName + ":"));
      assertNull("skipStateVersion attempt should NOT have state param", history.get(1));
      assertTrue(
          "Final attempt should have state param after refresh",
          history.get(2).startsWith(collName + ":"));

      // Verify refresh was triggered (at least initial load + refresh after stale errors)
      assertTrue("Refresh should have been called", refGets.get() >= 2);
    }
  }

  public void testStateRefreshThreadsConfiguredViaBuilder() throws Exception {
    String collName = "gettingstarted";
    AtomicReference<DocCollection> currentDoc = new AtomicReference<>(loadCollection(collName, 1));
    Map<String, ClusterState.CollectionRef> refs =
        Map.of(
            collName, new TestCollectionRef(currentDoc::get, new AtomicInteger(), null, null, -1));
    Set<String> liveNodes = new HashSet<>(Set.of("192.168.1.108:8983_solr"));

    try (ClusterStateProvider provider = getStateProvider(liveNodes, refs);
        RecordingCloudSolrClient client = new RecordingCloudSolrClient(provider, 7)) {
      assertEquals(7, client.getStateRefreshParallelism());
    }
  }

  public void testConcurrentRefreshIsDeduplicated() throws Exception {
    String collName = "gettingstarted";
    AtomicReference<DocCollection> currentDoc = new AtomicReference<>(loadCollection(collName, 1));
    AtomicInteger refGets = new AtomicInteger();
    CountDownLatch refreshStarted = new CountDownLatch(1);
    CountDownLatch releaseRefresh = new CountDownLatch(1);
    Map<String, ClusterState.CollectionRef> refs =
        Map.of(
            collName,
            new TestCollectionRef(currentDoc::get, refGets, refreshStarted, releaseRefresh, 1));
    Set<String> liveNodes = new HashSet<>(Set.of("192.168.1.108:8983_solr"));

    try (ClusterStateProvider provider = getStateProvider(liveNodes, refs);
        RecordingCloudSolrClient client = new RecordingCloudSolrClient(provider, 2)) {
      AtomicInteger sendCount = new AtomicInteger();
      client.setDefaultInvocation(
          (req, cols) -> {
            if (sendCount.incrementAndGet() <= 2) {
              throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "stale");
            }
            return null;
          });

      DummyRequest request = new DummyRequest(collName);
      ExecutorService executor =
          ExecutorUtil.newMDCAwareFixedThreadPool(
              2, new SolrNamedThreadFactory("CloudSolrClientCacheTest-parallel"));
      try {
        Future<NamedList<Object>> first = executor.submit(() -> client.request(request, collName));
        Future<NamedList<Object>> second = executor.submit(() -> client.request(request, collName));

        assertTrue(
            "Refresh should start within timeout", refreshStarted.await(30, TimeUnit.SECONDS));
        assertEquals("Only one refresh should be in flight", 1, refGets.get());
        releaseRefresh.countDown();

        NamedList<Object> firstResp = first.get(30, TimeUnit.SECONDS);
        NamedList<Object> secondResp = second.get(30, TimeUnit.SECONDS);

        assertNotNull(firstResp);
        assertNotNull(secondResp);
      } finally {
        ExecutorUtil.shutdownAndAwaitTermination(executor);
      }
    }
  }

  @SuppressWarnings({"unchecked"})
  private LBHttpSolrClient getMockLbHttpSolrClient(Map<String, Function<?, ?>> responses)
      throws Exception {
    LBHttpSolrClient mockLbclient = mock(LBHttpSolrClient.class);

    when(mockLbclient.request(any(LBSolrClient.Req.class)))
        .then(
            invocationOnMock -> {
              LBSolrClient.Req req = invocationOnMock.getArgument(0);
              Function<?, ?> f = responses.get("request");
              if (f == null) return null;
              Object res = f.apply(null);
              if (res instanceof Exception) throw (Throwable) res;
              LBSolrClient.Rsp rsp = new LBSolrClient.Rsp();
              rsp.rsp = (NamedList<Object>) res;
              rsp.server = req.servers.get(0);
              return rsp;
            });
    return mockLbclient;
  }

  private ClusterStateProvider getStateProvider(
      Set<String> livenodes, Map<String, ClusterState.CollectionRef> colls) {
    return new DelegatingClusterStateProvider(null) {
      @Override
      public ClusterState.CollectionRef getState(String collection) {
        return colls.get(collection);
      }

      @Override
      public Set<String> getLiveNodes() {
        return livenodes;
      }

      @Override
      public List<String> resolveAlias(String collection) {
        return Collections.singletonList(collection);
      }

      @Override
      public <T> T getClusterProperty(String propertyName, T def) {
        return def;
      }
    };
  }

  private DocCollection loadCollection(String collection, int version) throws Exception {
    ClusterState state =
        ClusterState.createFromJson(
            version, COLL1_STATE.getBytes(UTF_8), Collections.emptySet(), Instant.now(), null);
    return state.getCollectionOrNull(collection);
  }

  private static class RecordingCloudSolrClient extends CloudSolrClient implements AutoCloseable {
    private final ClusterStateProvider provider;
    private final ConcurrentLinkedQueue<Invocation> invocations = new ConcurrentLinkedQueue<>();
    private volatile Invocation defaultInvocation;
    private final List<String> stateHistory = Collections.synchronizedList(new ArrayList<>());
    private final NamedList<Object> okResponse;

    RecordingCloudSolrClient(ClusterStateProvider provider, int refreshThreads) {
      this(provider, true, true, false, refreshThreads);
    }

    RecordingCloudSolrClient(
        ClusterStateProvider provider,
        boolean updatesToLeaders,
        boolean parallelUpdates,
        boolean directUpdatesToLeadersOnly,
        int refreshThreads) {
      super(updatesToLeaders, parallelUpdates, directUpdatesToLeadersOnly, refreshThreads);
      this.provider = provider;
      NamedList<Object> header = new NamedList<>();
      header.add("status", 0);
      okResponse = new NamedList<>();
      okResponse.add("responseHeader", header);
    }

    void enqueue(Invocation invocation) {
      invocations.add(invocation);
    }

    void setDefaultInvocation(Invocation invocation) {
      this.defaultInvocation = invocation;
    }

    List<String> getStateVersionHistory() {
      synchronized (stateHistory) {
        return new ArrayList<>(stateHistory);
      }
    }

    @Override
    protected NamedList<Object> sendRequest(SolrRequest<?> request, List<String> inputCollections)
        throws SolrServerException, IOException {
      String stateParam =
          request.getParams() == null ? null : request.getParams().get(STATE_VERSION);
      stateHistory.add(stateParam);
      Invocation invocation = invocations.poll();
      if (invocation == null) {
        invocation = defaultInvocation;
      }
      if (invocation == null) {
        return okResponse;
      }
      try {
        NamedList<Object> rsp = invocation.invoke(request, inputCollections);
        return rsp == null ? okResponse : rsp;
      } catch (SolrServerException | IOException | SolrException e) {
        throw e;
      } catch (Exception e) {
        throw new SolrServerException(e);
      }
    }

    @Override
    protected LBSolrClient getLbClient() {
      throw new UnsupportedOperationException("LB client not used in test harness");
    }

    @Override
    public ClusterStateProvider getClusterStateProvider() {
      return provider;
    }

    @FunctionalInterface
    interface Invocation {
      NamedList<Object> invoke(SolrRequest<?> request, List<String> inputCollections)
          throws Exception;
    }
  }

  private static class DummyRequest extends SolrRequest<SimpleSolrResponse> {
    private final ModifiableSolrParams params = new ModifiableSolrParams();
    private final String collection;

    DummyRequest(String collection) {
      super(METHOD.GET, "/dummy");
      this.collection = collection;
    }

    @Override
    public ModifiableSolrParams getParams() {
      return params;
    }

    @Override
    public Collection<ContentStream> getContentStreams() {
      return null;
    }

    @Override
    protected SimpleSolrResponse createResponse(SolrClient solrClient) {
      return new SimpleSolrResponse();
    }

    @Override
    public boolean requiresCollection() {
      return true;
    }

    @Override
    public String getCollection() {
      return collection;
    }

    @Override
    public String getRequestType() {
      return SolrRequestType.UNSPECIFIED.toString();
    }
  }

  private static class DummyUpdateRequest extends DummyRequest {
    DummyUpdateRequest(String collection) {
      super(collection);
    }

    @Override
    public String getRequestType() {
      return SolrRequestType.UPDATE.toString();
    }
  }

  private static class TestCollectionRef extends ClusterState.CollectionRef {
    private final Supplier<DocCollection> supplier;
    private final AtomicInteger counter;
    private final CountDownLatch phaseOneReady; // Signals COUNT=phaseOneCount reached
    private final CountDownLatch phaseOneProceed; // Test signals OK to proceed
    private final CountDownLatch phaseTwoStarted; // Signals COUNT=phaseTwoCount blocked
    private final CountDownLatch phaseTwoProceed; // Test signals OK to complete
    private final int phaseOneCount;
    private final int phaseTwoCount;
    private final AtomicBoolean phaseOneTriggered = new AtomicBoolean(false);
    private final AtomicBoolean phaseTwoTriggered = new AtomicBoolean(false);

    // Two-phase constructor for precise control over async refresh ordering
    TestCollectionRef(
        Supplier<DocCollection> supplier,
        AtomicInteger counter,
        CountDownLatch phaseOneReady,
        CountDownLatch phaseOneProceed,
        CountDownLatch phaseTwoStarted,
        CountDownLatch phaseTwoProceed,
        int phaseOneCount,
        int phaseTwoCount) {
      super(null);
      this.supplier = supplier;
      this.counter = counter;
      this.phaseOneReady = phaseOneReady;
      this.phaseOneProceed = phaseOneProceed;
      this.phaseTwoStarted = phaseTwoStarted;
      this.phaseTwoProceed = phaseTwoProceed;
      this.phaseOneCount = phaseOneCount;
      this.phaseTwoCount = phaseTwoCount;
    }

    // Backward-compatible single-block constructor for existing tests
    TestCollectionRef(
        Supplier<DocCollection> supplier,
        AtomicInteger counter,
        CountDownLatch startLatch,
        CountDownLatch waitLatch,
        int blockAtCount) {
      this(supplier, counter, null, null, startLatch, waitLatch, -1, blockAtCount);
    }

    @Override
    public boolean isLazilyLoaded() {
      return true;
    }

    @Override
    public DocCollection get() {
      int count = counter.incrementAndGet();

      // Phase 1: Sync point to control async refresh completion timing
      if (phaseOneCount > 0
          && count == phaseOneCount
          && phaseOneTriggered.compareAndSet(false, true)) {
        if (phaseOneReady != null) {
          phaseOneReady.countDown();
        }
        if (phaseOneProceed != null) {
          try {
            phaseOneProceed.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
      }

      // Phase 2: Block for test verification
      if (phaseTwoCount > 0
          && count == phaseTwoCount
          && phaseTwoTriggered.compareAndSet(false, true)) {
        if (phaseTwoStarted != null) {
          phaseTwoStarted.countDown();
        }
        if (phaseTwoProceed != null) {
          try {
            phaseTwoProceed.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        }
      }

      return supplier.get();
    }
  }

  private static final String COLL1_STATE =
      "{'gettingstarted':{\n"
          + "    'replicationFactor':'2',\n"
          + "    'router':{'name':'compositeId'},\n"
          + "    'shards':{\n"
          + "      'shard1':{\n"
          + "        'range':'80000000-ffffffff',\n"
          + "        'state':'active',\n"
          + "        'replicas':{\n"
          + "          'core_node2':{\n"
          + "            'core':'gettingstarted_shard1_replica1',\n"
          + "            'base_url':'http://192.168.1.108:8983/solr',\n"
          + "            'node_name':'192.168.1.108:8983_solr',\n"
          + "            'state':'active',\n"
          + "            'leader':'true'},\n"
          + "          'core_node4':{\n"
          + "            'core':'gettingstarted_shard1_replica2',\n"
          + "            'base_url':'http://192.168.1.108:7574/solr',\n"
          + "            'node_name':'192.168.1.108:7574_solr',\n"
          + "            'state':'active'}}},\n"
          + "      'shard2':{\n"
          + "        'range':'0-7fffffff',\n"
          + "        'state':'active',\n"
          + "        'replicas':{\n"
          + "          'core_node1':{\n"
          + "            'core':'gettingstarted_shard2_replica1',\n"
          + "            'base_url':'http://192.168.1.108:8983/solr',\n"
          + "            'node_name':'192.168.1.108:8983_solr',\n"
          + "            'state':'active',\n"
          + "            'leader':'true'},\n"
          + "          'core_node3':{\n"
          + "            'core':'gettingstarted_shard2_replica2',\n"
          + "            'base_url':'http://192.168.1.108:7574/solr',\n"
          + "            'node_name':'192.168.1.108:7574_solr',\n"
          + "            'state':'active'}}}}}}";
}
