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
package org.apache.solr.search;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTaskManagement extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String COLLECTION_NAME = "collection1";

  private ExecutorService queryExecutor;
  private ExecutorService cancelExecutor;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    CollectionAdminRequest.createCollection(COLLECTION_NAME, "conf", 2, 1)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION_NAME, 2, 2);

    queryExecutor = ExecutorUtil.newMDCAwareCachedThreadPool("TestTaskManagement-Query");
    cancelExecutor = ExecutorUtil.newMDCAwareCachedThreadPool("TestTaskManagement-Cancel");

    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      doc.addField("foo1_s", Integer.toString(i));
      doc.addField("foo2_s", Boolean.toString(i % 2 == 0));
      doc.addField("foo4_s", new BytesRef(Boolean.toString(i % 2 == 0)));

      docs.add(doc);
    }

    cluster.getSolrClient(COLLECTION_NAME).add(docs);
    cluster.getSolrClient(COLLECTION_NAME).commit();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    queryExecutor.shutdown();
    cancelExecutor.shutdown();

    queryExecutor.awaitTermination(5, TimeUnit.SECONDS);
    CollectionAdminRequest.deleteCollection(COLLECTION_NAME).process(cluster.getSolrClient());

    super.tearDown();
  }

  @Test
  public void testNonExistentQuery() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("queryUUID", "foobar");

    GenericSolrRequest request =
        new GenericSolrRequest(SolrRequest.METHOD.GET, "/tasks/cancel", params);
    NamedList<Object> queryResponse = cluster.getSolrClient(COLLECTION_NAME).request(request);

    assertEquals("Query with queryID foobar not found", queryResponse.get("status"));
    assertEquals(404, queryResponse.get("responseCode"));
  }

  @Test
  public void testCancellationQuery() throws IOException, SolrServerException {
    Set<Integer> queryIdsSet = ConcurrentHashMap.newKeySet();
    Set<Integer> notFoundIdsSet = ConcurrentHashMap.newKeySet();

    List<CompletableFuture<Void>> queryFutures = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      queryFutures.add(executeQueryAsync(Integer.toString(i)));
    }

    List<CompletableFuture<Void>> cancelFutures = new ArrayList<>();

    NamedList<String> tasks = listTasks();

    for (int i = 0; i < 100; i++) {
      cancelFutures.add(cancelQuery(Integer.toString(i), queryIdsSet, notFoundIdsSet));
    }

    cancelFutures.forEach(CompletableFuture::join);
    queryFutures.forEach(CompletableFuture::join);

    // There is a very small window where we can successfully cancel the query because
    // QueryComponent will aggressively deregister, and even if we use DelayingSearchComponent these
    // queries are not around
    // assertFalse("Should have canceled at least one query", queryIdsSet.isEmpty());
    if (log.isInfoEnabled()) {
      log.info("Cancelled {} queries", queryIdsSet.size());
    }

    assertEquals(
        "Total query count did not match the expected value",
        queryIdsSet.size() + notFoundIdsSet.size(),
        100);
  }

  @Test
  public void testListCancellableQueries() throws Exception {
    for (int i = 0; i < 50; i++) {
      executeQueryAsync(Integer.toString(i));
    }

    NamedList<String> result = listTasks();

    Iterator<Map.Entry<String, String>> iterator = result.iterator();

    Set<Integer> presentQueryIDs = new HashSet<>();

    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();

      presentQueryIDs.add(Integer.parseInt(entry.getKey()));
    }

    MatcherAssert.assertThat(presentQueryIDs.size(), betweenInclusive(0, 50));
    for (int value : presentQueryIDs) {
      MatcherAssert.assertThat(value, betweenInclusive(0, 49));
    }
  }

  private static Matcher<Integer> betweenInclusive(int lower, int upper) {
    return Matchers.allOf(Matchers.greaterThanOrEqualTo(lower), Matchers.lessThanOrEqualTo(upper));
  }

  @SuppressWarnings("unchecked")
  private NamedList<String> listTasks() throws SolrServerException, IOException {
    NamedList<Object> response =
        cluster
            .getSolrClient(COLLECTION_NAME)
            .request(new GenericSolrRequest(SolrRequest.METHOD.GET, "/tasks/list"));
    return (NamedList<String>) response.get("taskList");
  }

  @Test
  public void testCheckSpecificQueryStatus() throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("taskUUID", "25");

    GenericSolrRequest request =
        new GenericSolrRequest(SolrRequest.METHOD.GET, "/tasks/list", params);
    NamedList<Object> queryResponse = cluster.getSolrClient(COLLECTION_NAME).request(request);

    String result = (String) queryResponse.get("taskStatus");

    assertTrue(result.contains("inactive"));
  }

  private CompletableFuture<Void> cancelQuery(
      final String queryID, Set<Integer> cancelledQueryIdsSet, Set<Integer> notFoundQueryIdSet) {
    return CompletableFuture.runAsync(
        () -> {
          ModifiableSolrParams params = new ModifiableSolrParams();

          params.set("queryUUID", queryID);
          SolrRequest<?> request = new QueryRequest(params);
          request.setPath("/tasks/cancel");

          try {
            NamedList<Object> queryResponse;

            queryResponse = cluster.getSolrClient(COLLECTION_NAME).request(request);

            int responseCode = (int) queryResponse.get("responseCode");

            if (responseCode == 200 /* HTTP OK */) {
              cancelledQueryIdsSet.add(Integer.parseInt(queryID));
            } else if (responseCode == 404 /* HTTP NOT FOUND */) {
              notFoundQueryIdSet.add(Integer.parseInt(queryID));
            }
          } catch (Exception e) {
            throw new CompletionException(e);
          }
        },
        cancelExecutor);
  }

  public void executeQuery(String queryId) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.set("q", "*:*");
    params.set("canCancel", "true");

    if (queryId != null) {
      params.set("queryUUID", queryId);
    }

    SolrRequest<?> request = new QueryRequest(params);

    cluster.getSolrClient(COLLECTION_NAME).request(request);
  }

  public CompletableFuture<Void> executeQueryAsync(String queryId) {
    return CompletableFuture.runAsync(
        () -> {
          try {
            executeQuery(queryId);
          } catch (Exception e) {
            throw new CompletionException(e);
          }
        },
        queryExecutor);
  }
}
