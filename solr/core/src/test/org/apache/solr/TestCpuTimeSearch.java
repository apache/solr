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
package org.apache.solr;

import java.io.IOException;
import java.util.List;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.ThreadCpuTimer;
import org.junit.BeforeClass;

public class TestCpuTimeSearch extends SolrCloudTestCase {

  private static final String COLLECTION = "cpu-col";

  @BeforeClass
  public static void setupSolr() throws Exception {
    System.setProperty(ThreadCpuTimer.ENABLE_CPU_TIME, "true");

    configureCluster(1)
        .addConfig(
            "conf", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1)
        .setRouterName(CompositeIdRouter.NAME)
        .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);

    waitForState("New collection", COLLECTION, clusterShape(2, 2));
    DocCollection coll = getCollectionState(COLLECTION);

    DocRouter router = DocRouter.getDocRouter(CompositeIdRouter.NAME);

    UpdateRequest req = new UpdateRequest();

    SolrInputDocument doc1 = new SolrInputDocument();
    String id1 = "1";
    doc1.setField("id", id1);
    doc1.setField("subject", "batman");
    doc1.setField("title", "foo bar");
    req.add(doc1);

    // For second record, generate an ID until we are sure it will be indexed
    // in a different shard from first record
    String slice1 = router.getTargetSlice(id1, doc1, null, null, coll).getName();
    String slice2;
    String id2;
    do {
      id2 = Integer.toString(random().nextInt(Integer.MAX_VALUE));
      slice2 = router.getTargetSlice(id2, doc1, null, null, coll).getName();
    } while (slice1.equals(slice2));

    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.setField("id", "2");
    doc2.setField("subject", "superman");
    req.add(doc2);
    req.commit(cluster.getSolrClient(), COLLECTION);
  }

  public void testWithoutDistrib() throws SolrServerException, IOException {
    SolrQuery query = new SolrQuery();
    query.setQuery("subject:batman OR subject:superman");
    query.addField("id");
    query.addField("subject");
    query.setDistrib(false);

    QueryResponse response;
    DocCollection coll = getCollectionState(COLLECTION);

    // Send the query to a random replica of the collection.
    // Distributing the query is disabled, so we should get a single result record.
    Replica randomReplica =
        pickRandom(
            coll.getSlices().stream()
                .flatMap(s -> s.getReplicas().stream())
                .toArray(Replica[]::new));
    try (SolrClient client = getHttpSolrClient(randomReplica.getCoreUrl())) {
      response = client.query(query);
    }

    SolrDocumentList results = response.getResults();
    int size = results.size();
    assertEquals("should have 1 result", 1, size);
    NamedList<Object> header = response.getHeader();
    List<Object> localCpuTimes = header.getAll(ThreadCpuTimer.LOCAL_CPU_TIME);
    List<Object> cpuTimes = header.getAll(ThreadCpuTimer.CPU_TIME);
    assertEquals("localCpuTime should not have values", 0, localCpuTimes.size());
    assertEquals("cpuTime should only have one value", 1, cpuTimes.size());
    long cpuTime = (long) cpuTimes.getFirst();
    assertTrue("cpuTime (" + cpuTime + ") should be positive", cpuTime >= 0);
  }

  public void testWithDistrib() throws SolrServerException, IOException {
    SolrQuery query = new SolrQuery();
    query.setQuery("subject:batman OR subject:superman");
    query.addField("id");
    query.addField("subject");

    QueryResponse response = cluster.getSolrClient(COLLECTION).query(query);

    SolrDocumentList results = response.getResults();
    int size = results.size();
    assertEquals("should have 2 results", 2, size);
    NamedList<Object> header = response.getHeader();
    List<Object> localCpuTimes = header.getAll(ThreadCpuTimer.LOCAL_CPU_TIME);
    List<Object> cpuTimes = header.getAll(ThreadCpuTimer.CPU_TIME);
    assertEquals("localCpuTime should not have values", 0, localCpuTimes.size());
    assertEquals("cpuTime should only have one value", 1, cpuTimes.size());
    long cpuTime = (long) cpuTimes.getFirst();
    assertTrue("cpuTime (" + cpuTime + ") should be positive", cpuTime >= 0);
  }
}
