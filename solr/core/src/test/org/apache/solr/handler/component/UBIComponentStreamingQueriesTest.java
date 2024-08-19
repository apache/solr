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

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.io.Lang;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.UpdateStream;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.json.JsonQueryRequest;
import org.apache.solr.client.solrj.request.json.TermsFacetMap;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.json.BucketBasedJsonFacet;
import org.apache.solr.client.solrj.response.json.BucketJsonFacet;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Tests the ability for {@link UBIComponent} to stream the gathered query data to another Solr
 * index using Streaming Expressions.
 */
public class UBIComponentStreamingQueriesTest extends SolrCloudTestCase {
  public static final String COLLECTION = "conf2_col";
  public static final String UBI_COLLECTION = "ubi";

  /** One client per node */
  private static final List<SolrClient> NODE_CLIENTS = new ArrayList<>(7);

  /**
   * clients (including cloud client) for easy randomization and looping of collection level
   * requests
   */
  private static final List<SolrClient> CLIENTS = new ArrayList<>(7);

  private static String zkHost;

  @BeforeClass
  public static void setupCluster() throws Exception {

    final int numShards = usually() ? 2 : 1;
    final int numReplicas = usually() ? 2 : 1;
    final int numNodes = 1 + (numShards * numReplicas); // at least one node w/o any replicas

    // The configset ubi_enabled has the UBIComponent configured and set to log to a collection
    // called "ubi".
    // The ubi collection itself just depends on the typical _default configset.
    configureCluster(numNodes)
        .addConfig("ubi-enabled", configset("ubi-enabled"))
        .addConfig("ubi", configset("_default"))
        .configure();

    zkHost = cluster.getZkServer().getZkAddress();

    CLIENTS.add(cluster.getSolrClient());
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      final SolrClient c = getHttpSolrClient(jetty.getBaseUrl().toString());
      NODE_CLIENTS.add(c);
      CLIENTS.add(c);
    }

    assertEquals(
        "failed to create collection",
        0,
        CollectionAdminRequest.createCollection(COLLECTION, "ubi-enabled", numShards, numReplicas)
            .process(cluster.getSolrClient())
            .getStatus());

    cluster.waitForActiveCollection(COLLECTION, numShards, numShards * numReplicas);

    assertEquals(
        "failed to create UBI collection",
        0,
        CollectionAdminRequest.createCollection(UBI_COLLECTION, "_default", numShards, numReplicas)
            .process(cluster.getSolrClient())
            .getStatus());

    cluster.waitForActiveCollection(UBI_COLLECTION, numShards, numShards * numReplicas);
  }

  @AfterClass
  public static void closeClients() throws Exception {
    try {
      IOUtils.close(NODE_CLIENTS);
    } finally {
      NODE_CLIENTS.clear();
      CLIENTS.clear();
    }
  }

  @After
  public void clearCollection() throws Exception {
    assertEquals(
        "DBQ failed", 0, cluster.getSolrClient().deleteByQuery(COLLECTION, "*:*").getStatus());
    assertEquals("commit failed", 0, cluster.getSolrClient().commit(COLLECTION).getStatus());
    assertEquals(
        "DBQ failed", 0, cluster.getSolrClient().deleteByQuery(UBI_COLLECTION, "*:*").getStatus());
    assertEquals("commit failed", 0, cluster.getSolrClient().commit(UBI_COLLECTION).getStatus());
  }

  public void testWritingStreamingExpression() {
    UBIQuery ubiQuery = new UBIQuery("5678");
    ubiQuery.setUserQuery("Apple Memory");

    String clause = getClause(ubiQuery);
    assertEquals(
        "Check the decoded version for ease of comparison",
        "commit(ubi,update(ubi,tuple(id=4.0,query_id=5678,user_query=Apple Memory)))",
        URLDecoder.decode(clause, StandardCharsets.UTF_8));
    assertEquals(
        "Verify the encoded version",
        "commit%28ubi%2Cupdate%28ubi%2Ctuple%28id%3D4.0%2Cquery_id%3D5678%2Cuser_query%3DApple+Memory%29%29%29",
        clause);
  }

  public void testUsingStreamingExpressionDirectly() throws Exception {

    UBIQuery ubiQuery = new UBIQuery("5678");
    ubiQuery.setUserQuery("Apple Memory");

    TupleStream stream;
    List<Tuple> tuples;
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();

    streamContext.setSolrClientCache(solrClientCache);

    StreamFactory streamFactory = new StreamFactory().withCollectionZkHost("ubi", zkHost);

    Lang.register(streamFactory);

    String clause = getClause(ubiQuery);
    stream = streamFactory.constructStream(clause);
    stream.setStreamContext(streamContext);
    tuples = getTuples(stream);
    stream.close();
    solrClientCache.close();

    assertEquals("Total tuples returned", 1, tuples.size());
    Tuple tuple = tuples.get(0);
    assertEquals("1", tuple.getString(UpdateStream.BATCH_INDEXED_FIELD_NAME));
    assertEquals("1", tuple.getString("totalIndexed"));

    // Check the UBI collection
    final JsonQueryRequest requestFromUBICollection = new JsonQueryRequest().setQuery("id:4.0").setLimit(1);

    // Randomly grab a client, it shouldn't matter which is used to check UBI event.
    SolrClient client = getRandClient();
    final QueryResponse responseUBI = requestFromUBICollection.process(client, UBI_COLLECTION);
    try {
      assertEquals(0, responseUBI.getStatus());
      assertEquals(1, responseUBI.getResults().getNumFound());
    } catch (AssertionError e) {
      throw new AssertionError(responseUBI + " + " + client + " => " + e.getMessage(), e);
    }
  }

  private List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    tupleStream.open();
    List<Tuple> tuples = new ArrayList<>();
    for (; ; ) {
      Tuple t = tupleStream.read();
      // log.info(" ... {}", t.fields);
      if (t.EOF) {
        break;
      } else {
        tuples.add(t);
      }
    }
    tupleStream.close();
    return tuples;
  }

  private static String getClause(UBIQuery ubiQuery) {
    String clause = "commit(ubi,update(ubi,tuple(id=4.0," + ubiQuery.toTuple() + ")))";
    //String clause = "commit(ubi,update(ubi,tuple(id=4.0)))";
    //clause = URLEncoder.encode(clause, StandardCharsets.UTF_8);
    return clause;
  }

  private static String getClause() {

    String clause = "commit(ubi,update(ubi,tuple(id=add(1,3), name_s=bob)))";
    return clause;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testRandomDocs() throws Exception {

    final UpdateRequest ureq = new UpdateRequest();

    ureq.add(sdoc("id", 1, "data_s", "data_1"));
    assertEquals("add failed", 0, ureq.process(getRandClient(), COLLECTION).getStatus());
    assertEquals("commit failed", 0, getRandClient().commit(COLLECTION).getStatus());

    // query our collection to generate a UBI event and then confirm it was recorded.

    String userQuery = "hot air";
    Map queryAttributes = new HashMap();
    queryAttributes.put("results_wanted", 1);

    final JsonQueryRequest req =
        new JsonQueryRequest()
            .setQuery("*:*")
            .setLimit(1)
            .withParam("ubi", "true")
            .withParam("query_id", "123")
            .withParam("user_query", userQuery)
            .withParam("query_attributes", queryAttributes);

    // Randomly grab a client, it shouldn't matter which is used to generate the query event.
    SolrClient client = getRandClient();
    final QueryResponse rsp = req.process(client, COLLECTION);
    try {
      assertEquals(0, rsp.getStatus());
      assertEquals(1, rsp.getResults().getNumFound());
    } catch (AssertionError e) {
      throw new AssertionError(rsp + " + " + client + " => " + e.getMessage(), e);
    }

    // Check the UBI collection
    final JsonQueryRequest requestUBI = new JsonQueryRequest().setQuery("id:49").setLimit(1);

    // Randomly grab a client, it shouldn't matter which is used to check UBI event.
    client = getRandClient();
    final QueryResponse responseUBI = requestUBI.process(client, UBI_COLLECTION);
    try {
      assertEquals(0, responseUBI.getStatus());
      assertEquals(1, responseUBI.getResults().getNumFound());
    } catch (AssertionError e) {
      throw new AssertionError(responseUBI + " + " + client + " => " + e.getMessage(), e);
    }
  }

  public void randomDocs() throws Exception {

    // index some random documents, using a mix-match of batches, to various SolrClients

    final int uniqueMod = atLeast(43); // the number of unique sig values expected
    final int numBatches = atLeast(uniqueMod); // we'll add at least one doc per batch
    int docCounter = 0;
    for (int batchId = 0; batchId < numBatches; batchId++) {
      final UpdateRequest ureq = new UpdateRequest();
      final int batchSize = atLeast(2);
      for (int i = 0; i < batchSize; i++) {
        docCounter++;
        ureq.add(
            sdoc( // NOTE: No 'id' field, SignatureUpdateProcessor fills it in for us
                "data_s", (docCounter % uniqueMod)));
      }
      assertEquals("add failed", 0, ureq.process(getRandClient(), COLLECTION).getStatus());
    }
    assertEquals("commit failed", 0, getRandClient().commit(COLLECTION).getStatus());

    assertTrue(docCounter > uniqueMod);

    // query our collection and confirm no duplicates on the signature field (using faceting)
    // Check every (node) for consistency...
    final JsonQueryRequest req =
        new JsonQueryRequest()
            .setQuery("*:*")
            .setLimit(0)
            .withFacet("data_facet", new TermsFacetMap("data_s").setLimit(uniqueMod + 1));
    for (SolrClient client : CLIENTS) {
      final QueryResponse rsp = req.process(client, COLLECTION);
      try {
        assertEquals(0, rsp.getStatus());
        assertEquals(uniqueMod, rsp.getResults().getNumFound());

        final BucketBasedJsonFacet facet =
            rsp.getJsonFacetingResponse().getBucketBasedFacets("data_facet");
        assertEquals(uniqueMod, facet.getBuckets().size());
        for (BucketJsonFacet bucket : facet.getBuckets()) {
          assertEquals("Bucket " + bucket.getVal(), 1, bucket.getCount());
        }
      } catch (AssertionError e) {
        throw new AssertionError(rsp + " + " + client + " => " + e.getMessage(), e);
      }
    }
  }

  /**
   * returns a random SolrClient -- either a CloudSolrClient, or an HttpSolrClient pointed at a node
   * in our cluster.
   */
  private static SolrClient getRandClient() {
    return CLIENTS.get(random().nextInt(CLIENTS.size()));
  }

  private static String readLastLineOfFile(File file) throws IOException {
    try (ReversedLinesFileReader reader =
        ReversedLinesFileReader.builder().setFile(file).setCharset(StandardCharsets.UTF_8).get()) {
      return reader.readLine();
    }
  }
}
