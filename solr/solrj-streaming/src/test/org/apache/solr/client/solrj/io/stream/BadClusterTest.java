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
package org.apache.solr.client.solrj.io.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionNamedParameter;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.junit.BeforeClass;
import org.junit.Test;

/**
*  Tests behaviors of CloudSolrStream when the cluster is behaving badly.
**/

@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class BadClusterTest extends SolrCloudTestCase {

  private static final String collection = "streams";
  private static final String id = "id";

  private static final StreamFactory streamFactory = new StreamFactory()
      .withFunctionName("search", CloudSolrStream.class);

  private static String zkHost;

  @BeforeClass
  public static void configureCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
        .configure();

    CollectionAdminRequest.createCollection(collection, "conf", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 1, 1);

    zkHost = cluster.getZkServer().getZkAddress();
    streamFactory.withCollectionZkHost(collection, zkHost);
  }

  // test order is important because the cluster progressively gets worse, but it is only created once in BeforeClass as in other tests
  // ordering can not be strictly enforced with JUnit annotations because of parallel executions, so we have this aggregated test instead
  @Test
  public void testBadCluster() throws Exception {
    testEmptyCollection();
    testAllNodesDown();
    testClusterShutdown();
  }

  private void testEmptyCollection() throws Exception {
    CloudSolrStream stream = new CloudSolrStream(buildSearchExpression(), streamFactory);
    assertEquals(0, getTuples(stream).size());
  }

  private void testAllNodesDown() throws Exception {

    CloudSolrStream stream = new CloudSolrStream(buildSearchExpression(), streamFactory);
    cluster.expireZkSession(cluster.getReplicaJetty(getReplicas().get(0)));

    try {
      getTuples(stream);
      fail("Expected IOException");
    } catch (IOException ioe) {
    }
  }

  private void testClusterShutdown() throws Exception {

    CloudSolrStream stream = new CloudSolrStream(buildSearchExpression(), streamFactory);
    cluster.shutdown();

    try {
      getTuples(stream);
      fail("Expected IOException: SolrException: TimeoutException");
    } catch (IOException ioe) {
      SolrException se = (SolrException) ioe.getCause();
      TimeoutException te = (TimeoutException) se.getCause();
      assertNotNull(te);
    }
  }

  private StreamExpression buildSearchExpression() {
    StreamExpression expression = new StreamExpression("search");
    expression.addParameter(collection);
    expression.addParameter(new StreamExpressionNamedParameter(CommonParams.Q, "*:*"));
    expression.addParameter(new StreamExpressionNamedParameter(CommonParams.FL, id));
    expression.addParameter(new StreamExpressionNamedParameter(CommonParams.SORT, id + " asc"));
    return expression;
  }

  private List<Replica> getReplicas() throws IOException {
    return TupleStream.getReplicas(zkHost, collection, null, new MultiMapSolrParams(Map.of()));
  }

  private List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    tupleStream.open();
    List<Tuple> tuples = new ArrayList<>();
    for(;;) {
      Tuple t = tupleStream.read();
      if(t.EOF) {
        break;
      } else {
        tuples.add(t);
      }
    }
    tupleStream.close();
    return tuples;
  }
}