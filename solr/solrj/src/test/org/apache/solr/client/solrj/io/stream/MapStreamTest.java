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
import java.util.Objects;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MapStreamTest extends SolrCloudTestCase {

  private static final String COLLECTIONORALIAS = "collection1";

  private static final String id = "id";

  private static final int TIMEOUT = DEFAULT_TIMEOUT;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig(
            "conf",
            getFile("solrj")
                .toPath()
                .resolve("solr")
                .resolve("configsets")
                .resolve("streaming")
                .resolve("conf"))
        .addConfig(
            "ml",
            getFile("solrj")
                .toPath()
                .resolve("solr")
                .resolve("configsets")
                .resolve("ml")
                .resolve("conf"))
        .configure();

    String collection = COLLECTIONORALIAS;
    CollectionAdminRequest.createCollection(collection, "conf", 2, 1)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(
        collection, cluster.getZkStateReader(), false, true, TIMEOUT);
  }

  @Before
  public void cleanIndex() throws Exception {
    new UpdateRequest().deleteByQuery("*:*").commit(cluster.getSolrClient(), COLLECTIONORALIAS);
  }

  @Test
  public void testMapStream() throws Exception {

    TupleStream stream;
    List<Tuple> tuples;
    StreamContext streamContext = new StreamContext();
    StreamFactory factory =
        new StreamFactory()
            .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
            .withFunctionName("search", CloudSolrStream.class)
            .withFunctionName("select", SelectStream.class)
            .withFunctionName("tuple", TupStream.class)
            .withFunctionName("map", MapStream.class);

    String fn = factory.getFunctionName(MapStream.class);

    String clause = "map(tuple(a=\"123\",c=\"345\"),\"a=b\")";
    stream = factory.constructStream(clause);
    stream.setStreamContext(streamContext);
    tuples = getTuples(stream);
    assertTupleValue(tuples.get(0), "a", "b", "123");
    assertString(tuples.get(0), "c", "345");
  }

  public void testMapStreamUpdate() throws Exception {
    UpdateResponse resp =
        new UpdateRequest()
            .add(id, "1", "a_ss", "foo", "b_i", "1", "c_d", "3.3", "d_b", "true")
            .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;

    StreamFactory factory =
        new StreamFactory()
            .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
            .withFunctionName("search", CloudSolrStream.class)
            .withFunctionName("map", MapStream.class)
            .withFunctionName("update", UpdateStream.class)
            .withFunctionName("tuple", TupStream.class)
            .withFunctionName("select", SelectStream.class);

    // Basic test
    expression =
        StreamExpressionParser.parse(
            "search(" + COLLECTIONORALIAS + ", q=\"id:1\", fl=\"id\", sort=\"id asc\")");

    stream = factory.constructStream(Objects.requireNonNull(expression));
    StreamContext streamContext = new StreamContext();
    stream.setStreamContext(streamContext);
    tuples = getTuples(stream);
    assert (tuples.size() == 1);

    expression =
        StreamExpressionParser.parse(
            "update("
                + COLLECTIONORALIAS
                + ",map(tuple(id=\"1\",a_ss=\"bar\"),\"a_ss=add-distinct\"))");
    stream = new UpdateStream(expression, factory);
    stream.setStreamContext(streamContext);
    tuples = getTuples(stream);
    cluster.getSolrClient().commit(COLLECTIONORALIAS);
    assert (tuples.size() == 1);
    Tuple t = tuples.get(0);
    assert (!t.EOF);
    assertEquals(1, t.get("batchIndexed"));

    expression =
        StreamExpressionParser.parse(
            "search(" + COLLECTIONORALIAS + ", q=\"*:*\", fl=\"id,a_ss,c_d,*\", sort=\"id asc\")");

    stream = factory.constructStream(Objects.requireNonNull(expression));
    streamContext = new StreamContext();
    stream.setStreamContext(streamContext);
    tuples = getTuples(stream);
    assert (tuples.size() == 1);
    assertEquals("1", tuples.get(0).get("id"));
    assertEquals(1L, tuples.get(0).get("b_i"));
    assertEquals(List.of("foo", "bar"), tuples.get(0).get("a_ss"));
  }

  public boolean assertString(Tuple tuple, String fieldName, String v) throws Exception {
    String lv = (String) tuple.get(fieldName);
    if (!v.equals(lv)) {
      throw new Exception("Strings not equal:" + v + " : " + lv);
    }
    return true;
  }

  public boolean assertTupleValue(Tuple t, String f, String k, Object v) throws Exception {
    Object vv = ((Tuple) t.get(f)).get(k);
    if (!v.equals(vv)) {
      throw new Exception("Objects not equal:" + v + " : " + vv);
    }
    return true;
  }

  protected List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    tupleStream.open();
    List<Tuple> tuples = new ArrayList<>();
    for (Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
      tuples.add(t);
    }
    tupleStream.close();
    return tuples;
  }
}
