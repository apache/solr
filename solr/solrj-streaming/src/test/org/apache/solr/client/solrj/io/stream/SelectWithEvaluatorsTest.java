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
import java.util.Locale;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.eval.AddEvaluator;
import org.apache.solr.client.solrj.io.eval.GreaterThanEvaluator;
import org.apache.solr.client.solrj.io.eval.IfThenElseEvaluator;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * All base tests will be done with CloudSolrStream. Under the covers CloudSolrStream uses
 * SolrStream so SolrStream will get fully exercised through these tests.
 */
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Lucene45"})
public class SelectWithEvaluatorsTest extends SolrCloudTestCase {

  private static final String COLLECTIONORALIAS = "collection1";
  private static final int TIMEOUT = DEFAULT_TIMEOUT;
  private static final String id = "id";

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

    String collection;
    boolean useAlias = random().nextBoolean();
    if (useAlias) {
      collection = COLLECTIONORALIAS + "_collection";
    } else {
      collection = COLLECTIONORALIAS;
    }
    CollectionAdminRequest.createCollection(collection, "conf", 2, 1)
        .process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(
        collection, cluster.getZkStateReader(), false, true, TIMEOUT);
    if (useAlias) {
      CollectionAdminRequest.createAlias(COLLECTIONORALIAS, collection)
          .process(cluster.getSolrClient());
    }
  }

  @Before
  public void cleanIndex() throws Exception {
    new UpdateRequest().deleteByQuery("*:*").commit(cluster.getSolrClient(), COLLECTIONORALIAS);
  }

  @Test
  public void testSelectWithEvaluatorsStream() throws Exception {

    new UpdateRequest()
        .add(id, "1", "a_s", "foo", "b_i", "1", "c_d", "3.3", "d_b", "true")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);

    String clause;
    TupleStream stream;
    List<Tuple> tuples;
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();
    streamContext.setSolrClientCache(solrClientCache);

    StreamFactory factory =
        new StreamFactory()
            .withCollectionZkHost("collection1", cluster.getZkServer().getZkAddress())
            .withFunctionName("search", CloudSolrStream.class)
            .withFunctionName("select", SelectStream.class)
            .withFunctionName("add", AddEvaluator.class)
            .withFunctionName("if", IfThenElseEvaluator.class)
            .withFunctionName("gt", GreaterThanEvaluator.class);
    try {
      // Basic test
      clause =
          "select("
              + "id,"
              + "add(b_i,c_d) as result,"
              + "search(collection1, q=*:*, fl=\"id,a_s,b_i,c_d,d_b\", sort=\"id asc\")"
              + ")";
      stream = factory.constructStream(clause);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assertFields(tuples, "id", "result");
      assertNotFields(tuples, "a_s", "b_i", "c_d", "d_b");
      assertEquals(1, tuples.size());
      assertDouble(tuples.get(0), "result", 4.3);
      assertEquals(4.3, tuples.get(0).get("result"));
    } finally {
      solrClientCache.close();
    }
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

  protected void assertFields(List<Tuple> tuples, String... fields) throws Exception {
    for (Tuple tuple : tuples) {
      for (String field : fields) {
        if (!tuple.getFields().containsKey(field)) {
          throw new Exception(String.format(Locale.ROOT, "Expected field '%s' not found", field));
        }
      }
    }
  }

  protected void assertNotFields(List<Tuple> tuples, String... fields) throws Exception {
    for (Tuple tuple : tuples) {
      for (String field : fields) {
        if (tuple.getFields().containsKey(field)) {
          throw new Exception(String.format(Locale.ROOT, "Unexpected field '%s' found", field));
        }
      }
    }
  }

  public boolean assertLong(Tuple tuple, String fieldName, long l) throws Exception {
    long lv = (long) tuple.get(fieldName);
    if (lv != l) {
      throw new Exception("Longs not equal:" + l + " : " + lv);
    }

    return true;
  }

  public boolean assertDouble(Tuple tuple, String fieldName, double expectedValue)
      throws Exception {
    double value = (double) tuple.get(fieldName);
    if (expectedValue != value) {
      throw new Exception("Doubles not equal:" + value + " : " + expectedValue);
    }

    return true;
  }

  public boolean assertString(Tuple tuple, String fieldName, String expected) throws Exception {
    String actual = (String) tuple.get(fieldName);

    if ((null != expected || null == actual) && (null == expected || expected.equals(actual))) {
      return true;
    }
    throw new Exception("Longs not equal:" + expected + " : " + actual);
  }
}
