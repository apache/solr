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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.CloudSolrStream;
import org.apache.solr.client.solrj.io.stream.SelectStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.UpdateStream;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpression;
import org.apache.solr.client.solrj.io.stream.expr.StreamExpressionParser;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.core.SolrCore;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.handler.LoggingStream;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Lucene45"})
public class UBIComponentStreamingQueriesTest extends SolrCloudTestCase {

  private static final String COLLECTIONORALIAS = "collection1";
  private static final int TIMEOUT = DEFAULT_TIMEOUT;
  private static final String id = "id";

  private static boolean useAlias;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig(
            "conf", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    String collection;
    useAlias = random().nextBoolean();
    if (useAlias) {
      collection = COLLECTIONORALIAS + "_collection";
    } else {
      collection = COLLECTIONORALIAS;
    }

    CollectionAdminRequest.createCollection(collection, "conf", 2, 1)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collection, 2, 2);

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
  public void testUBIQueryStream() throws Exception {

    UBIQuery ubiQuery;
    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();

    try (solrClientCache) {
      streamContext.setSolrClientCache(solrClientCache);
      StreamFactory factory =
          new StreamFactory().withFunctionName("ubiQuery", UBIQueryStream.class);
      // Basic test
      ubiQuery = new UBIQuery("123");

      expression = StreamExpressionParser.parse("ubiQuery()");
      streamContext.put("ubi-query", ubiQuery);
      stream = new UBIQueryStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assertEquals(1, tuples.size());
      assertFields(tuples, "query_id", "timestamp");
      assertString(tuples.get(0), "query_id", "123");

      assertNotNull(Instant.parse(tuples.get(0).getString("timestamp")));

      // assertNotFields(tuples, "user_query", "event_attributes");

      // Include another field to see what is returned
      ubiQuery = new UBIQuery("234");
      ubiQuery.setApplication("typeahead");

      streamContext.put("ubi-query", ubiQuery);
      stream = new UBIQueryStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assertEquals(1, tuples.size());
      assertFields(tuples, "query_id", "timestamp", "application");
      assertString(tuples.get(0), "query_id", "234");
      assertString(tuples.get(0), "application", "typeahead");

      // Introduce event_attributes map of data
      ubiQuery = new UBIQuery("345");

      @SuppressWarnings({"unchecked", "rawtypes"})
      Map<String, Object> queryAttributes = new HashMap();
      queryAttributes.put("attribute1", "one");
      queryAttributes.put("attribute2", 2);
      ubiQuery.setQueryAttributes(queryAttributes);

      streamContext.put("ubi-query", ubiQuery);
      stream = new UBIQueryStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assertEquals(1, tuples.size());
      assertFields(tuples, "query_id", "timestamp", "query_attributes");
      assertString(tuples.get(0), "query_id", "345");
      assertString(tuples.get(0), "query_attributes", "{\"attribute1\":\"one\",\"attribute2\":2}");
    }
  }

  @Test
  public void testWritingToLogUbiQueryStream() throws Exception {
    // Test that we can write out UBIQuery data cleanly to the jsonl file
    UBIQuery ubiQuery = new UBIQuery("345");
    ubiQuery.setUserQuery("Memory RAM");
    ubiQuery.setApplication("typeahead");

    @SuppressWarnings({"unchecked", "rawtypes"})
    Map<String, Object> queryAttributes = new HashMap();
    queryAttributes.put("parsed_query", "memory OR ram");
    queryAttributes.put("experiment", "secret");
    queryAttributes.put("marginBoost", 2.1);
    ubiQuery.setQueryAttributes(queryAttributes);

    StreamExpression expression;
    List<Tuple> tuples;
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();

    try (solrClientCache) {
      streamContext.setSolrClientCache(solrClientCache);
      StreamFactory factory =
          new StreamFactory()
              .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
              .withFunctionName("search", CloudSolrStream.class)
              .withFunctionName("ubiQuery", UBIQueryStream.class)
              .withFunctionName("logging", LoggingStream.class);

      expression = StreamExpressionParser.parse("logging(test.jsonl,ubiQuery())");
      streamContext.put("ubi-query", ubiQuery);
      streamContext.put("solr-core", findSolrCore());
      LoggingStream stream = new LoggingStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      assertEquals(1, tuples.size());
      assertFields(tuples, "totalIndexed");
      assertLong(tuples.get(0), "totalIndexed", 1);

      // Someday when we have parseJSON() streaming expression we can replace this.
      Path filePath = stream.getFilePath();
      try (ReversedLinesFileReader reader =
          new ReversedLinesFileReader.Builder()
              .setCharset(StandardCharsets.UTF_8)
              .setPath(filePath)
              .get()) {
        String jsonLine = reader.readLine(); // Read the last line
        assertNotNull(jsonLine);
        ObjectMapper objectMapper = new ObjectMapper();
        @SuppressWarnings({"unchecked", "rawtypes"})
        Map ubiQueryAsMap = objectMapper.readValue(jsonLine, Map.class);
        assertEquals(ubiQuery.getQueryId(), ubiQueryAsMap.get("query_id"));
        assertEquals(ubiQuery.getApplication(), ubiQueryAsMap.get("application"));
        assertNotNull(ubiQueryAsMap.get("timestamp"));
        assertEquals(
            "{\"experiment\":\"secret\",\"marginBoost\":2.1,\"parsed_query\":\"memory OR ram\"}",
            ubiQueryAsMap.get("query_attributes"));
      }
    }
  }

  @Test
  public void testWritingToSolrUbiQueryStream() throws Exception {
    // Test that we can write out UBIQuery, especially the queryAttributes map, to Solr collection

    UBIQuery ubiQuery = new UBIQuery("345");
    ubiQuery.setUserQuery("Memory RAM");
    ubiQuery.setApplication("typeahead");

    @SuppressWarnings({"unchecked", "rawtypes"})
    Map<String, Object> queryAttributes = new HashMap();
    queryAttributes.put("parsed_query", "memory OR ram");
    queryAttributes.put("experiment", "secret");
    queryAttributes.put("marginBoost", 2.1);
    ubiQuery.setQueryAttributes(queryAttributes);

    StreamExpression expression;
    TupleStream stream;
    List<Tuple> tuples;
    StreamContext streamContext = new StreamContext();
    SolrClientCache solrClientCache = new SolrClientCache();

    // String zkHost = cluster.getZkServer().getZkAddress();

    try (solrClientCache) {
      streamContext.setSolrClientCache(solrClientCache);
      StreamFactory factory =
          new StreamFactory()
              .withCollectionZkHost(COLLECTIONORALIAS, cluster.getZkServer().getZkAddress())
              .withFunctionName("search", CloudSolrStream.class)
              .withFunctionName("update", UpdateStream.class)
              .withFunctionName("select", SelectStream.class)
              .withFunctionName("ubiQuery", UBIQueryStream.class);

      expression =
          StreamExpressionParser.parse(
              "update("
                  + COLLECTIONORALIAS
                  + ", batchSize=5, select(\n"
                  + "      ubiQuery(),\n"
                  + "      query_id as id,\n"
                  + "      timestamp,\n"
                  + "      application,\n"
                  + "      user_query,\n"
                  + "      query_attributes\n"
                  + "    ))");
      streamContext.put("ubi-query", ubiQuery);
      stream = new UpdateStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);

      cluster.getSolrClient().commit(COLLECTIONORALIAS);

      assertEquals(1, tuples.size());
      Tuple t = tuples.get(0);
      assertFalse(t.EOF);
      assertEquals(1, t.get("batchIndexed"));
      assertEquals(1L, t.get("totalIndexed"));

      // Ensure that destinationCollection actually has the new ubi query docs.
      expression =
          StreamExpressionParser.parse(
              "search(" + COLLECTIONORALIAS + ", q=*:*, fl=\"id,*\", sort=\"id asc\")");
      stream = new CloudSolrStream(expression, factory);
      stream.setStreamContext(streamContext);
      tuples = getTuples(stream);
      assertEquals(1, tuples.size());

      Tuple tuple = tuples.get(0);
      assertEquals(ubiQuery.getQueryId(), tuple.get("id"));
      assertEquals(ubiQuery.getApplication(), tuple.get("application"));
      assertEquals(ubiQuery.getUserQuery(), tuple.get("user_query"));
      assertEquals(ubiQuery.getTimestamp(), tuple.getDate("timestamp"));
      assertEquals(
          "{\"experiment\":\"secret\",\"marginBoost\":2.1,\"parsed_query\":\"memory OR ram\"}",
          tuple.get("query_attributes"));
    }
  }

  protected List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    List<Tuple> tuples = new ArrayList<>();

    try (tupleStream) {
      tupleStream.open();
      for (Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
        tuples.add(t);
      }
    }
    return tuples;
  }

  protected void assertOrder(List<Tuple> tuples, int... ids) throws Exception {
    assertOrderOf(tuples, "id", ids);
  }

  protected void assertOrderOf(List<Tuple> tuples, String fieldName, int... ids) throws Exception {
    int i = 0;
    for (int val : ids) {
      Tuple t = tuples.get(i);
      String tip = t.getString(fieldName);
      if (!tip.equals(Integer.toString(val))) {
        throw new Exception("Found value:" + tip + " expecting:" + val);
      }
      ++i;
    }
  }

  public boolean assertString(Tuple tuple, String fieldName, String expected) throws Exception {
    String actual = (String) tuple.get(fieldName);

    if (!Objects.equals(expected, actual)) {
      throw new Exception("Longs not equal:" + expected + " : " + actual);
    }

    return true;
  }

  public boolean assertLong(Tuple tuple, String fieldName, long l) throws Exception {
    long lv = (long) tuple.get(fieldName);
    if (lv != l) {
      throw new Exception("Longs not equal:" + l + " : " + lv);
    }

    return true;
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

  protected boolean assertGroupOrder(Tuple tuple, int... ids) throws Exception {
    List<?> group = (List<?>) tuple.get("tuples");
    int i = 0;
    for (int val : ids) {
      Map<?, ?> t = (Map<?, ?>) group.get(i);
      Long tip = (Long) t.get("id");
      if (tip.intValue() != val) {
        throw new Exception("Found value:" + tip.intValue() + " expecting:" + val);
      }
      ++i;
    }
    return true;
  }

  private static SolrCore findSolrCore() {
    for (JettySolrRunner solrRunner : cluster.getJettySolrRunners()) {
      for (SolrCore solrCore : solrRunner.getCoreContainer().getCores()) {
        if (solrCore != null) {
          return solrCore;
        }
      }
    }
    throw new RuntimeException("Didn't find any valid cores.");
  }
}
