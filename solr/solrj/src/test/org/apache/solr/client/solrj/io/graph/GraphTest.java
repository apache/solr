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

package org.apache.solr.client.solrj.io.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.StreamingTest;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.SolrParams;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
/**
 *  All base tests will be done with CloudSolrStream. Under the covers CloudSolrStream uses SolrStream so
 *  SolrStream will get fully exercised through these tests.
 *
 **/

@LuceneTestCase.Slow
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class GraphTest extends SolrCloudTestCase {

  private static final String COLLECTION = "collection1";

  private static final String id = "id";

  private static final int TIMEOUT = 30;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
        .configure();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 2, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 2, 2);
  }

  @Before
  public void cleanIndex() throws Exception {
    new UpdateRequest()
        .deleteByQuery("*:*")
        .commit(cluster.getSolrClient(), COLLECTION);
  }

  @Test
  // commented 15-Sep-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
  public void testShortestPathStream() throws Exception {

    final boolean with_SOLR_15546_fix = true;

    final String ORIGINAL_TITLE = random().nextBoolean() ? "Original 'P' paperback" : "Original \"H\" hardback";
    final String TRANSLATED_TITLE = "Translation";

    new UpdateRequest()
        .add(id, "0", "from_s", "jim", "to_s", "mike", "predicate_s", "knows")
        .add(id, "1", "from_s", "jim", "to_s", "dave", "predicate_s", "knows")
        .add(id, "2", "from_s", "jim", "to_s", "stan", "predicate_s", "knows")
        .add(id, "3", "from_s", "dave", "to_s", "stan", "predicate_s", "knows")
        .add(id, "4", "from_s", "dave", "to_s", "bill", "predicate_s", "knows")
        .add(id, "5", "from_s", "dave", "to_s", "mike", "predicate_s", "knows")
        .add(id, "20", "from_s", "dave", "to_s", "alex", "predicate_s", "knows")
        .add(id, "21", "from_s", "alex", "to_s", "steve", "predicate_s", "knows")
        .add(id, "6", "from_s", "stan", "to_s", "alice", "predicate_s", "knows")
        .add(id, "7", "from_s", "stan", "to_s", "mary", "predicate_s", "knows")
        .add(id, "8", "from_s", "stan", "to_s", "dave", "predicate_s", "knows")
        .add(id, "10", "from_s", "mary", "to_s", "mike", "predicate_s", "knows")
        .add(id, "11", "from_s", "mary", "to_s", "max", "predicate_s", "knows")
        .add(id, "12", "from_s", "mary", "to_s", "jim", "predicate_s", "knows")
        .add(id, "13", "from_s", "mary", "to_s", "steve", "predicate_s", "knows")
        // SOLR-15546: fromNode and toNode contains colon
        .add(id, "14", "from_s", "https://aaa", "to_s", "https://bbb", "predicate_s", "links")
        .add(id, "15", "from_s", "https://bbb", "to_s", "https://ccc", "predicate_s", "links")
        // SOLR-15546: fromNode and toNode contains space
        .add(id, "16", "from_s", "Author", "to_s", TRANSLATED_TITLE, "predicate_s", "author_to_book")
        .add(id, "17", "from_s", TRANSLATED_TITLE, "to_s", "Translator", "predicate_s", "book_to_translator")
        .add(id, "18", "from_s", "Ann Author", "to_s", TRANSLATED_TITLE, "predicate_s", "author_to_book")
        .add(id, "19", "from_s", TRANSLATED_TITLE, "to_s", "Tim Translator", "predicate_s", "book_to_translator")
        .add(id, "22", "from_s", "Ann Author", "to_s", ORIGINAL_TITLE, "predicate_s", "author_to_book")
        .add(id, "23", "from_s", ORIGINAL_TITLE, "to_s", "Tim Translator", "predicate_s", "book_to_translator")
        .commit(cluster.getSolrClient(), COLLECTION);

    List<Tuple> tuples = null;
    Set<String> paths = null;
    ShortestPathStream stream = null;
    String zkHost = cluster.getZkServer().getZkAddress();
    StreamContext context = new StreamContext();
    SolrClientCache cache = new SolrClientCache();
    context.setSolrClientCache(cache);

    SolrParams sParams = StreamingTest.mapParams("fq", "predicate_s:knows");

    stream = new ShortestPathStream(zkHost,
                                                       "collection1",
                                                       "jim",
                                                       "steve",
                                                        "from_s",
                                                        "to_s",
                                                        sParams,
                                                        20,
                                                        3,
                                                        6);



    stream.setStreamContext(context);
    paths = new HashSet<>();
    tuples = getTuples(stream);

    assertTrue(tuples.size() == 2);

    for(Tuple tuple : tuples) {
      paths.add(tuple.getStrings("path").toString());
    }

    assertTrue(paths.contains("[jim, dave, alex, steve]"));
    assertTrue(paths.contains("[jim, stan, mary, steve]"));

    //Test with batch size of 1

    sParams = StreamingTest.mapParams("fq", "predicate_s:knows");

    stream = new ShortestPathStream(zkHost,
        "collection1",
        "jim",
        "steve",
        "from_s",
        "to_s",
        sParams,
        1,
        3,
        6);

    stream.setStreamContext(context);
    paths = new HashSet<>();
    tuples = getTuples(stream);

    assertTrue(tuples.size() == 2);

    for(Tuple tuple : tuples) {
      paths.add(tuple.getStrings("path").toString());
    }

    assertTrue(paths.contains("[jim, dave, alex, steve]"));
    assertTrue(paths.contains("[jim, stan, mary, steve]"));

    //Test with bad predicate

    sParams = StreamingTest.mapParams("fq", "predicate_s:crap");

    stream = new ShortestPathStream(zkHost,
        "collection1",
        "jim",
        "steve",
        "from_s",
        "to_s",
        sParams,
        1,
        3,
        6);

    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assertTrue(tuples.size() == 0);

    //Test with depth 2

    sParams = StreamingTest.mapParams("fq", "predicate_s:knows");

    stream = new ShortestPathStream(zkHost,
        "collection1",
        "jim",
        "steve",
        "from_s",
        "to_s",
        sParams,
        1,
        3,
        2);

    stream.setStreamContext(context);
    tuples = getTuples(stream);

    assertTrue(tuples.size() == 0);



    //Take out alex
    sParams = StreamingTest.mapParams("fq", "predicate_s:knows NOT to_s:alex");

    stream = new ShortestPathStream(zkHost,
        "collection1",
        "jim",
        "steve",
        "from_s",
        "to_s",
        sParams,
        10,
        3,
        6);

    stream.setStreamContext(context);
    paths = new HashSet<>();
    tuples = getTuples(stream);
    assertTrue(tuples.size() == 1);

    for(Tuple tuple : tuples) {
      paths.add(tuple.getStrings("path").toString());
    }

    assertTrue(paths.contains("[jim, stan, mary, steve]"));

    if (with_SOLR_15546_fix) {
    // SOLR-15546: fromNode and toNode contains colon
    stream = new ShortestPathStream(zkHost,
        "collection1",
        "https://aaa",
        "https://bbb",
        "from_s",
        "to_s",
        StreamingTest.mapParams("fq", "predicate_s:links"),
        10,
        3,
        6);

    stream.setStreamContext(context);
    paths = new HashSet<>();
    tuples = getTuples(stream);
    assertTrue(tuples.size() == 1);

    for(Tuple tuple : tuples) {
      paths.add(tuple.getStrings("path").toString());
    }

    assertTrue(paths.contains("[https://aaa, https://bbb]"));

    // SOLR-15546: fromNode and toNode and interim node contains colon
    stream = new ShortestPathStream(zkHost,
        "collection1",
        "https://aaa",
        "https://ccc",
        "from_s",
        "to_s",
        StreamingTest.mapParams("fq", "predicate_s:links"),
        10,
        3,
        6);

    stream.setStreamContext(context);
    paths = new HashSet<>();
    tuples = getTuples(stream);
    assertTrue(tuples.size() == 1);

    for(Tuple tuple : tuples) {
      paths.add(tuple.getStrings("path").toString());
    }

    assertTrue(paths.contains("[https://aaa, https://bbb, https://ccc]"));
    }

    // SOLR-15546: fromNode and toNode contains colon
    stream = new ShortestPathStream(zkHost,
        "collection1",
        "Ann Author",
        "Tim Translator",
        "from_s",
        "to_s",
        StreamingTest.mapParams(),
        10,
        3,
        6);

    stream.setStreamContext(context);
    paths = new HashSet<>();
    tuples = getTuples(stream);

    for(Tuple tuple : tuples) {
      paths.add(tuple.getStrings("path").toString());
    }

    if (with_SOLR_15546_fix) {
      if (ORIGINAL_TITLE.contains("\"")) {
        assertEquals(1, tuples.size());
        // double quotes in the interim ORIGINAL_TITLE node were not matched
        assertTrue(paths.contains("[Ann Author, "+TRANSLATED_TITLE+", Tim Translator]"));
      } else {
        assertEquals(2, tuples.size());
        assertTrue(paths.contains("[Ann Author, "+ORIGINAL_TITLE+", Tim Translator]"));
        assertTrue(paths.contains("[Ann Author, "+TRANSLATED_TITLE+", Tim Translator]"));
      }
    } else {
      assertEquals(1, tuples.size());
      // "Ann Author" fromNode got interpreted as "Ann" OR "Author"
      assertTrue(paths.contains("[Author, "+TRANSLATED_TITLE+", Tim Translator]"));
    }

    cache.close();
  }

  protected List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
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

  public boolean assertLong(Tuple tuple, String fieldName, long l) throws Exception {
    long lv = (long)tuple.get(fieldName);
    if(lv != l) {
      throw new Exception("Longs not equal:"+l+" : "+lv);
    }

    return true;
  }

}

