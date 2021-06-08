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

package org.apache.solr.handler.sql;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.util.Precision;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.io.Tuple;
import org.apache.solr.client.solrj.io.stream.SolrStream;
import org.apache.solr.client.solrj.io.stream.StreamContext;
import org.apache.solr.client.solrj.io.stream.TupleStream;
import org.apache.solr.client.solrj.io.stream.expr.StreamFactory;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.SolrDefaultStreamFactory;
import org.apache.solr.util.LogLevel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verify auto-plist with rollup over a facet expression when using collection alias over multiple collections.
 */
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Lucene45"})
@LogLevel("org.apache.solr.handler.sql=DEBUG;org.apache.solr.client.solrj.io.stream=DEBUG")
public class SQLAnalyticsTest extends SolrCloudTestCase {

  private static final String ALIAS_NAME = "SOME_ALIAS_WITH_MANY_COLLS";

  private static final String id = "id";
  private static final int NUM_COLLECTIONS = 4; // this test requires at least 2 collections, each with multiple shards
  private static final int NUM_DOCS_PER_COLLECTION = 100;
  private static final int NUM_SHARDS_PER_COLLECTION = 4;
  private static final int CARDINALITY = 10;
  private static final RandomGenerator rand = new JDKRandomGenerator(5150);
  private static List<String> listOfCollections;
  private static SolrClientCache solrClientCache;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.tests.numeric.dv", "true");

    configureCluster(NUM_COLLECTIONS).withMetrics(false)
        .addConfig("conf", configset("sql"))
        .configure();
    cleanup();
    setupCollectionsAndAlias();

    solrClientCache = new SolrClientCache();
  }

  /**
   * setup the testbed with necessary collections, documents, and alias
   */
  public static void setupCollectionsAndAlias() throws Exception {

    final NormalDistribution[] dists = new NormalDistribution[CARDINALITY];
    for (int i = 0; i < dists.length; i++) {
      dists[i] = new NormalDistribution(rand, i + 1, 1d);
    }

    Locale[] locales = Locale.getAvailableLocales();

    // used for creating a int holding date values, like 20210101
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    final Calendar cal = Calendar.getInstance();
    cal.set(2021, 0, 1);

    List<String> collections = new ArrayList<>(NUM_COLLECTIONS);
    final List<Exception> errors = new LinkedList<>();
    Stream.iterate(1, n -> n + 1).limit(NUM_COLLECTIONS).forEach(colIdx -> {
      final String collectionName = "coll" + colIdx;
      collections.add(collectionName);
      try {
        CollectionAdminRequest.createCollection(collectionName, "conf", NUM_SHARDS_PER_COLLECTION, 1).process(cluster.getSolrClient());
        cluster.waitForActiveCollection(collectionName, NUM_SHARDS_PER_COLLECTION, NUM_SHARDS_PER_COLLECTION);

        // want a variable num of docs per collection so that avg of avg does not work ;-)
        final int numDocsInColl = colIdx % 2 == 0 ? NUM_DOCS_PER_COLLECTION / 2 : NUM_DOCS_PER_COLLECTION;
        final int limit = NUM_COLLECTIONS == 1 ? NUM_DOCS_PER_COLLECTION * 2 : numDocsInColl;
        final AtomicInteger incr = new AtomicInteger(0);
        UpdateRequest ur = new UpdateRequest();
        Stream.iterate(0, n -> n + 1).limit(limit)
            .forEach(docId -> ur.add(id, UUID.randomUUID().toString(),
                "a_s", "hello" + docId,
                "a_i", String.valueOf(docId % CARDINALITY),
                "b_s", rand.nextBoolean() ? "foo" : null,
                "b_i", rand.nextBoolean() ? "1" : "0",
                "dateAsInt", nextDateAsInt(cal, incr.getAndIncrement(), sdf),
                "locale_s", locales[rand.nextInt(locales.length)].toString(),
                "a_d", String.valueOf(dists[docId % dists.length].sample())));
        ur.commit(cluster.getSolrClient(), collectionName);
      } catch (SolrServerException | IOException e) {
        errors.add(e);
      }
    });

    if (!errors.isEmpty()) {
      throw errors.get(0);
    }

    listOfCollections = collections;
    String aliasedCollectionString = String.join(",", collections);
    CollectionAdminRequest.createAlias(ALIAS_NAME, aliasedCollectionString).process(cluster.getSolrClient());
  }

  private static String nextDateAsInt(Calendar cal, int dateIncr, SimpleDateFormat sdf) {
    Calendar cloned = (Calendar)cal.clone();
    cloned.add(Calendar.DATE, dateIncr);
    return sdf.format(cloned.getTime());
  }

  public static void cleanup() throws Exception {
    if (cluster != null && cluster.getSolrClient() != null) {
      // cleanup the alias and the collections behind it
      CollectionAdminRequest.deleteAlias(ALIAS_NAME).process(cluster.getSolrClient());
      if (listOfCollections != null) {
        final List<Exception> errors = new LinkedList<>();
        listOfCollections.stream().map(CollectionAdminRequest::deleteCollection).forEach(c -> {
          try {
            c.process(cluster.getSolrClient());
          } catch (SolrServerException | IOException e) {
            errors.add(e);
          }
        });
        if (!errors.isEmpty()) {
          throw errors.get(0);
        }
      }
    }
  }

  @AfterClass
  public static void after() throws Exception {
    cleanup();

    if (solrClientCache != null) {
      solrClientCache.close();
    }
  }

  /**
   * Test parallelized calls to facet expression, one for each collection in the alias
   */
  @Test
  public void testMinMaxInteger() throws Exception {
    SolrParams params = sqlParams("SELECT min(dateAsInt), max(dateAsInt) from $ALIAS");
    List<SortedMap<Object, Object>> collect = new LinkedList<>();
    //for (int i=0; i < 100; i++) {
      List<Tuple> tuples = getTuples(params, sqlUrl(ALIAS_NAME));
      collect.add(toMap(tuples.get(0)));
    //}

    for (int i=0; i < collect.size(); i++) {
      System.out.println(">> "+i+": "+collect.get(i));
    }
  }

  /*
  
   */
  @Test
  public void testGroupByIntDate() throws Exception {
    SolrParams params = sqlParams("SELECT dateAsInt, count(*) FROM $ALIAS GROUP BY dateAsInt ORDER BY dateAsInt desc limit 1000");
    List<Tuple> tuples = getTuples(params, sqlUrl(ALIAS_NAME));
    tuples.forEach(t -> System.out.println(toMap(t)));
  }

  @Test
  public void testColIsNotNull() throws Exception {
    SolrParams params = sqlParams("SELECT dateAsInt, a_s, b_s FROM $ALIAS WHERE b_s=\"foo\"");
    List<Tuple> tuples = getTuples(params, sqlUrl(ALIAS_NAME));
    tuples.forEach(t -> System.out.println(toMap(t)));
  }

  @Test
  public void testWhereWildcard() throws Exception {
    SolrParams params = sqlParams("SELECT a_s, count(*) as cnt FROM $ALIAS WHERE a_s='hello*' GROUP BY a_s ORDER BY count(*) desc LIMIT 10");
    List<Tuple> tuples = getTuples(params, sqlUrl(ALIAS_NAME));
    System.out.println("\n\n>> SQL returned: "+tuples.size());
    tuples.forEach(t -> System.out.println(toMap(t)));
  }


  /*
  select interpretation_requestLocale, interpretation_clusteringSignature, count(*) as requestCount
  from production_canonical_view where requestDate >= 20210501 and requestDate <= '20210507' and interpretation_flowDomain = 'cannedDialog' and interpretation_requestLocale = 'en_AU' group by interpretation_requestLocale, interpretation_clusteringSignature order by count(*) desc limit 500
   */
  @Test
  public void testFilterByIntDate() throws Exception {
    SolrParams params = sqlParams("SELECT locale_s, count(*) FROM $ALIAS WHERE dateAsInt >= 20210101 GROUP BY locale_s ORDER BY count(*) desc limit 10");
    List<Tuple> tuples = getTuples(params, sqlUrl(ALIAS_NAME));
    tuples.forEach(t -> System.out.println(t.getFields()));
  }

  private String sqlUrl(String aliasName) {
    return cluster.getJettySolrRunners().get(0).getBaseUrl().toString()+"/"+aliasName;
  }

  private SolrParams sqlParams(String sql) {
    String evalSql = sql.replace("$ALIAS", ALIAS_NAME);
    return toParams(Map.of(CommonParams.QT, "/sql", "stmt", evalSql));
  }

  private SolrParams toParams(Map<String,String> map) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    for (Map.Entry<String,String> e : map.entrySet()) {
      params.set(e.getKey(), e.getValue());
    }
    return params;
  }

  protected List<Tuple> getTuples(final SolrParams params, String baseUrl) throws IOException {
    return getTuples(new SolrStream(baseUrl, params));
  }

  // assert results are the same, with some sorting and rounding of floating point values
  private void assertListOfTuplesEquals(List<Tuple> exp, List<Tuple> act) {
    List<SortedMap<Object, Object>> expList = exp.stream().map(this::toComparableMap).collect(Collectors.toList());
    List<SortedMap<Object, Object>> actList = act.stream().map(this::toComparableMap).collect(Collectors.toList());
    assertEquals(expList, actList);
  }

  private SortedMap<Object, Object> toComparableMap(Tuple t) {
    SortedMap<Object, Object> cmap = new TreeMap<>();
    for (Map.Entry<Object, Object> e : t.getFields().entrySet()) {
      Object value = e.getValue();
      if (value instanceof Double) {
        cmap.put(e.getKey(), Precision.round((Double) value, 5));
      } else if (value instanceof Float) {
        cmap.put(e.getKey(), Precision.round((Float) value, 3));
      } else {
        cmap.put(e.getKey(), e.getValue());
      }
    }
    return cmap;
  }

  private SortedMap<Object, Object> toMap(Tuple t) {
    SortedMap<Object, Object> cmap = new TreeMap<>();
    for (Map.Entry<Object, Object> e : t.getFields().entrySet()) {
      cmap.put(e.getKey(), e.getValue());
    }
    return cmap;
  }

  List<Tuple> getTuples(TupleStream tupleStream) throws IOException {
    //StreamFactory factory = new SolrDefaultStreamFactory().withDefaultZkHost(cluster.getZkServer().getZkAddress());
    //System.out.println(">> stream: "+tupleStream.toExplanation(factory));
    List<Tuple> tuples = new ArrayList<>();
    try (tupleStream) {
      tupleStream.open();
      for (Tuple t = tupleStream.read(); !t.EOF; t = tupleStream.read()) {
        tuples.add(t);
      }
    }
    return tuples;
  }
}
