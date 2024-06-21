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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

/** */
public class DebugComponentTest extends SolrTestCaseJ4 {

  private static final String ANY_RID = "ANY_RID";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
    assertU(adoc("id", "1", "title", "this is a title.", "inStock_b1", "true"));
    assertU(adoc("id", "2", "title", "this is another title.", "inStock_b1", "true"));
    assertU(adoc("id", "3", "title", "Mary had a little lamb.", "inStock_b1", "false"));
    assertU(
        adoc("id", "4", "title", "Vegeta is the prince of all saiyans.", "inStock_b1", "false"));
    assertU(
        adoc(
            "id",
            "5",
            "title",
            "Kakaroth aka Son Goku is a legendary saiyan warrior",
            "inStock_b1",
            "false"));
    assertU(
        adoc("id", "6", "title", "Gohan is the first saiyan-human hybrid.", "inStock_b1", "false"));
    assertU(commit());
  }

  @Test
  public void testBasicInterface() {
    // make sure the basics are in place
    assertQ(
        req("q", "*:*", CommonParams.DEBUG_QUERY, "true"),
        "//str[@name='rawquerystring']='*:*'",
        "//str[@name='querystring']='*:*'",
        "//str[@name='parsedquery']='MatchAllDocsQuery(*:*)'",
        "//str[@name='parsedquery_toString']='*:*'",
        "count(//lst[@name='explain']/*)=6",
        "//lst[@name='explain']/str[@name='1']",
        "//lst[@name='explain']/str[@name='2']",
        "//lst[@name='explain']/str[@name='3']",
        "//str[@name='QParser']", // make sure the QParser is specified
        "count(//lst[@name='timing']/*)=3", // should be three pieces to timings
        "//lst[@name='timing']/double[@name='time']", // make sure we have a time value, but don't
        // specify its result
        "count(//lst[@name='prepare']/*)>0",
        "//lst[@name='prepare']/double[@name='time']",
        "count(//lst[@name='process']/*)>0",
        "//lst[@name='process']/double[@name='time']");
  }

  // Test the ability to specify which pieces to include

  @Test
  public void testPerItemInterface() {
    // Same as debugQuery = true
    assertQ(
        req("q", "*:*", "debug", "true"),
        "//str[@name='rawquerystring']='*:*'",
        "//str[@name='querystring']='*:*'",
        "//str[@name='parsedquery']='MatchAllDocsQuery(*:*)'",
        "//str[@name='parsedquery_toString']='*:*'",
        "//str[@name='QParser']", // make sure the QParser is specified
        "count(//lst[@name='explain']/*)=6",
        "//lst[@name='explain']/str[@name='1']",
        "//lst[@name='explain']/str[@name='2']",
        "//lst[@name='explain']/str[@name='3']",
        "count(//lst[@name='timing']/*)=3", // should be three pieces to timings
        "//lst[@name='timing']/double[@name='time']", // make sure we have a time value, but don't
        // specify its result
        "count(//lst[@name='prepare']/*)>0",
        "//lst[@name='prepare']/double[@name='time']",
        "count(//lst[@name='process']/*)>0",
        "//lst[@name='process']/double[@name='time']");
    // timing only
    assertQ(
        req("q", "*:*", "debug", CommonParams.TIMING),
        "count(//str[@name='rawquerystring'])=0",
        "count(//str[@name='querystring'])=0",
        "count(//str[@name='parsedquery'])=0",
        "count(//str[@name='parsedquery_toString'])=0",
        "count(//lst[@name='explain']/*)=0",
        "count(//str[@name='QParser'])=0", // make sure the QParser is specified
        "count(//lst[@name='timing']/*)=3", // should be three pieces to timings
        "//lst[@name='timing']/double[@name='time']", // make sure we have a time value, but don't
        // specify its result
        "count(//lst[@name='prepare']/*)>0",
        "//lst[@name='prepare']/double[@name='time']",
        "count(//lst[@name='process']/*)>0",
        "//lst[@name='process']/double[@name='time']");
    // query only
    assertQ(
        req("q", "*:*", "debug", CommonParams.QUERY),
        "//str[@name='rawquerystring']='*:*'",
        "//str[@name='querystring']='*:*'",
        "//str[@name='parsedquery']='MatchAllDocsQuery(*:*)'",
        "//str[@name='parsedquery_toString']='*:*'",
        "count(//lst[@name='explain']/*)=0",
        "//str[@name='QParser']", // make sure the QParser is specified
        "count(//lst[@name='timing']/*)=0");

    // explains
    assertQ(
        req("q", "*:*", "debug", CommonParams.RESULTS),
        "count(//str[@name='rawquerystring'])=0",
        "count(//str[@name='querystring'])=0",
        "count(//str[@name='parsedquery'])=0",
        "count(//str[@name='parsedquery_toString'])=0",
        "count(//lst[@name='explain']/*)=6",
        "//lst[@name='explain']/str[@name='1']",
        "//lst[@name='explain']/str[@name='2']",
        "//lst[@name='explain']/str[@name='3']",
        "count(//str[@name='QParser'])=0", // make sure the QParser is specified
        "count(//lst[@name='timing']/*)=0");

    assertQ(
        req("q", "*:*", "debug", CommonParams.RESULTS, "debug", CommonParams.QUERY),
        "//str[@name='rawquerystring']='*:*'",
        "//str[@name='querystring']='*:*'",
        "//str[@name='parsedquery']='MatchAllDocsQuery(*:*)'",
        "//str[@name='parsedquery_toString']='*:*'",
        "//str[@name='QParser']", // make sure the QParser is specified
        "count(//lst[@name='explain']/*)=6",
        "//lst[@name='explain']/str[@name='1']",
        "//lst[@name='explain']/str[@name='2']",
        "//lst[@name='explain']/str[@name='3']",
        "count(//lst[@name='timing']/*)=0");

    // Grouping
    assertQ(
        req(
            "q",
            "*:*",
            "debug",
            CommonParams.RESULTS,
            "group",
            CommonParams.TRUE,
            "group.field",
            "inStock_b1",
            "debug",
            CommonParams.TRUE),
        "//str[@name='rawquerystring']='*:*'",
        "count(//lst[@name='explain']/*)=2");
  }

  @Test
  public void testModifyRequestTrack() {
    DebugComponent component = new DebugComponent();
    List<SearchComponent> components = new ArrayList<>(1);
    components.add(component);
    for (int i = 0; i < 10; i++) {
      SolrQueryRequest req =
          req("q", "test query", "distrib", "true", CommonParams.REQUEST_ID, "123456-my_rid");
      SolrQueryResponse resp = new SolrQueryResponse();
      ResponseBuilder rb = new ResponseBuilder(req, resp, components);
      ShardRequest sreq = new ShardRequest();
      sreq.params = new ModifiableSolrParams();
      sreq.purpose = ShardRequest.PURPOSE_GET_FIELDS;
      sreq.purpose |= ShardRequest.PURPOSE_GET_DEBUG;
      // expecting the same results with debugQuery=true or debug=track
      if (random().nextBoolean()) {
        rb.setDebug(true);
      } else {
        rb.setDebug(false);
        rb.setDebugTrack(true);
        // should not depend on other debug options
        rb.setDebugQuery(random().nextBoolean());
        rb.setDebugTimings(random().nextBoolean());
        rb.setDebugResults(random().nextBoolean());
      }
      component.modifyRequest(rb, null, sreq);
      // if the request has debugQuery=true or debug=track, the sreq should get debug=track always
      assertTrue(
          Arrays.asList(sreq.params.getParams(CommonParams.DEBUG)).contains(CommonParams.TRACK));
      // the purpose must be added as readable param to be included in the shard logs
      assertEquals(
          "GET_FIELDS,GET_DEBUG,SET_TERM_STATS", sreq.params.get(CommonParams.REQUEST_PURPOSE));
      // the rid must be added to be included in the shard logs
      assertEquals("123456-my_rid", sreq.params.get(CommonParams.REQUEST_ID));
      // close requests - this method obtains a searcher in order to access its StatsCache
      req.close();
    }
  }

  @Test
  public void testPrepare() throws IOException {
    DebugComponent component = new DebugComponent();
    List<SearchComponent> components = new ArrayList<>(1);
    components.add(component);
    SolrQueryRequest req;
    ResponseBuilder rb;
    for (int i = 0; i < 10; i++) {
      req = req("q", "test query", "distrib", "true");
      rb = new ResponseBuilder(req, new SolrQueryResponse(), components);
      rb.isDistrib = true;
      addRequestId(rb, ANY_RID);

      // expecting the same results with debugQuery=true or debug=track
      if (random().nextBoolean()) {
        rb.setDebug(true);
      } else {
        rb.setDebug(false);
        rb.setDebugTrack(true);
        // should not depend on other debug options
        rb.setDebugQuery(random().nextBoolean());
        rb.setDebugTimings(random().nextBoolean());
        rb.setDebugResults(random().nextBoolean());
      }
      component.prepare(rb);
      ensureTrackRecordsRid(rb, ANY_RID);
    }

    req = req("q", "test query", "distrib", "true", CommonParams.REQUEST_ID, "123");
    rb = new ResponseBuilder(req, new SolrQueryResponse(), components);
    rb.isDistrib = true;
    rb.setDebug(true);
    component.prepare(rb);
    ensureTrackRecordsRid(rb, "123");
  }

  //
  // NOTE: String representations are not meant to be exact or backward compatible.
  // For example, foo:bar^3, foo:bar^3.0 and (foo:bar)^3 are equivalent.  Use your
  // judgement when modifying these tests.
  //
  @Test
  public void testQueryToString() {

    // test that both boosts are represented in a double-boost scenario
    assertQ(
        req("debugQuery", "true", "indent", "true", "rows", "0", "q", "(foo_s:aaa^3)^4"),
        "//str[@name='parsedquery'][.='foo_s:aaa^3.0^4.0']");

    // test to see that extra parens are avoided
    assertQ(
        req("debugQuery", "true", "indent", "true", "rows", "0", "q", "+foo_s:aaa^3 -bar_s:bbb^0"),
        "//str[@name='parsedquery'][.='+foo_s:aaa^3.0 -bar_s:bbb^0.0']");

    // test that parens are added when needed
    assertQ(
        req(
            "debugQuery",
            "true",
            "indent",
            "true",
            "rows",
            "0",
            "q",
            "foo_s:aaa (bar_s:bbb baz_s:ccc)"),
        "//str[@name='parsedquery'][.='foo_s:aaa (bar_s:bbb baz_s:ccc)']");

    // test boosts on subqueries
    assertQ(
        req(
            "debugQuery",
            "true",
            "indent",
            "true",
            "rows",
            "0",
            "q",
            "foo_s:aaa^3 (bar_s:bbb baz_s:ccc)^4"),
        "//str[@name='parsedquery'][.='foo_s:aaa^3.0 (bar_s:bbb baz_s:ccc)^4.0']");

    // test constant score query boost exists
    assertQ(
        req("debugQuery", "true", "indent", "true", "rows", "0", "q", "foo_s:aaa^=3"),
        "//str[@name='parsedquery'][contains(.,'3.0')]");
  }

  @Test
  public void reciprocalRankFusion_debugQuery_shouldReturnExplainabilitySnippet() {
    String vectorToSearch = "[1.0, 2.0, 3.0, 4.0]";
    assertQ(
        req(
            CommonParams.JSON,
            "{ \"queries\":{"
                + " \"lexical\": { \"lucene\": { \"query\": \"id:(10^=2 OR 2^=1)\" }},"
                + " \"vector-based\": { \"knn\": { \"f\": \"vector\", \"topK\": 5, \"query\": \""
                + vectorToSearch
                + "\" }}"
                + "},"
                + " \"limit\": 10,"
                + " \"fields\": [id,score],"
                + " \"params\":{\"combiner\": true, \"debug\": \"query\" }}"),
        "//lst[@name='debug']/lst[@name='queriesToCombine']/lst[@name='lexical']/str[@name='querystring']='id:(10^=2 OR 2^=1)'",
        "//lst[@name='debug']/lst[@name='queriesToCombine']/lst[@name='lexical']/str[@name='queryparser']='LuceneQParser'",
        "//lst[@name='debug']/lst[@name='queriesToCombine']/lst[@name='lexical']/str[@name='parsedquery']='ConstantScore(id:10)^2.0 ConstantScore(id:2)^1.0'",
        "//lst[@name='debug']/lst[@name='queriesToCombine']/lst[@name='lexical']/str[@name='parsedquery_toString']='(ConstantScore(id:10))^2.0 (ConstantScore(id:2))^1.0'",
        "//lst[@name='debug']/lst[@name='queriesToCombine']/lst[@name='vector-based']/str[@name='querystring']='[1.0, 2.0, 3.0, 4.0]'",
        "//lst[@name='debug']/lst[@name='queriesToCombine']/lst[@name='vector-based']/str[@name='queryparser']='KnnQParser'",
        "//lst[@name='debug']/lst[@name='queriesToCombine']/lst[@name='vector-based']/str[@name='parsedquery']='KnnFloatVectorQuery(KnnFloatVectorQuery:vector[1.0,...][5])'",
        "//lst[@name='debug']/lst[@name='queriesToCombine']/lst[@name='vector-based']/str[@name='parsedquery_toString']='KnnFloatVectorQuery:vector[1.0,...][5]'");
  }

  @Test
  public void
      reciprocalRankFusion_debugResults_shouldReturnReciprocalRankFusionExplainabilitySnippet() {
    // lexical Ranking: [5,3]
    // lexical2 Ranking: [4,5,3]
    assertQ(
        req(
            CommonParams.JSON,
            "{ \"queries\":{"
                + " \"lexical\": { \"lucene\": { \"query\": \"id:(5^=1.2 OR 3^=1.1)\" }},"
                + " \"lexical2\": { \"lucene\": { \"query\": \"id:(4^=2.3 OR 5^=2.2 OR 3^=2.1)\" }}"
                + "},"
                + " \"limit\": 10,"
                + " \"fields\": [id,score],"
                + " \"params\":{\"combiner\": true, \"debug\": \"results\", \"debug.explain.structured\": true}}"),
        "//lst[@name='debug']/lst[@name='explain']/lst[@name='5']/float[@name='value']='0.032522473'",
        "//lst[@name='debug']/lst[@name='explain']/lst[@name='5']/str[@name='description']='1/(60+1) + 1/(60+2) because its ranks were: 1 for query(lexical), 2 for query(lexical2)'",
        "//lst[@name='debug']/lst[@name='explain']/lst[@name='3']/float[@name='value']='0.032002047'",
        "//lst[@name='debug']/lst[@name='explain']/lst[@name='3']/str[@name='description']='1/(60+2) + 1/(60+3) because its ranks were: 2 for query(lexical), 3 for query(lexical2)'",
        "//lst[@name='debug']/lst[@name='explain']/lst[@name='4']/float[@name='value']='0.016393442'",
        "//lst[@name='debug']/lst[@name='explain']/lst[@name='4']/str[@name='description']='1/(60+1) because its ranks were: not in the results for query(lexical), 1 for query(lexical2)'");
  }

  @Test
  public void reciprocalRankFusion_debugResults_shouldReturnOriginalQuerySnippets() {
    // lexical Ranking: [5,3]
    // lexical2 Ranking: [4,5,3]
    assertQ(
        req(
            CommonParams.JSON,
            "{ \"queries\":{"
                + " \"lexical\": { \"lucene\": { \"query\": \"id:(5^=1.2 OR 3^=1.1)\" }},"
                + " \"lexical2\": { \"lucene\": { \"query\": \"id:(4^=2.3 OR 5^=2.2 OR 3^=2.1)\" }}"
                + "},"
                + " \"limit\": 10,"
                + " \"fields\": [id,score],"
                + " \"params\":{\"combiner\": true, \"debug\": \"results\", \"debug.explain.structured\": true}}"),
        "//lst[@name='debug']/lst[@name='explain']/lst[@name='4']/arr[@name='details']/lst[1]/str[@name='description']='lexical'",
        "//lst[@name='debug']/lst[@name='explain']/lst[@name='4']/arr[@name='details']/lst[2]/str[@name='description']='lexical2'",
        "//lst[@name='debug']/lst[@name='explain']/lst[@name='4']/arr[@name='details']/lst[1]/arr[@name='details']/lst[1]/bool[@name='match']='false'",
        "//lst[@name='debug']/lst[@name='explain']/lst[@name='4']/arr[@name='details']/lst[2]/arr[@name='details']/lst[1]/bool[@name='match']='true'",
        "//lst[@name='debug']/lst[@name='explain']/lst[@name='4']/arr[@name='details']/lst[2]/arr[@name='details']/lst[1]/float[@name='value']='2.3'");
  }

  @SuppressWarnings("unchecked")
  private void ensureTrackRecordsRid(ResponseBuilder rb, String expectedRid) {
    final String rid =
        (String) ((NamedList<Object>) rb.getDebugInfo().get("track")).get(CommonParams.REQUEST_ID);
    assertEquals("Expecting " + expectedRid + " but found " + rid, expectedRid, rid);
  }

  private void addRequestId(ResponseBuilder rb, String requestId) {
    ModifiableSolrParams params = new ModifiableSolrParams(rb.req.getParams());
    params.add(CommonParams.REQUEST_ID, requestId);
    rb.req.setParams(params);
  }
}
