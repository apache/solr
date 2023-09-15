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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestReRankQParserPlugin extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-collapseqparser.xml", "schema11.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.setUp();
    clearIndex();
    assertU(commit());
  }

  @Test
  public void testReRankQParserPluginConstants() {
    assertEquals(ReRankQParserPlugin.NAME, "rerank");

    assertEquals(ReRankQParserPlugin.RERANK_QUERY, "reRankQuery");

    assertEquals(ReRankQParserPlugin.RERANK_DOCS, "reRankDocs");
    assertEquals(ReRankQParserPlugin.RERANK_DOCS_DEFAULT, 200);

    assertEquals(ReRankQParserPlugin.RERANK_WEIGHT, "reRankWeight");
    assertEquals(ReRankQParserPlugin.RERANK_WEIGHT_DEFAULT, 2.0d, 0.0d);

    assertEquals(ReRankQParserPlugin.RERANK_OPERATOR, "reRankOperator");
  }

  @Test
  public void testReRankQueries() {

    assertU(delQ("*:*"));
    assertU(commit());

    String[] doc = {
      "id", "1", "term_s", "YYYY", "group_s", "group1", "test_ti", "5", "test_tl", "10", "test_tf",
      "2000"
    };
    assertU(adoc(doc));
    assertU(commit());
    String[] doc1 = {
      "id", "2", "term_s", "YYYY", "group_s", "group1", "test_ti", "50", "test_tl", "100",
      "test_tf", "200"
    };
    assertU(adoc(doc1));

    String[] doc2 = {
      "id", "3", "term_s", "YYYY", "test_ti", "5000", "test_tl", "100", "test_tf", "200"
    };
    assertU(adoc(doc2));
    assertU(commit());
    String[] doc3 = {
      "id", "4", "term_s", "YYYY", "test_ti", "500", "test_tl", "1000", "test_tf", "2000"
    };
    assertU(adoc(doc3));

    String[] doc4 = {
      "id", "5", "term_s", "YYYY", "group_s", "group2", "test_ti", "4", "test_tl", "10", "test_tf",
      "2000"
    };
    assertU(adoc(doc4));
    assertU(commit());
    String[] doc5 = {
      "id", "6", "term_s", "YYYY", "group_s", "group2", "test_ti", "10", "test_tl", "100",
      "test_tf", "200"
    };
    assertU(adoc(doc5));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=200}");
    params.add("q", "term_s:YYYY");
    params.add("rqq", "{!edismax bf=$bff}*:*");
    params.add("bff", "field(test_ti)");
    params.add("start", "0");
    params.add("rows", "6");
    params.add("df", "text");
    assertQ(
        req(params),
        "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[3]/str[@name='id'][.='2']",
        "//result/doc[4]/str[@name='id'][.='6']",
        "//result/doc[5]/str[@name='id'][.='1']",
        "//result/doc[6]/str[@name='id'][.='5']");

    // check each of the reRankOperators return the expected score for item 3
    for (Map.Entry<String, String> scoreByOp :
        Map.of("add", "10002.1", "multiply", "1000.2", "replace", "10002.0").entrySet()) {
      params = new ModifiableSolrParams();
      String operation = scoreByOp.getKey();
      if (random().nextBoolean()) {
        operation = operation.toUpperCase(Locale.ROOT);
      }
      final Function<String, String> rerankQueryByOp =
          op ->
              "{!"
                  + ReRankQParserPlugin.NAME
                  + " "
                  + ReRankQParserPlugin.RERANK_QUERY
                  + "=$rqq "
                  + ReRankQParserPlugin.RERANK_OPERATOR
                  + "="
                  + op
                  + " "
                  + ReRankQParserPlugin.RERANK_DOCS
                  + "=200}";
      params.add("rq", rerankQueryByOp.apply(operation));
      params.add("q", "term_s:YYYY^=0.1"); // force score=0.1
      params.add("rqq", "{!edismax bf=$bff}*:*"); // returns 1 + $bff
      params.add("bff", "field(test_ti)"); // test_ti=5000 for item 3
      params.add("start", "0");
      params.add("rows", "6");
      params.add("df", "text");
      params.add("fl", "id,score");
      assertQ(
          req(params),
          "*[count(//doc)=6]",
          "//result/doc[1]/str[@name='id'][.='3']",
          "//result/doc[1]/float[@name='score'][.='" + scoreByOp.getValue() + "']");
      final String badOp =
          random().nextBoolean()
              ? operation + operation
              : operation.substring(0, operation.length() - 1);
      params.set("rq", rerankQueryByOp.apply(badOp));
      assertQEx("Wrong reRankOperation:" + badOp, req(params), SolrException.ErrorCode.BAD_REQUEST);
    }

    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=6}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "{!edismax bq=$bqq2}*:*");
    params.add("bqq2", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");
    params.add("df", "text");

    assertQ(
        req(params),
        "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='6']",
        "//result/doc[3]/str[@name='id'][.='5']",
        "//result/doc[4]/str[@name='id'][.='4']",
        "//result/doc[5]/str[@name='id'][.='3']",
        "//result/doc[6]/str[@name='id'][.='1']");

    // Test with sort by score.
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=6}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "{!edismax bq=$bqq2}*:*");
    params.add("bqq2", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");
    params.add("sort", "score desc");
    params.add("df", "text");
    assertQ(
        req(params),
        "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='6']",
        "//result/doc[3]/str[@name='id'][.='5']",
        "//result/doc[4]/str[@name='id'][.='4']",
        "//result/doc[5]/str[@name='id'][.='3']",
        "//result/doc[6]/str[@name='id'][.='1']");

    // Test with compound sort.
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=6}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "{!edismax bq=$bqq2}*:*");
    params.add("bqq2", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");
    params.add("sort", "score desc,test_ti asc");
    params.add("df", "text");

    assertQ(
        req(params),
        "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='6']",
        "//result/doc[3]/str[@name='id'][.='5']",
        "//result/doc[4]/str[@name='id'][.='4']",
        "//result/doc[5]/str[@name='id'][.='3']",
        "//result/doc[6]/str[@name='id'][.='1']");

    // Test with elevation

    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=6 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=50}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "{!edismax bq=$bqq2}*:*");
    params.add("bqq2", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");
    params.add("qt", "/elevate");
    params.add("elevateIds", "1");
    assertQ(
        req(params),
        "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']",
        "//result/doc[3]/str[@name='id'][.='6']",
        "//result/doc[4]/str[@name='id'][.='5']",
        "//result/doc[5]/str[@name='id'][.='4']",
        "//result/doc[6]/str[@name='id'][.='3']");

    // Test TermQuery rqq
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=6 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");
    params.add("df", "text");

    assertQ(
        req(params),
        "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='6']",
        "//result/doc[3]/str[@name='id'][.='5']",
        "//result/doc[4]/str[@name='id'][.='4']",
        "//result/doc[5]/str[@name='id'][.='3']",
        "//result/doc[6]/str[@name='id'][.='1']");

    // Test Elevation
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=6 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");
    params.add("qt", "/elevate");
    params.add("elevateIds", "1,4");

    assertQ(
        req(params),
        "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='1']", // Elevated
        "//result/doc[2]/str[@name='id'][.='4']", // Elevated
        "//result/doc[3]/str[@name='id'][.='2']", // Boosted during rerank.
        "//result/doc[4]/str[@name='id'][.='6']",
        "//result/doc[5]/str[@name='id'][.='5']",
        "//result/doc[6]/str[@name='id'][.='3']");

    // Test Elevation swapped
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=6 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");
    params.add("qt", "/elevate");
    params.add("elevateIds", "4,1");

    assertQ(
        req(params),
        "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='4']", // Elevated
        "//result/doc[2]/str[@name='id'][.='1']", // Elevated
        "//result/doc[3]/str[@name='id'][.='2']", // Boosted during rerank.
        "//result/doc[4]/str[@name='id'][.='6']",
        "//result/doc[5]/str[@name='id'][.='5']",
        "//result/doc[6]/str[@name='id'][.='3']");

    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=4 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");
    params.add("qt", "/elevate");
    params.add("elevateIds", "4,1");

    assertQ(
        req(params),
        "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='4']", // Elevated
        "//result/doc[2]/str[@name='id'][.='1']", // Elevated
        "//result/doc[3]/str[@name='id'][.='6']",
        "//result/doc[4]/str[@name='id'][.='5']",
        "//result/doc[5]/str[@name='id'][.='3']",
        "//result/doc[6]/str[@name='id'][.='2']" // Not in reRankDocs
        );

    // Test Elevation with start beyond the rerank docs
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=3 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "4");
    params.add("rows", "10");
    params.add("qt", "/elevate");
    params.add("elevateIds", "4,1");

    assertQ(
        req(params),
        "*[count(//doc)=2]",
        "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[2]/str[@name='id'][.='2']" // Was not in reRankDocs
        );

    // Test Elevation with zero results
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=3 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=2}");
    params.add("q", "{!edismax bq=$bqq1}nada");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "4");
    params.add("rows", "10");
    params.add("qt", "/elevate");
    params.add("elevateIds", "4,1");

    assertQ(req(params), "*[count(//doc)=0]");

    // Pass in reRankDocs lower than the length being collected.
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=1 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");

    assertQ(
        req(params),
        "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='6']",
        "//result/doc[2]/str[@name='id'][.='5']",
        "//result/doc[3]/str[@name='id'][.='4']",
        "//result/doc[4]/str[@name='id'][.='3']",
        "//result/doc[5]/str[@name='id'][.='2']",
        "//result/doc[6]/str[@name='id'][.='1']");

    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=0 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");

    assertQ(
        req(params),
        "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='6']",
        "//result/doc[2]/str[@name='id'][.='5']",
        "//result/doc[3]/str[@name='id'][.='4']",
        "//result/doc[4]/str[@name='id'][.='3']",
        "//result/doc[5]/str[@name='id'][.='2']",
        "//result/doc[6]/str[@name='id'][.='1']");

    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=2 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:4^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "10");

    assertQ(
        req(params),
        "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='5']",
        "//result/doc[2]/str[@name='id'][.='6']",
        "//result/doc[3]/str[@name='id'][.='4']",
        "//result/doc[4]/str[@name='id'][.='3']",
        "//result/doc[5]/str[@name='id'][.='2']",
        "//result/doc[6]/str[@name='id'][.='1']");

    // Test reRankWeight of 0, reranking will have no effect.
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=6 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=0}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add("bqq1", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "5");

    assertQ(
        req(params),
        "*[count(//doc)=5]",
        "//result/doc[1]/str[@name='id'][.='6']",
        "//result/doc[2]/str[@name='id'][.='5']",
        "//result/doc[3]/str[@name='id'][.='4']",
        "//result/doc[4]/str[@name='id'][.='3']",
        "//result/doc[5]/str[@name='id'][.='2']");

    MetricsMap metrics =
        (MetricsMap)
            ((SolrMetricManager.GaugeWrapper)
                    h.getCore()
                        .getCoreMetricManager()
                        .getRegistry()
                        .getMetrics()
                        .get("CACHE.searcher.queryResultCache"))
                .getGauge();
    Map<String, Object> stats = metrics.getValue();

    long inserts = (Long) stats.get("inserts");

    assertTrue(inserts > 0);

    // Test range query
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=6}");
    params.add("q", "test_ti:[0 TO 2000]");
    params.add("rqq", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "6");

    assertQ(
        req(params),
        "*[count(//doc)=5]",
        "//result/doc[1]/str[@name='id'][.='6']",
        "//result/doc[2]/str[@name='id'][.='5']",
        "//result/doc[3]/str[@name='id'][.='4']",
        "//result/doc[4]/str[@name='id'][.='2']",
        "//result/doc[5]/str[@name='id'][.='1']");

    stats = metrics.getValue();

    long inserts1 = (Long) stats.get("inserts");

    // Last query was added to the cache
    assertTrue(inserts1 > inserts);

    // Run same query and see if it was cached. This tests the query result cache hit with rewritten
    // queries
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=6}");
    params.add("q", "test_ti:[0 TO 2000]");
    params.add("rqq", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "6");

    assertQ(
        req(params),
        "*[count(//doc)=5]",
        "//result/doc[1]/str[@name='id'][.='6']",
        "//result/doc[2]/str[@name='id'][.='5']",
        "//result/doc[3]/str[@name='id'][.='4']",
        "//result/doc[4]/str[@name='id'][.='2']",
        "//result/doc[5]/str[@name='id'][.='1']");

    stats = metrics.getValue();
    long inserts2 = (Long) stats.get("inserts");
    // Last query was NOT added to the cache
    assertEquals(inserts1, inserts2);

    // Test range query embedded in larger query
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=6}");
    // function query for predictable scores (relative to id) independent of similarity
    params.add("q", "{!func}id_i");
    // constant score for each clause (unique per doc) for predictable scores independent of
    // similarity
    // NOTE: biased in favor of doc id == 2
    params.add("rqq", "id:1^=10 id:2^=40 id:3^=30 id:4^=40 id:5^=50 id:6^=60");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "6");

    assertQ(
        req(params),
        "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='6']",
        "//result/doc[2]/str[@name='id'][.='5']",
        "//result/doc[3]/str[@name='id'][.='4']",
        "//result/doc[4]/str[@name='id'][.='2']", // reranked out of orig order
        "//result/doc[5]/str[@name='id'][.='3']",
        "//result/doc[6]/str[@name='id'][.='1']");

    // Test with start beyond reRankDocs
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=3 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=2}");
    params.add("q", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60");
    params.add("rqq", "id:1^1000");
    params.add("fl", "id,score");
    params.add("start", "4");
    params.add("rows", "5");

    assertQ(
        req(params),
        "*[count(//doc)=2]",
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='1']");

    // Test ReRankDocs > docs returned

    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=6 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=2}");
    params.add("q", "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50");
    params.add("rqq", "id:1^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "1");

    assertQ(req(params), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");

    // Test with zero results
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=3 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=2}");
    params.add("q", "term_s:NNNN");
    params.add("rqq", "id:1^1000");
    params.add("fl", "id,score");
    params.add("start", "4");
    params.add("rows", "5");

    assertQ(req(params), "*[count(//doc)=0]");
  }

  @Test
  public void testOverRank() {

    assertU(delQ("*:*"));
    assertU(commit());

    // Test the scenario that where we rank more documents than we return.

    String[] doc = {
      "id", "1", "term_s", "YYYY", "group_s", "group1", "test_ti", "5", "test_tl", "10", "test_tf",
      "2000"
    };
    assertU(adoc(doc));
    String[] doc1 = {
      "id", "2", "term_s", "YYYY", "group_s", "group1", "test_ti", "50", "test_tl", "100",
      "test_tf", "200"
    };
    assertU(adoc(doc1));

    String[] doc2 = {
      "id", "3", "term_s", "YYYY", "test_ti", "5000", "test_tl", "100", "test_tf", "200"
    };
    assertU(adoc(doc2));
    String[] doc3 = {
      "id", "4", "term_s", "YYYY", "test_ti", "500", "test_tl", "1000", "test_tf", "2000"
    };
    assertU(adoc(doc3));

    String[] doc4 = {
      "id", "5", "term_s", "YYYY", "group_s", "group2", "test_ti", "4", "test_tl", "10", "test_tf",
      "2000"
    };
    assertU(adoc(doc4));

    String[] doc5 = {
      "id", "6", "term_s", "YYYY", "group_s", "group2", "test_ti", "10", "test_tl", "100",
      "test_tf", "200"
    };
    assertU(adoc(doc5));

    String[] doc6 = {
      "id", "7", "term_s", "YYYY", "group_s", "group1", "test_ti", "5", "test_tl", "10", "test_tf",
      "2000"
    };
    assertU(adoc(doc6));

    String[] doc7 = {
      "id", "8", "term_s", "YYYY", "group_s", "group1", "test_ti", "50", "test_tl", "100",
      "test_tf", "200"
    };
    assertU(adoc(doc7));

    String[] doc8 = {
      "id", "9", "term_s", "YYYY", "test_ti", "5000", "test_tl", "100", "test_tf", "200"
    };
    assertU(adoc(doc8));
    String[] doc9 = {
      "id", "10", "term_s", "YYYY", "test_ti", "500", "test_tl", "1000", "test_tf", "2000"
    };
    assertU(adoc(doc9));

    String[] doc10 = {
      "id", "11", "term_s", "YYYY", "group_s", "group2", "test_ti", "4", "test_tl", "10", "test_tf",
      "2000"
    };
    assertU(adoc(doc10));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=11 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add(
        "bqq1",
        "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60 id:7^70 id:8^80 id:9^90 id:10^100 id:11^110");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "2");
    params.add("df", "text");

    assertQ(
        req(params),
        "*[count(//doc)=2]",
        "//result/doc[1]/str[@name='id'][.='8']",
        "//result/doc[2]/str[@name='id'][.='2']");

    // Test Elevation
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=11 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=2}");
    params.add("q", "{!edismax bq=$bqq1}*:*");
    params.add(
        "bqq1",
        "id:1^10 id:2^20 id:3^30 id:4^40 id:5^50 id:6^60 id:7^70 id:8^80 id:9^90 id:10^100 id:11^110");
    params.add("rqq", "test_ti:50^1000");
    params.add("fl", "id,score");
    params.add("start", "0");
    params.add("rows", "3");
    params.add("qt", "/elevate");
    params.add("elevateIds", "1,4");

    assertQ(
        req(params),
        "*[count(//doc)=3]",
        "//result/doc[1]/str[@name='id'][.='1']", // Elevated
        "//result/doc[2]/str[@name='id'][.='4']", // Elevated
        "//result/doc[3]/str[@name='id'][.='8']"); // Boosted during rerank.
  }

  @Test
  public void testRerankQueryParsingShouldFailWithoutMandatoryReRankQueryParameter() {
    assertU(delQ("*:*"));
    assertU(commit());

    String[] doc = {
      "id", "1", "term_s", "YYYY", "group_s", "group1", "test_ti", "5", "test_tl", "10", "test_tf",
      "2000"
    };
    assertU(adoc(doc));
    assertU(commit());
    String[] doc1 = {
      "id", "2", "term_s", "YYYY", "group_s", "group1", "test_ti", "50", "test_tl", "100",
      "test_tf", "200"
    };
    assertU(adoc(doc1));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();

    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=200}");
    params.add("q", "term_s:YYYY");
    params.add("start", "0");
    params.add("rows", "2");

    ignoreException("reRankQuery parameter is mandatory");
    SolrException se =
        expectThrows(
            SolrException.class,
            "A syntax error should be thrown when "
                + ReRankQParserPlugin.RERANK_QUERY
                + " parameter is not specified",
            () -> h.query(req(params)));
    assertEquals(se.code(), SolrException.ErrorCode.BAD_REQUEST.code);
    unIgnoreException("reRankQuery parameter is mandatory");
  }

  @Test
  public void testReRankQueriesWithDefType() {

    assertU(delQ("*:*"));
    assertU(commit());

    final String[] doc1 = {"id", "1"};
    assertU(adoc(doc1));
    assertU(commit());
    final String[] doc2 = {"id", "2"};
    assertU(adoc(doc2));
    assertU(commit());

    final String preferredDocId;
    final String lessPreferredDocId;
    if (random().nextBoolean()) {
      preferredDocId = "1";
      lessPreferredDocId = "2";
    } else {
      preferredDocId = "2";
      lessPreferredDocId = "1";
    }

    for (final String defType :
        new String[] {null, LuceneQParserPlugin.NAME, ExtendedDismaxQParserPlugin.NAME}) {
      final ModifiableSolrParams params = new ModifiableSolrParams();
      params.add(
          "rq",
          "{!"
              + ReRankQParserPlugin.NAME
              + " "
              + ReRankQParserPlugin.RERANK_QUERY
              + "=id:"
              + preferredDocId
              + "}");
      params.add("q", "*:*");
      if (defType != null) {
        params.add(QueryParsing.DEFTYPE, defType);
      }
      assertQ(
          req(params),
          "*[count(//doc)=2]",
          "//result/doc[1]/str[@name='id'][.='" + preferredDocId + "']",
          "//result/doc[2]/str[@name='id'][.='" + lessPreferredDocId + "']");
    }
  }

  @Test
  public void testMinExactCount() {

    assertU(delQ("*:*"));
    assertU(commit());

    int numDocs = 200;

    for (int i = 0; i < numDocs; i++) {
      assertU(
          adoc(
              "id", String.valueOf(i),
              "id_p_i", String.valueOf(i),
              "field_t",
                  IntStream.range(0, numDocs)
                      .mapToObj(Integer::toString)
                      .collect(Collectors.joining(" "))));
    }
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "field_t:0");
    params.add("start", "0");
    params.add("rows", "10");
    params.add("fl", "id,score");
    params.add("sort", "score desc, id_p_i asc");
    assertQ(
        req(params),
        "*[count(//doc)=10]",
        "//result[@numFound='" + numDocs + "']",
        "//result[@numFoundExact='true']",
        "//result/doc[1]/str[@name='id'][.='0']",
        "//result/doc[2]/str[@name='id'][.='1']",
        "//result/doc[3]/str[@name='id'][.='2']",
        "//result/doc[4]/str[@name='id'][.='3']",
        "//result/doc[5]/str[@name='id'][.='4']",
        "//result/doc[6]/str[@name='id'][.='5']",
        "//result/doc[7]/str[@name='id'][.='6']",
        "//result/doc[8]/str[@name='id'][.='7']",
        "//result/doc[9]/str[@name='id'][.='8']",
        "//result/doc[10]/str[@name='id'][.='9']");

    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rrq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=20}");
    params.add("rrq", "id:10");
    assertQ(
        req(params),
        "*[count(//doc)=10]",
        "//result[@numFound='" + numDocs + "']",
        "//result[@numFoundExact='true']",
        "//result/doc[1]/str[@name='id'][.='10']",
        "//result/doc[2]/str[@name='id'][.='0']",
        "//result/doc[3]/str[@name='id'][.='1']",
        "//result/doc[4]/str[@name='id'][.='2']",
        "//result/doc[5]/str[@name='id'][.='3']",
        "//result/doc[6]/str[@name='id'][.='4']",
        "//result/doc[7]/str[@name='id'][.='5']",
        "//result/doc[8]/str[@name='id'][.='6']",
        "//result/doc[9]/str[@name='id'][.='7']",
        "//result/doc[10]/str[@name='id'][.='8']");

    params.add(CommonParams.MIN_EXACT_COUNT, "2");
    assertQ(
        req(params),
        "*[count(//doc)=10]",
        "//result[@numFound<='" + numDocs + "']",
        "//result[@numFoundExact='false']",
        "//result/doc[1]/str[@name='id'][.='10']",
        "//result/doc[2]/str[@name='id'][.='0']",
        "//result/doc[3]/str[@name='id'][.='1']",
        "//result/doc[4]/str[@name='id'][.='2']",
        "//result/doc[5]/str[@name='id'][.='3']",
        "//result/doc[6]/str[@name='id'][.='4']",
        "//result/doc[7]/str[@name='id'][.='5']",
        "//result/doc[8]/str[@name='id'][.='6']",
        "//result/doc[9]/str[@name='id'][.='7']",
        "//result/doc[10]/str[@name='id'][.='8']");
  }

  @Test
  public void testReRankScaler() throws Exception {

    ReRankScaler reRankScaler = new ReRankScaler("0-1", "5-100", 1, ReRankOperator.ADD, null, true);
    assertTrue(reRankScaler.scaleReRankScores());
    assertTrue(reRankScaler.scaleMainScores());
    assertEquals(reRankScaler.getMainQueryMin(), 0);
    assertEquals(reRankScaler.getMainQueryMax(), 1);
    assertEquals(reRankScaler.getReRankQueryMin(), 5);
    assertEquals(reRankScaler.getReRankQueryMax(), 100);

    Map<Integer, Float> scores = new HashMap<>();
    scores.put(1, 180.25f);
    scores.put(2, 90.125f);
    scores.put(3, (180.25f + 90.125f) / 2); // halfway
    ReRankScaler.MinMaxExplain minMaxExplain = ReRankScaler.getMinMaxExplain(0, 1, scores);
    Map<Integer, Float> scaled = ReRankScaler.minMaxScaleScores(scores, minMaxExplain);
    assertEquals(scaled.size(), 3);
    assertTrue(scaled.containsKey(1));
    assertTrue(scaled.containsKey(2));
    assertTrue(scaled.containsKey(3));
    float scaled1 = scaled.get(1);
    float scaled2 = scaled.get(2);
    float scaled3 = scaled.get(3);
    assertEquals(scaled1, 1.0f, 0);
    assertEquals(scaled2, 0.0f, 0);
    // scaled3 should be halfway between scaled1 and scaled2
    assertEquals((scaled1 + scaled2) / 2, scaled3, 0);
    minMaxExplain = ReRankScaler.getMinMaxExplain(50, 100, scores);
    scaled = ReRankScaler.minMaxScaleScores(scores, minMaxExplain);
    scaled1 = scaled.get(1);
    scaled2 = scaled.get(2);
    scaled3 = scaled.get(3);
    assertEquals(scaled1, 100.0f, 0);
    assertEquals(scaled2, 50.0f, 0);
    // scaled3 should be halfway between scaled1 and scaled2
    assertEquals((scaled1 + scaled2) / 2, scaled3, 0);

    scores.put(1, 10f);
    scores.put(2, 10f);
    scores.put(3, 10f);
    minMaxExplain = ReRankScaler.getMinMaxExplain(0, 1, scores);

    scaled = ReRankScaler.minMaxScaleScores(scores, minMaxExplain);
    scaled1 = scaled.get(1);
    scaled2 = scaled.get(2);
    scaled3 = scaled.get(3);
    assertEquals(scaled1, .5f, 0);
    assertEquals(scaled2, .5f, 0);
    assertEquals(scaled3, .5f, 0);
  }

  @Test
  public void testReRankScaleQueries() throws Exception {

    assertU(delQ("*:*"));
    assertU(commit());

    String[] doc = {
      "id", "1", "term_t", "YYYY", "group_s", "group1", "test_ti", "5", "test_tl", "10", "test_tf",
      "2000"
    };
    assertU(adoc(doc));
    assertU(commit());
    String[] doc1 = {
      "id",
      "2",
      "term_t",
      "YYYY YYYY",
      "group_s",
      "group1",
      "test_ti",
      "50",
      "test_tl",
      "100",
      "test_tf",
      "200"
    };
    assertU(adoc(doc1));

    String[] doc2 = {
      "id", "3", "term_t", "YYYY YYYY YYYY", "test_ti", "5000", "test_tl", "100", "test_tf", "200"
    };
    assertU(adoc(doc2));
    assertU(commit());
    String[] doc3 = {
      "id",
      "4",
      "term_t",
      "YYYY YYYY YYYY YYYY",
      "test_ti",
      "500",
      "test_tl",
      "1000",
      "test_tf",
      "2000"
    };
    assertU(adoc(doc3));

    String[] doc4 = {
      "id",
      "5",
      "term_t",
      "YYYY YYYY YYYY YYYY YYYY",
      "group_s",
      "group2",
      "test_ti",
      "4",
      "test_tl",
      "10",
      "test_tf",
      "2000"
    };
    assertU(adoc(doc4));
    assertU(commit());
    String[] doc5 = {
      "id",
      "6",
      "term_t",
      "YYYY YYYY YYYY YYYY YYYY YYYY",
      "group_s",
      "group2",
      "test_ti",
      "10",
      "test_tl",
      "100",
      "test_tf",
      "200"
    };
    assertU(adoc(doc5));
    assertU(commit());

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_MAIN_SCALE
            + "=10-20 "
            + ReRankQParserPlugin.RERANK_SCALE
            + "=10-20 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=1 "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=200}");
    params.add("q", "term_t:YYYY");
    params.add("fl", "id,score");
    params.add("rqq", "{!edismax bf=$bff}*:*");
    params.add("bff", "field(test_ti)");
    params.add("start", "0");
    params.add("rows", "6");
    params.add("df", "text");
    params.add("debugQuery", "true");
    assertQ(
        req(params),
        "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[1]/float[@name='score'][.='37.70526']",
        "//result/doc[2]/str[@name='id'][.='6']",
        "//result/doc[2]/float[@name='score'][.='30.012009']",
        "//result/doc[3]/str[@name='id'][.='4']",
        "//result/doc[3]/float[@name='score'][.='29.82389']",
        "//result/doc[4]/str[@name='id'][.='5']",
        "//result/doc[4]/float[@name='score'][.='29.527113']",
        "//result/doc[5]/str[@name='id'][.='2']",
        "//result/doc[5]/float[@name='score'][.='25.665672']",
        "//result/doc[6]/str[@name='id'][.='1']",
        "//result/doc[6]/float[@name='score'][.='20.002003']");

    // Test with fewer rows then matching docs.
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_MAIN_SCALE
            + "=10-20 "
            + ReRankQParserPlugin.RERANK_SCALE
            + "=10-20 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=1 "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=200}");
    params.add("q", "term_t:YYYY");
    params.add("fl", "id,score");
    params.add("rqq", "{!edismax bf=$bff}*:*");
    params.add("bff", "field(test_ti)");
    params.add("start", "0");
    params.add("rows", "4");
    params.add("df", "text");
    params.add("debugQuery", "true");
    assertQ(
        req(params),
        "*[count(//doc)=4]",
        "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[1]/float[@name='score'][.='37.70526']",
        "//result/doc[2]/str[@name='id'][.='6']",
        "//result/doc[2]/float[@name='score'][.='30.012009']",
        "//result/doc[3]/str[@name='id'][.='4']",
        "//result/doc[3]/float[@name='score'][.='29.82389']",
        "//result/doc[4]/str[@name='id'][.='5']",
        "//result/doc[4]/float[@name='score'][.='29.527113']");

    // Test no-rerank hits.
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_MAIN_SCALE
            + "=10-20 "
            + ReRankQParserPlugin.RERANK_SCALE
            + "=10-20 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=1 "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=200}");
    params.add("q", "term_t:YYYY");
    params.add("fl", "id,score");
    params.add("rqq", "{!edismax bf=$bff}BBBBBBBB"); // No hit re-rank.
    params.add("bff", "field(test_ti)");
    params.add("start", "0");
    params.add("rows", "6");
    params.add("df", "text");
    params.add("debugQuery", "true");

    assertQ(
        req(params),
        "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='6']",
        "//result/doc[1]/float[@name='score'][.='20.0']",
        "//result/doc[2]/str[@name='id'][.='5']",
        "//result/doc[2]/float[@name='score'][.='19.527113']",
        "//result/doc[3]/str[@name='id'][.='4']",
        "//result/doc[3]/float[@name='score'][.='18.831097']",
        "//result/doc[4]/str[@name='id'][.='3']",
        "//result/doc[4]/float[@name='score'][.='17.705261']",
        "//result/doc[5]/str[@name='id'][.='2']",
        "//result/doc[5]/float[@name='score'][.='15.5736']",
        "//result/doc[6]/str[@name='id'][.='1']",
        "//result/doc[6]/float[@name='score'][.='10.0']");

    // Test reRank only top 4
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_MAIN_SCALE
            + "=10-20 "
            + ReRankQParserPlugin.RERANK_SCALE
            + "=10-20 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=1 "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=4}");
    params.add("q", "term_t:YYYY");
    params.add("fl", "id,score");
    params.add("rqq", "{!edismax bf=$bff}*:*");
    params.add("bff", "field(test_ti)");
    params.add("start", "0");
    params.add("rows", "6");
    params.add("df", "text");
    assertQ(
        req(params),
        "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[1]/float[@name='score'][.='37.70526']",
        "//result/doc[2]/str[@name='id'][.='6']",
        "//result/doc[2]/float[@name='score'][.='30.012009']",
        "//result/doc[3]/str[@name='id'][.='4']",
        "//result/doc[3]/float[@name='score'][.='29.82389']",
        "//result/doc[4]/str[@name='id'][.='5']",
        "//result/doc[4]/float[@name='score'][.='29.527113']",
        "//result/doc[5]/str[@name='id'][.='2']",
        "//result/doc[5]/float[@name='score'][.='15.5736']",
        "//result/doc[6]/str[@name='id'][.='1']",
        "//result/doc[6]/float[@name='score'][.='10.0']");

    // Test reRank more than found
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_MAIN_SCALE
            + "=10-20 "
            + ReRankQParserPlugin.RERANK_SCALE
            + "=10-20 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=1 "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=4}");
    params.add("q", "term_t:YYYY");
    params.add("fq", "id:(4 OR 5)");
    params.add("fl", "id,score");
    params.add("rqq", "{!edismax bf=$bff}*:*");
    params.add("bff", "field(test_ti)");
    params.add("start", "0");
    params.add("rows", "6");
    params.add("df", "text");
    assertQ(
        req(params),
        "*[count(//doc)=2]",
        "//result/doc[1]/str[@name='id'][.='4']",
        "//result/doc[1]/float[@name='score'][.='30.0']",
        "//result/doc[2]/str[@name='id'][.='5']",
        "//result/doc[2]/float[@name='score'][.='30.0']");

    // Test reRank more than found
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_MAIN_SCALE
            + "=10-20 "
            + ReRankQParserPlugin.RERANK_SCALE
            + "=10-20 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=1 "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=4}");
    params.add("q", "term_t:YYYY");
    params.add("fq", "id:(4 OR 5)");
    params.add("fl", "id,score");
    params.add("rqq", "{!edismax bf=$bff}*:*");
    params.add("bff", "field(test_ti)");
    params.add("start", "0");
    params.add("rows", "6");
    params.add("df", "text");
    params.add("debugQuery", "true");
    assertQ(
        req(params),
        "*[count(//doc)=2]",
        "//result/doc[1]/str[@name='id'][.='4']",
        "//result/doc[1]/float[@name='score'][.='30.0']",
        "//result/doc[2]/str[@name='id'][.='5']",
        "//result/doc[2]/float[@name='score'][.='30.0']");

    String explainResponse = JQ(req(params));
    assertTrue(explainResponse.contains("30.0 = combined scaled first and second pass score"));

    assertTrue(explainResponse.contains("10.0 = first pass score scaled between: 10-20"));
    assertTrue(explainResponse.contains("20.0 = second pass score scaled between: 10-20"));

    assertTrue(explainResponse.contains("20.0 = first pass score scaled between: 10-20"));

    assertTrue(explainResponse.contains("10.0 = second pass score scaled between: 10-20"));

    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_MAIN_SCALE
            + "=10-20 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=1 "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=4}");
    params.add("q", "term_t:YYYY");
    params.add("fl", "id,score");
    params.add("rqq", "{!edismax bf=$bff}*:*");
    params.add("bff", "field(test_ti)");
    params.add("start", "0");
    params.add("rows", "6");
    params.add("df", "text");
    params.add("debugQuery", "true");
    assertQ(
        req(params),
        "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[1]/float[@name='score'][.='5018.705']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[2]/float[@name='score'][.='519.8311']",
        "//result/doc[3]/str[@name='id'][.='6']",
        "//result/doc[3]/float[@name='score'][.='31.0']",
        "//result/doc[4]/str[@name='id'][.='5']",
        "//result/doc[4]/float[@name='score'][.='24.527113']",
        "//result/doc[5]/str[@name='id'][.='2']",
        "//result/doc[5]/float[@name='score'][.='15.5736']",
        "//result/doc[6]/str[@name='id'][.='1']",
        "//result/doc[6]/float[@name='score'][.='10.0']");

    explainResponse = JQ(req(params));
    assertTrue(
        explainResponse.contains(
            "5018.705 = combined scaled first and unscaled second pass score"));
    assertTrue(
        explainResponse.contains(
            "519.8311 = combined scaled first and unscaled second pass score"));
    assertTrue(
        explainResponse.contains("31.0 = combined scaled first and unscaled second pass score "));
    assertTrue(
        explainResponse.contains(
            "24.527113 = combined scaled first and unscaled second pass score"));

    assertTrue(explainResponse.contains("15.5736 = scaled main query score between: 10-20"));

    assertTrue(explainResponse.contains("10.0 = scaled main query score between: 10-20"));

    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_MAIN_SCALE
            + "=10-20 "
            + ReRankQParserPlugin.RERANK_WEIGHT
            + "=1 "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=4}");
    params.add("q", "term_t:YYYY");
    params.add("fl", "id,score");
    params.add("rqq", "{!edismax bf=$bff}id:(3 OR 4 OR 6 OR 5)");
    params.add("bff", "field(test_ti)");
    params.add("start", "0");
    params.add("rows", "6");
    params.add("df", "text");
    params.add("debug", "true");
    assertQ(
        req(params),
        "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[1]/float[@name='score'][.='5018.4053']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[2]/float[@name='score'][.='519.5313']",
        "//result/doc[3]/str[@name='id'][.='6']",
        "//result/doc[3]/float[@name='score'][.='30.700203']",
        "//result/doc[4]/str[@name='id'][.='5']",
        "//result/doc[4]/float[@name='score'][.='24.227316']",
        "//result/doc[5]/str[@name='id'][.='2']",
        "//result/doc[5]/float[@name='score'][.='15.5736']",
        "//result/doc[6]/str[@name='id'][.='1']",
        "//result/doc[6]/float[@name='score'][.='10.0']");

    assertTrue(
        explainResponse.contains(
            "5018.705 = combined scaled first and unscaled second pass score"));
    assertTrue(
        explainResponse.contains(
            "519.8311 = combined scaled first and unscaled second pass score"));
    assertTrue(
        explainResponse.contains("31.0 = combined scaled first and unscaled second pass score "));
    assertTrue(
        explainResponse.contains(
            "24.527113 = combined scaled first and unscaled second pass score"));

    assertTrue(explainResponse.contains("15.5736 = scaled main query score between: 10-20"));

    assertTrue(explainResponse.contains("10.0 = scaled main query score between: 10-20"));

    // Use default reRankWeight of 2
    params = new ModifiableSolrParams();
    params.add(
        "rq",
        "{!"
            + ReRankQParserPlugin.NAME
            + " "
            + ReRankQParserPlugin.RERANK_MAIN_SCALE
            + "=10-20 "
            + ReRankQParserPlugin.RERANK_QUERY
            + "=$rqq "
            + ReRankQParserPlugin.RERANK_DOCS
            + "=4}");
    params.add("q", "term_t:YYYY");
    params.add("fl", "id,score");
    params.add("rqq", "{!edismax bf=$bff}id:(3 OR 4 OR 6 OR 5)");
    params.add("bff", "field(test_ti)");
    params.add("start", "0");
    params.add("rows", "6");
    params.add("df", "text");
    params.add("debugQuery", "true");
    assertQ(
        req(params),
        "*[count(//doc)=6]",
        "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[1]/float[@name='score'][.='10019.105']",
        "//result/doc[2]/str[@name='id'][.='4']",
        "//result/doc[2]/float[@name='score'][.='1020.2315']",
        "//result/doc[3]/str[@name='id'][.='6']",
        "//result/doc[3]/float[@name='score'][.='41.400406']",
        "//result/doc[4]/str[@name='id'][.='5']",
        "//result/doc[4]/float[@name='score'][.='28.927517']",
        "//result/doc[5]/str[@name='id'][.='2']",
        "//result/doc[5]/float[@name='score'][.='15.5736']",
        "//result/doc[6]/str[@name='id'][.='1']",
        "//result/doc[6]/float[@name='score'][.='10.0']");

    explainResponse = JQ(req(params));
    assertTrue(
        explainResponse.contains(
            "10019.105 = combined scaled first and unscaled second pass score"));
    assertTrue(
        explainResponse.contains(
            "1020.2315 = combined scaled first and unscaled second pass score"));
    assertTrue(
        explainResponse.contains(
            "41.400406 = combined scaled first and unscaled second pass score"));
    assertTrue(
        explainResponse.contains(
            "28.927517 = combined scaled first and unscaled second pass score"));

    assertTrue(explainResponse.contains("15.5736 = scaled main query score between: 10-20"));

    assertTrue(explainResponse.contains("10.0 = scaled main query score between: 10-20"));
  }
}
