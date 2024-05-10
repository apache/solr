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
package org.apache.solr.core;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.search.FooQParserPlugin;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.ValueSourceParser;
import org.junit.BeforeClass;

/**
 * This class started life as a test for SOLR-749 to prove that value source plugins were properly
 * initialized, but it has since evolved to also help prove that ValueSource's are not asked to
 * compute values for documents unnecessarily.
 *
 * @see CountUsageValueSourceParser
 * @see <a href="https://issues.apache.org/jira/browse/SOLR-749">SOLR-749</a>
 */
public class SOLR749Test extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-SOLR-749.xml", "schema.xml");
  }

  public void testConstruction() {
    SolrCore core = h.getCore();
    assertNotNull("core is null and it shouldn't be", core);
    QParserPlugin parserPlugin = core.getQueryPlugin(QParserPlugin.DEFAULT_QTYPE);
    assertNotNull("parserPlugin is null and it shouldn't be", parserPlugin);
    assertTrue(
        "parserPlugin is not an instanceof " + FooQParserPlugin.class,
        parserPlugin instanceof FooQParserPlugin);

    ValueSourceParser vsp = core.getValueSourceParser("boost");
    assertNotNull("vsp is null and it shouldn't be", vsp);
    assertTrue(
        "vsp is not an instanceof " + DummyValueSourceParser.class,
        vsp instanceof DummyValueSourceParser);
  }

  public void testHowManyDocsHaveBoostFunctionComputed() {
    for (int i = 0; i < 100; i++) {
      assertU(adoc("id", "" + i));
    }
    assertU(commit());

    // NOTE: we can't rely on the default lucene syntax because "FooQParser" is registered as
    // "lucene"
    assertQ(req("q", "{!notfoo}*:*"), "//result[@numFound=100]");
    assertQ(req("q", "{!notfoo}id_i1:[* TO 49]"), "//result[@numFound=50]");
    try {
      assertQ(
          "query wrapped in boost func should only eval func for query matches",
          req(
              "q", "{!boost b=$boostFunc defType=notfoo}id_i1:[* TO 49]",
              "boostFunc", "countUsage('boost_func',3.4)"),
          "//result[@numFound=50]");
      assertEquals(50, CountUsageValueSourceParser.getAndClearCount("boost_func"));

      assertQ(
          "func query that is filtered should be evaled only for filtered docs",
          req(
              "q", "{!func}product(id_i1,countUsage('func_q',4.5))",
              "fq", "{!notfoo}id_i1:[30 TO 59]"),
          "//result[@numFound=30]");
      assertEquals(30, CountUsageValueSourceParser.getAndClearCount("func_q"));

      assertQ(
          "func query that wraps a query which is also used as a should be evaled only for filtered docs",
          req(
              "q", "{!func}product(query($qq),countUsage('func_q_wrapping_fq',4.5))",
              "qq", "{!notfoo}id_i1:[20 TO 39]",
              "fq", "{!query v=$qq}"),
          "//result[@numFound=20]");
      assertEquals(20, CountUsageValueSourceParser.getAndClearCount("func_q_wrapping_fq"));

      assertQ(
          "frange in complex boolean query w/ other mandatory clauses to check skipping",
          req(
              "q",
              "{!notfoo}(+id_i1:[20 TO 39] -id:25 +{!frange l=4.5 u=4.5 v='countUsage(frange_in_bq,4.5)'})"),
          "//result[@numFound=19]");

      // don't assume specific clause evaluation ordering.
      // ideally this is 19, but could be as high as 20 depending on whether frange's
      // scorer has next() called on it before other clauses skipTo
      int count = CountUsageValueSourceParser.getAndClearCount("frange_in_bq");
      assertTrue("frange_in_bq: " + count, (19 <= count && count <= 20));

      assertQ(
          "func in complex boolean query w/ constant scoring mandatory clauses",
          req(
              "q",
              "{!notfoo}(+id_i1:[20 TO 29]^0 +{!frange l=4.5 u=4.5 v='countUsage(func_in_bq,4.5)'})"),
          "//result[@numFound=10]");

      // don't assume specific clause evaluation ordering.
      // ideally this is 10, but could be as high as 11 depending on whether func's
      // scorer has next() called on it before other clauses skipTo
      count = CountUsageValueSourceParser.getAndClearCount("func_in_bq");
      assertTrue("func_in_bq: " + count, (10 <= count && count <= 11));

      // non-cached frange queries should default to post-filtering
      // (ie: only be computed on candidates of other q/fq restrictions)
      // regardless of how few/many docs match the frange
      assertQ(
          "query matching 1 doc w/ implicitly post-filtered frange matching all docs",
          req(
              // match all...
              "q", "{!notfoo cache=false}*:*",
              // ...restrict to 1 match. post filter will happily match all docs, but should only be
              // asked about 1...
              "fq", "{!frange cache=false l=30 u=30}abs(id_i1)",
              "fq", "{!frange cache=false l=4.5 u=4.5 v='countUsage(postfilt_match_all,4.5)'})"),
          "//result[@numFound=1]");
      assertEquals(1, CountUsageValueSourceParser.getAndClearCount("postfilt_match_all"));
      //
      assertQ(
          "query matching all docs w/ implicitly post-filtered frange matching no docs",
          req(
              // match some...
              "q", "{!notfoo cache=false}id_i1:[20 TO 39]",
              // ...regular conjunction filter rules out all post filter will happily match all
              // docs, but should never be asked...
              "fq", "{!frange cache=false cost=0 l=50}abs(id_i1)",
              "fq", "{!frange cache=false l=4.5 u=4.5 v='countUsage(postfilt_match_all,4.5)'})"),
          "//result[@numFound=0]");
      assertEquals(0, CountUsageValueSourceParser.getAndClearCount("postfilt_match_all"));

      // Tests that TwoPhaseIterator is employed optimally
      // note: map(countUsage(lowCost,0),0,0,id_i1) == return "id_i1" value & keep track of access
      assertQ(
          "query matching 20 -> 10 -> 5 docs; two non-cached queries",
          req(
              // match 20. the below IDs have alternating even/odd pairings to test possible
              // sequencing of evaluation
              "q", "{!notfoo cache=false}id_i1:[20 TO 39]",
              // match 10 (subset of above)
              "fq", "{!notfoo cache=false}id_i1:(20 21 25 26 28 29 31 32 36 37)",
              // eliminate #20
              "fq",
                  "{!frange cache=false cost=5 l=21 u=99 v='map(countUsage(lowCost,0),0,0,id_i1)'})",
              // match 5 -- (the odd ones since l=1 thus don't match 0)
              "fq",
                  "{!frange cache=false cost=10 l=1 v='mod(map(countUsage(lastFilter,0),0,0,id_i1),2)'}"),
          "//result[@numFound=5]");
      assertEquals(10, CountUsageValueSourceParser.getAndClearCount("lowCost"));
      assertEquals(9, CountUsageValueSourceParser.getAndClearCount("lastFilter"));
    } finally {
      CountUsageValueSourceParser.clearCounters();
    }
  }
}
