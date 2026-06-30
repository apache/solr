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

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests for {@link IntervalsQParserPlugin}. */
public class TestIntervalsQParserPlugin extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema11.xml");
  }

  @Test
  public void testIntervalsNoJsonQueryParam() throws Exception {
    assertU(adoc("id", "1", "v_t", "hello world"));
    assertU(commit());

    // Without a json_query param the parser returns MatchNoDocsQuery
    assertQ(
        "intervals qparser without json_query should return no docs",
        req("q", "{!intervals}"),
        "//result[@numFound='0']");
  }

  @Test
  public void testIntervalsMatchRuleMatchesDocument() throws Exception {
    assertU(adoc("id", "10", "v_t", "foo bar"));
    assertU(adoc("id", "11", "v_t", "baz qux"));
    assertU(commit());

    // {v_t:{match:{query:"foo"}}} produces an IntervalQuery on v_t
    assertQ(
        "intervals qparser with match rule should match documents containing the term",
        req(
            "q",
            "{!intervals json_query=myQuery}",
            "json",
            "{json_queries:{myQuery:{v_t:{match:{query:foo}}}}}"),
        "//result[@numFound='1']",
        "//doc/str[@name='id'][.='10']");
  }

  @Test
  public void testIntervalsAllOfAnyOfNamedQuery() throws Exception {
    assertU(adoc("id", "30", "title_t", "alpha beta gamma delta"));
    assertU(adoc("id", "31", "title_t", "alpha beta epsilon delta"));
    assertU(adoc("id", "32", "title_t", "alpha zeta gamma delta"));
    assertU(commit());

    assertQ(
        "intervals qparser should support field -> all_of with nested any_of from a named json_query",
        req(
            "q",
            "{!intervals json_query=second_query}",
            "json",
            "{json_queries:{"
                + "second_query:{"
                + "title_t:{"
                + "all_of:{"
                + "ordered:true,"
                + "intervals:["
                + "{match:{query:'alpha beta', max_gaps:0, ordered:true}},"
                + "{any_of:{intervals:["
                + "{match:{query:'gamma delta', max_gaps:0, ordered:true}},"
                + "{match:{query:'epsilon delta', max_gaps:0, ordered:true}}"
                + "]}}"
                + "]"
                + "}"
                + "}"
                + "}"
                + "}}"),
        "//result[@numFound='2']");
  }

  @Test
  public void testIntervalsNoMatchingRule() throws Exception {
    assertU(adoc("id", "20", "v_t", "hello world"));
    assertU(commit());

    // Match rule text not present in any document
    assertQ(
        "intervals qparser with non-matching rule should return no docs",
        req(
            "q",
            "{!intervals json_query=myQuery}",
            "json",
            "{json_queries:{myQuery:{v_t:{match:{query:zzznomatch}}}}}"),
        "//result[@numFound='0']");
  }
}
