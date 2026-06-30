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
  public void testIntervalsReturnsNoResults() throws Exception {
    assertU(adoc("id", "1", "v_t", "hello world"));
    assertU(commit());

    // The parser always returns MatchNoDocsQuery for now, so numFound must be 0
    assertQ(
        "intervals qparser should return no docs",
        req("q", "{!intervals json_query=myQuery}"),
        "//result[@numFound='0']");
  }

  @Test
  public void testIntervalsWithJsonQueriesPassThrough() throws Exception {
    assertU(adoc("id", "2", "v_t", "foo bar"));
    assertU(commit());

    // json_queries is accepted as a top-level JSON DSL key and the json_query param names an entry;
    // the parser still returns MatchNoDocsQuery (not yet implemented), so numFound must be 0
    assertQ(
        "intervals qparser with json_queries should return no docs",
        req(
            "q",
            "{!intervals json_query=myQuery}",
            "json",
            "{json_queries:{myQuery:{term:{f:v_t,value:foo}}}}"),
        "//result[@numFound='0']");
  }
}
