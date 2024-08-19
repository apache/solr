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

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

public class UBIComponentTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTest() throws Exception {
    initCore("solrconfig-ubi-local-component.xml", "schema12.xml");
    assertNull(h.validateUpdate(adoc("id", "1", "subject", "aa")));
    assertNull(h.validateUpdate(adoc("id", "two", "subject", "aa")));
    assertNull(h.validateUpdate(adoc("id", "3", "subject", "aa")));
    assertU(commit());
  }

  @Test
  public void testGeneratingAQueryId() {
    assertQ(
        "Make sure we generate a query id",
        req("qt", "/with_ubi", "q", "aa", "rows", "2", "ubi", "true"),
        "//lst[@name='ubi']/str[@name='query_id'][.='1234']");
  }

  @Test
  public void testZeroResultsGeneratesQueryId() {
    assertQ(
        "Make sure we generate a query id even when no results are returned",
        req("qt", "/with_ubi", "q", "abcdefgxyz", "rows", "0", "ubi", "true"),
        "//*[@numFound='0']",
        "//lst[@name='ubi']/str[@name='query_id'][.='1234']");
  }

  @Test
  public void testPassedInQueryIdIsUsed() {
    assertQ(
        "Make sure we reuse a passed in query id",
        req("qt", "/with_ubi", "q", "aa", "rows", "0", "ubi", "true", "query_id", "123abc"),
        "//lst[@name='ubi']/str[@name='query_id'][.='123abc']");
  }

  @Test
  public void testGenerateQueryId() {
    assertQ(
        "Make sure we generate a query id if one is not passed in",
        req("qt", "/with_ubi", "q", "aa", "rows", "0", "ubi", "true"),
        "//lst[@name='ubi']/str[@name='query_id'][.='1234']");
  }

  @Test
  public void testJSONQuerySyntax() throws Exception {
    assertJQ(
        req(
            "qt",
            "/with_ubi",
            "json",
            "{\n"
                + "    'query': 'aa',\n"
                + "    'fields': '*',\n"
                + "    'offset': 0,\n"
                + "    'limit': 2,\n"
                + "    'params': {\n"
                + "    'df': 'subject',\n"
                + "    'qt': '/with_ubi',\n"
                + "    'ubi': 'true'\n"
                + "   }\n"
                + "}"),
        "response/numFound==3",
        "ubi/query_id=='1234'");

    assertJQ(
        req(
            "qt",
            "/with_ubi",
            "json",
            "{\n"
                + "    'query': 'aa',\n"
                + "    'fields': '*',\n"
                + "    'offset': 0,\n"
                + "    'limit': 2,\n"
                + "    'params': {\n"
                + "        'df': 'subject',\n"
                + "        'ubi': 'true',\n"
                + "        'query_id': 'xjy-42-1rj'\n"
                + "        'user_query': 'aa'\n"
                + "        'query_attributes': {\n"
                + "            'page': 2,\n"
                + "            'filter': 'inStock:true',\n"
                + "        }\n"
                + "   }\n"
                + "}"),
        "response/numFound==3",
        "ubi/query_id=='xjy-42-1rj'");
  }

  @Test
  public void testTrackingOfUserQuery() {
    assertQ(
        "Make sure we generate a query id",
        req("qt", "/with_ubi", "q", "aa", "rows", "0", "ubi", "true", "user_query", "fresh air"),
        "//lst[@name='ubi']/str[@name='query_id'][.='1234']");
  }

  @Test
  public void testDisabling() {
    assertQ(
        "Make sure we don't generate a query_id",
        req("qt", "/with_ubi", "q", "aa", "ubi", "false"),
        "count(//lst[@name='ubi'])=0");
  }
}
