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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

public class UBIComponentTest extends SolrTestCaseJ4 {

  private static File ubiQueriesLog;

  @BeforeClass
  public static void beforeTest() throws Exception {

    System.setProperty("solr.log.dir", createTempDir("solr_logs").toString());

    initCore("solrconfig-ubi-component.xml", "schema12.xml");
    assertNull(h.validateUpdate(adoc("id", "1", "subject", "aa")));
    assertNull(h.validateUpdate(adoc("id", "two", "subject", "aa")));
    assertNull(h.validateUpdate(adoc("id", "3", "subject", "aa")));
    assertU(commit());

    ubiQueriesLog =
        new File(EnvUtils.getProperty("solr.log.dir") + "/" + UBIComponent.UBI_QUERY_JSONL_LOG);
    assertTrue(ubiQueriesLog.exists());
  }

  @Test
  public void testToLogIds() {
    SolrQueryRequest req = null;
    try {
      String handler = "/withubi";
      req = req("qt", "/withubi", "q", "aa", "rows", "2", "ubi", "true");

      assertQ(
          "Make sure we generate a query id",
          req,
          "//lst[@name='ubi']/str[@name='query_id'][.='1234']");
      // Need to test the writing out to the logs..
      // SolrQueryResponse qr = h.queryAndResponse(handler, req);
      // NamedList<Object> entries = qr.getToLog();
      // String docIds = (String) entries.get("ubi");
      // assertNotNull(docIds);
      // assertTrue(docIds.matches("\\w+,\\w+"));
    } finally {
      req.close();
    }
  }

  @Test
  public void testZeroResults() {
    // SolrQueryRequest req = null;
    // try {
    // String handler = "/withubi";
    // req = req("qt", "/withubi", "q", "aa", "rows", "0", "ubi", "true");

    assertQ(
        "Make sure we generate a query id",
        req("qt", "/withubi", "q", "aa", "rows", "0", "ubi", "true"),
        "//lst[@name='ubi']/str[@name='query_id'][.='1234']");

    // SolrQueryResponse qr = h.queryAndResponse(handler, req);
    // NamedList<Object> entries = qr.getToLog();
    // String docIds = (String) entries.get("ubi");
    // assertNull(docIds);
    // } finally {
    // req.close();
    // }
  }

  @Test
  public void testExternallyGeneratedQueryId() {
    assertQ(
        "Make sure we generate a query id",
        req("qt", "/withubi", "q", "aa", "rows", "0", "ubi", "true", "query_id", "123abc"),
        "//lst[@name='ubi']/str[@name='query_id'][.='123abc']");
  }

  @Test
  public void testJSONQuerySyntax() throws Exception {
    // need to fix this.  It just checks the number of docs,
    // but doesn't do anything about the ubi clauses...
    // doesn't appear to trigger the ubi...
    assertJQ(
        req(
            "qt",
            "/withubi",
            "json",
            "{\n"
                + "    'query': 'aa',\n"
                + "    'fields': '*',\n"
                + "    'offset': 0,\n"
                + "    'limit': 2,\n"
                + "    'params': {\n"
                + "    'df': 'subject',\n"
                + "        'qt': '/withubi',\n"
                + "        'ubi': 'true'\n"
                + "   }\n"
                + "}"),
        "response/numFound==3",
        "ubi/query_id=='1234'");

    String lastLine = readLastLineOfFile(ubiQueriesLog);

    String json = "{\"query_id\":\"1234\",\"user_query\":null,\"doc_ids\":\"1,two\"}";
    assertJSONEquals(json, lastLine);

    assertJQ(
        req(
            "qt",
            "/withubi",
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
                + "        'user_query': {\n"
                + "            'query': 'aa',\n"
                + "            'page': 2,\n"
                + "            'filter': 'inStock:true',\n"
                + "        }\n"
                + "   }\n"
                + "}"),
        "response/numFound==3",
        "ubi/query_id=='xjy-42-1rj'");

    lastLine = readLastLineOfFile(ubiQueriesLog);

    json =
        "{\"query_id\":\"xjy-42-1rj\",\"user_query\":{\"query\":\"aa\",\"page\":2,\"filter\":\"inStock:true\"},\"doc_ids\":\"1,two\"}";
    assertJSONEquals(json, lastLine);
  }

  @Test
  public void testTrackingOfUserQuery() {
    assertQ(
        "Make sure we generate a query id",
        req("qt", "/withubi", "q", "aa", "rows", "0", "ubi", "true", "user_query", "fresh air"),
        "//lst[@name='ubi']/str[@name='query_id'][.='1234']");
  }

  @Test
  public void testDisabling() {
    // SolrQueryRequest req = null;
    // try {
    // String handler = "/withubi";

    assertQ(
        "Make sure we don't generate a query_id",
        req("qt", "/withubi", "q", "aa", "ubi", "false"),
        "count(//lst[@name='ubi'])=0");

    // SolrQueryResponse qr = h.queryAndResponse(handler, req);
    // NamedList<Object> entries = qr.getToLog();
    // String ubi = (String) entries.get("ubi");
    // assertNull(ubi);
    // } finally {
    // req.close();
    // }
  }

  private static String readLastLineOfFile(File file) throws IOException {
    try (ReversedLinesFileReader reader =
        ReversedLinesFileReader.builder().setFile(file).setCharset(StandardCharsets.UTF_8).get()) {
      String line = reader.readLine();
      return line;
    }
  }
}
