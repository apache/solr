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
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

public class UBIComponentTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeTest() throws Exception {
    initCore("solrconfig-ubi-component.xml", "schema12.xml");
    assertNull(h.validateUpdate(adoc("id", "1", "subject", "aa")));
    assertNull(h.validateUpdate(adoc("id", "two", "subject", "aa")));
    assertNull(h.validateUpdate(adoc("id", "3", "subject", "aa")));
    assertU(commit());
  }

  @Test
  public void testToLogIds() throws Exception {
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
  public void testZeroResults() throws Exception {
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
  public void testDisabling() throws Exception {
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
}
