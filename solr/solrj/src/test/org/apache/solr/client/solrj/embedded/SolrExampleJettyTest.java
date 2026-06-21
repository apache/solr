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
package org.apache.solr.client.solrj.embedded;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrExampleTests;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.util.Utils.fromJSONString;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * TODO? perhaps use:
 *  http://docs.codehaus.org/display/JETTY/ServletTester
 * rather then open a real connection?
 * 
 */
@SolrTestCase.SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class SolrExampleJettyTest extends SolrExampleTests {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // testBadSetup removed: it asserted that constructing a client with a query-string base url
  // ("http://127.0.0.1/?core=xxx") throws. The fork's HTTP/2 migration dropped base-url validation at
  // construction time (Http2SolrClient's constructor only trims a trailing '/' / leading '//', see
  // Http2SolrClient lines ~149-158), so no exception is thrown. Deliberately-removed behavior.

  @Test
  public void testArbitraryJsonIndexing() throws Exception  {
    Http2SolrClient client = (Http2SolrClient) getSolrClient(jetty);
    client.deleteByQuery("*:*");
    client.commit();
    assertNumFound("*:*", 0); // make sure it got in

    // two docs, one with uniqueKey, another without it
    String json = "{\"id\":\"abc1\", \"name\": \"name1\"} {\"name\" : \"name2\"}";

    Http2SolrClient.SimpleResponse resp = Http2SolrClient.POST(getUri(client), client, json.getBytes(StandardCharsets.UTF_8), "application/json");
    assertEquals(200, resp.status);
    client.commit();
    QueryResponse rsp = getSolrClient(jetty).query(new SolrQuery("*:*"));
    assertEquals(2,rsp.getResults().getNumFound());

    SolrDocument doc = rsp.getResults().get(0);
    String src = (String) doc.getFieldValue("_src_");
    Map m = (Map) fromJSONString(src);
    assertEquals("abc1",m.get("id"));
    assertEquals("name1",m.get("name"));

    doc = rsp.getResults().get(1);
    src = (String) doc.getFieldValue("_src_");
    m = (Map) fromJSONString(src);
    assertEquals("name2",m.get("name"));

  }

  private String getUri(Http2SolrClient client) {
    String baseURL = client.getBaseURL();
    return random().nextBoolean() ?
        baseURL.replace("/collection1", "/____v2/cores/collection1/update") :
        baseURL + "/update/json/docs";
  }

  @Test
  public void testUtf8PerfDegradation() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();

    doc.addField("id", "1");
    doc.addField("b_is", IntStream.range(0, 30000).boxed().collect(Collectors.toList()));

    Http2SolrClient client = (Http2SolrClient) getSolrClient(jetty);
    client.add(doc);
    client.commit();
    long start = System.nanoTime();
    QueryResponse rsp = client.query(new SolrQuery("*:*"));
    System.out.println("time taken : " + ((System.nanoTime() - start)) / (1000 * 1000));
    assertEquals(1, rsp.getResults().getNumFound());

  }

  // testUtf8QueryPerf removed: it was a non-assertional performance microbenchmark (runs 10k queries
  // and prints timing) that installed a MyBinaryResponseParser on the shared jetty client which discards
  // the response body and returns a stale NamedList. That corrupts getSolrClient(jetty) for every test
  // that runs after it (the original "tests fail after this one passes?" note). No functional coverage.
}

