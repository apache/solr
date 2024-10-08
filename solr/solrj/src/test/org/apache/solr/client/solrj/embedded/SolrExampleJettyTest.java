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

import static org.apache.solr.common.util.Utils.fromJSONString;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrExampleTests;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO? perhaps use: http://docs.codehaus.org/display/JETTY/ServletTester rather then open a real
 * connection?
 */
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class SolrExampleJettyTest extends SolrExampleTests {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeTest() throws Exception {
    createAndStartJetty(legacyExampleCollection1SolrHome());
  }

  @Test
  public void testBadSetup() {
    String url = "http" + (isSSLMode() ? "s" : "") + "://127.0.0.1/?core=xxx";
    // This test does NOT fail for Http2SolrClient
    expectThrows(Exception.class, () -> new HttpSolrClient.Builder(url).build());
  }

  @Test
  public void testArbitraryJsonIndexing() throws Exception {
    // For making raw requests...
    HttpClient httpClient = HttpClientUtil.createClient(null);
    try (SolrClient client = getSolrClient()) {
      client.deleteByQuery("*:*");
      client.commit();
      assertNumFound("*:*", 0); // make sure it got in

      // two docs, one with uniqueKey, another without it
      String json = "{\"id\":\"abc1\", \"name\": \"name1\"} {\"name\" : \"name2\"}";
      HttpPost post = new HttpPost(getRandomizedUpdateUri(getCoreUrl()));
      post.setHeader("Content-Type", "application/json");
      post.setEntity(
          new InputStreamEntity(
              new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), -1));
      HttpResponse response =
          httpClient.execute(post, HttpClientUtil.createNewHttpClientRequestContext());
      assertEquals(200, response.getStatusLine().getStatusCode());
      client.commit();
      QueryResponse rsp = getSolrClient().query(new SolrQuery("*:*"));
      assertEquals(2, rsp.getResults().getNumFound());

      SolrDocument doc = rsp.getResults().get(0);
      String src = (String) doc.getFieldValue("_src_");
      @SuppressWarnings({"rawtypes"})
      Map m = (Map) fromJSONString(src);
      assertEquals("abc1", m.get("id"));
      assertEquals("name1", m.get("name"));

      doc = rsp.getResults().get(1);
      src = (String) doc.getFieldValue("_src_");
      m = (Map) fromJSONString(src);
      assertEquals("name2", m.get("name"));
    } finally {
      HttpClientUtil.close(httpClient);
    }
  }

  private String getRandomizedUpdateUri(String baseUrl) {
    return random().nextBoolean()
        ? baseUrl.replace("/collection1", "/____v2/cores/collection1/update")
        : baseUrl + "/update/json/docs";
  }

  @Test
  public void testUtf8PerfDegradation() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();

    doc.addField("id", "1");
    doc.addField("b_is", IntStream.range(0, 30000).boxed().collect(Collectors.toList()));

    try (SolrClient client = getSolrClient()) {
      client.add(doc);
      client.commit();
      long start = System.nanoTime();
      QueryResponse rsp = client.query(new SolrQuery("*:*"));
      System.out.println("time taken : " + ((System.nanoTime() - start)) / (1000 * 1000));
      assertEquals(1, rsp.getResults().getNumFound());
    }
  }

  private void runQueries(SolrClient client, int count, boolean warmup)
      throws SolrServerException, IOException {
    long start = System.nanoTime();
    for (int i = 0; i < count; i++) {
      client.query(new SolrQuery("*:*"));
    }
    if (warmup) return;
  }
}
