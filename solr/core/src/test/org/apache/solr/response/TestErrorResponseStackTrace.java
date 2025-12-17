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
package org.apache.solr.response;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.codec.CharEncoding;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.apache.HttpClientUtil;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.json.JsonMapResponseParser;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

@SuppressSSL
public class TestErrorResponseStackTrace extends SolrTestCaseJ4 {

  @ClassRule public static final SolrJettyTestRule solrRule = new SolrJettyTestRule();

  @BeforeClass
  public static void setupSolrHome() throws Exception {
    Path configSet = createTempDir("configSet");
    copyMinConf(configSet);
    // insert a special filterCache configuration
    Path solrConfig = configSet.resolve("conf/solrconfig.xml");
    Files.writeString(
        solrConfig,
        Files.readString(solrConfig)
            .replace(
                "</config>",
                "  <searchComponent name=\"errorComponent\" class=\"org.apache.solr.response.TestErrorResponseStackTrace$ErrorComponent\"/>\n"
                    + "  <requestHandler name=\"/withError\" class=\"solr.SearchHandler\">\n"
                    + "    <arr name=\"first-components\">\n"
                    + "      <str>errorComponent</str>\n"
                    + "    </arr>\n"
                    + "  </requestHandler>\n"
                    + "</config>"));

    solrRule.startSolr(LuceneTestCase.createTempDir());
    solrRule.newCollection().withConfigSet(configSet.toString()).create();
  }

  @Test
  public void testFullStackTrace() throws Exception {
    final String url = solrRule.getBaseUrl().toString() + "/collection1/withError?q=*:*&wt=json";
    final HttpGet get = new HttpGet(url);
    var client = HttpClientUtil.createClient(null);
    try (CloseableHttpResponse responseRaw = client.execute(get)) {

      assertEquals(500, responseRaw.getStatusLine().getStatusCode());
      NamedList<Object> response =
          new JsonMapResponseParser()
              .processResponse(responseRaw.getEntity().getContent(), CharEncoding.UTF_8);

      assertEquals(500L, response._get("error/code"));
      assertEquals("java.lang.RuntimeException", response._get("error/errorClass"));
      assertEquals("Stacktrace should be populated.", response._get("error/msg"));
      assertTrue(
          ((String) response._get("error/trace/stackTrace[0]"))
              .contains("org.apache.solr.response.TestErrorResponseStackTrace$ErrorComponent"));
      assertEquals("java.io.IOException", response._get("error/trace/causedBy/errorClass"));
      assertEquals("This is the cause", response._get("error/trace/causedBy/msg"));
      assertTrue(
          ((String) response._get("error/trace/causedBy/trace/stackTrace[0]"))
              .contains("org.apache.solr.response.TestErrorResponseStackTrace$ErrorComponent"));
    } finally {
      HttpClientUtil.close(client);
    }
  }

  @Test
  @SuppressWarnings({"unchecked"})
  public void testRemoteSolrException() {
    var client = solrRule.getSolrClient("collection1");
    QueryRequest queryRequest =
        new QueryRequest(new ModifiableSolrParams().set("q", "*:*").set("wt", "json"));
    queryRequest.setPath("/withError");
    RemoteSolrException exception =
        expectThrows(RemoteSolrException.class, () -> queryRequest.process(client, "collection1"));
    assertTrue(exception.getRemoteErrorObject() instanceof NamedList);
    var remoteError = (NamedList<Object>) exception.getRemoteErrorObject();
    assertEquals(500, remoteError._get("code"));
    assertEquals("java.lang.RuntimeException", remoteError._get("errorClass"));
    assertEquals("Stacktrace should be populated.", remoteError._get("msg"));
    assertTrue(
        ((String) remoteError._get("trace/stackTrace[0]"))
            .contains("org.apache.solr.response.TestErrorResponseStackTrace$ErrorComponent"));
    assertEquals("java.io.IOException", remoteError._get("trace/causedBy/errorClass"));
    assertEquals("This is the cause", remoteError._get("trace/causedBy/msg"));
    assertTrue(
        ((String) remoteError._get("trace/causedBy/trace/stackTrace[0]"))
            .contains("org.apache.solr.response.TestErrorResponseStackTrace$ErrorComponent"));
  }

  public static class ErrorComponent extends SearchComponent {

    @Override
    public void prepare(ResponseBuilder rb) throws IOException {
      // prepared
    }

    @Override
    public void process(ResponseBuilder rb) throws IOException {
      throw new RuntimeException(
          "Stacktrace should be populated.", new IOException("This is the cause"));
    }

    @Override
    public String getDescription() {
      return null;
    }
  }
}
