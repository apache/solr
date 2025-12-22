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
package org.apache.solr.servlet;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Properties;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.apache.HttpClientUtil;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class ResponseHeaderTest extends SolrTestCaseJ4 {

  @ClassRule public static SolrJettyTestRule solrJettyTestRule = new SolrJettyTestRule();

  private static Path solrHomeDirectory;

  @BeforeClass
  public static void beforeTest() throws Exception {
    solrHomeDirectory = createTempDir();
    Path collectionDirectory = solrHomeDirectory.resolve("collection1");

    Files.copy(
        SolrTestCaseJ4.TEST_PATH().resolve("solr.xml"),
        solrHomeDirectory.resolve("solr.xml"),
        StandardCopyOption.REPLACE_EXISTING);

    // Create minimal config with custom solrconfig for headers testing
    SolrTestCaseJ4.copyMinConf(collectionDirectory, "name=collection1\n", "solrconfig-headers.xml");

    solrJettyTestRule.startSolr(solrHomeDirectory, new Properties(), JettyConfig.builder().build());
  }

  @Test
  public void testHttpResponse() throws IOException {
    URI uri = URI.create(solrJettyTestRule.getBaseUrl() + "/collection1/withHeaders?q=*:*");
    HttpGet httpGet = new HttpGet(uri);
    try (CloseableHttpClient httpClient = HttpClientUtil.createClient(null)) {
      HttpResponse response = httpClient.execute(httpGet);
      Header[] headers = response.getAllHeaders();
      boolean containsWarningHeader = false;
      for (Header header : headers) {
        if ("Warning".equals(header.getName())) {
          containsWarningHeader = true;
          assertEquals("This is a test warning", header.getValue());
          break;
        }
      }
      assertTrue("Expected header not found", containsWarningHeader);
      HttpClientUtil.close(httpClient);
    }
  }

  public static class ComponentThatAddsHeader extends SearchComponent {

    @Override
    public void prepare(ResponseBuilder rb) {
      rb.rsp.addHttpHeader("Warning", "This is a test warning");
    }

    @Override
    public void process(ResponseBuilder rb) {}

    @Override
    public String getDescription() {
      return null;
    }
  }
}
