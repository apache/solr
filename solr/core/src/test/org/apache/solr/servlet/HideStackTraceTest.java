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
import java.io.InputStream;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

/** SOLR-14886 : Suppress stack trace in Query response */
@SuppressSSL
public class HideStackTraceTest extends SolrJettyTestBase {

  private static Path solrHome;

  @BeforeClass
  public static void before() throws Exception {
    solrHome = createTempDir();
    setupJettyTestHome(solrHome.toFile(), "collection1");
    // overwrite solr.xml to hide stack traces
    Files.copy(
        TEST_PATH().resolve("solr-hideStackTrace.xml"),
        solrHome.resolve("solr.xml"),
        StandardCopyOption.REPLACE_EXISTING);
    Path top = Paths.get(SolrTestCaseJ4.TEST_HOME() + "/collection1/conf");
    Files.copy(
        top.resolve("solrconfig-errorComponent.xml"),
        Paths.get(solrHome.toString() + "/collection1/conf").resolve("solrconfig.xml"),
        StandardCopyOption.REPLACE_EXISTING);
    createAndStartJetty(solrHome.toAbsolutePath().toString());
  }

  @AfterClass
  public static void after() throws Exception {
    if (client != null) client.close();
    client = null;
    afterSolrJettyTestBase();
    if (solrHome != null) {
      cleanUpJettyHome(solrHome.toFile());
    }
  }

  @Test
  public void testHideStackTrace() throws Exception {
    final String url = jetty.getBaseUrl().toString() + "/collection1/withError?q=*:*&wt=xml";
    final HttpGet get = new HttpGet(url);

    try (final CloseableHttpClient client = buildHttpClient();
        final CloseableHttpResponse response = client.execute(get)) {

      Assert.assertNotNull("Should have a response", response);

      final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      final DocumentBuilder builder = factory.newDocumentBuilder();
      Document doc;
      try (final InputStream in = response.getEntity().getContent()) {
        doc = builder.parse(in);
      }

      final XPath xpath = XPathFactory.newInstance().newXPath();
      final String error = xpath.evaluate("/response/lst[name='error']", doc);
      Assert.assertNotNull("Should contain an error", error);
      final String message = xpath.evaluate("/response/lst/str[name='msg']", doc);
      Assert.assertNotNull("Should contain an error message", message);
      final String trace = xpath.evaluate("/response/lst/str[name='trace']", doc);
      Assert.assertTrue("Should not contain the trace", trace.isEmpty());
    }
  }

  private CloseableHttpClient buildHttpClient() {
    final Builder requestConfigBuilder = RequestConfig.custom();
    requestConfigBuilder
        .setConnectTimeout(15000)
        .setSocketTimeout(30000)
        .setCookieSpec(CookieSpecs.STANDARD);
    final RequestConfig requestConfig = requestConfigBuilder.build();

    final PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
    connManager.setMaxTotal(5);
    connManager.setDefaultMaxPerRoute(5);

    final HttpClientBuilder clientBuilder = HttpClientBuilder.create();
    clientBuilder.setDefaultRequestConfig(requestConfig);
    clientBuilder.setConnectionManager(connManager);
    return clientBuilder.build();
  }

  public static class ErrorComponent extends SearchComponent {

    @Override
    public void prepare(ResponseBuilder rb) throws IOException {
      // prepared
    }

    @Override
    public void process(ResponseBuilder rb) throws IOException {
      throw new RuntimeException("Stack trace should not be populated.");
    }

    @Override
    public String getDescription() {
      return null;
    }
  }
}
