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
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** SOLR-14886 : Suppress stack trace in Query response */
@SuppressSSL
public class HideStackTraceTest extends SolrTestCaseJ4 {

  @ClassRule public static final SolrJettyTestRule solrRule = new SolrJettyTestRule();

  @BeforeClass
  public static void setupSolrHome() throws Exception {

    System.setProperty("solr.hideStackTrace", "true");

    Path configSet = createTempDir("configSet");
    copyMinConf(configSet.toFile());
    // insert a special filterCache configuration
    Path solrConfig = configSet.resolve("conf/solrconfig.xml");
    Files.writeString(
        solrConfig,
        Files.readString(solrConfig)
            .replace(
                "</config>",
                "  <searchComponent name=\"errorComponent\" class=\"org.apache.solr.servlet.HideStackTraceTest$ErrorComponent\"/>\n"
                    + "  <requestHandler name=\"/withError\" class=\"solr.SearchHandler\">\n"
                    + "    <arr name=\"first-components\">\n"
                    + "      <str>errorComponent</str>\n"
                    + "    </arr>\n"
                    + "  </requestHandler>\n"
                    + "</config>"));

    solrRule.startSolr(LuceneTestCase.createTempDir());
    solrRule.newCollection().withConfigSet(configSet.toString()).create();
  }

  @AfterClass
  public static void cleanup() throws Exception {
    System.clearProperty("solr.hideStackTrace");
  }

  @Test
  public void testHideStackTrace() throws Exception {
    // Normal stack:
    // {
    // "responseHeader":{
    // "status":500,
    // "QTime":10
    // },
    // "error":{
    // "msg":"Stack trace should not be populated.",
    // "trace":"java.lang.RuntimeException: Stack trace should not be populated.\n\tat
    // org.apache.solr.servlet.HideStackTraceTest$ErrorComponent.process(HideStackTraceTest.java:130)\n\tat
    // org.apache.solr.handler.component.SearchHandler.handleRequestBody(SearchHandler.java:442)\n\tat
    // org.apache.solr.handler.RequestHandlerBase.handleRequest(RequestHandlerBase.java:224)\n\tat
    // org.apache.solr.core.SolrCore.execute(SolrCore.java:2890)\n\tat
    // org.apache.solr.servlet.HttpSolrCall.executeCoreRequest(HttpSolrCall.java:870)\n\tat
    // org.apache.solr.servlet.HttpSolrCall.call(HttpSolrCall.java:559)\n\tat
    // org.apache.solr.servlet.SolrDispatchFilter.dispatch(SolrDispatchFilter.java:254)\n\tat
    // org.apache.solr.servlet.SolrDispatchFilter.lambda$0(SolrDispatchFilter.java:215)\n\tat
    // org.apache.solr.servlet.ServletUtils.traceHttpRequestExecution2(ServletUtils.java:241)\n\tat
    // org.apache.solr.servlet.ServletUtils.rateLimitRequest(ServletUtils.java:211)\n\tat
    // org.apache.solr.servlet.SolrDispatchFilter.doFilter(SolrDispatchFilter.java:209)\n\tat
    // org.apache.solr.servlet.SolrDispatchFilter.doFilter(SolrDispatchFilter.java:192)\n\tat
    // org.eclipse.jetty.servlet.FilterHolder.doFilter(FilterHolder.java:202)\n\tat
    // org.eclipse.jetty.servlet.ServletHandler$Chain.doFilter(ServletHandler.java:1635)\n\tat
    // org.apache.solr.embedded.JettySolrRunner$DebugFilter.doFilter(JettySolrRunner.java:187)\n\tat
    // org.eclipse.jetty.servlet.FilterHolder.doFilter(FilterHolder.java:202)\n\tat
    // org.eclipse.jetty.servlet.ServletHandler$Chain.doFilter(ServletHandler.java:1635)\n\tat
    // org.eclipse.jetty.servlet.ServletHandler.doHandle(ServletHandler.java:527)\n\tat
    // org.eclipse.jetty.server.handler.ScopedHandler.nextHandle(ScopedHandler.java:221)\n\tat
    // org.eclipse.jetty.server.session.SessionHandler.doHandle(SessionHandler.java:1570)\n\tat
    // org.eclipse.jetty.server.handler.ScopedHandler.nextHandle(ScopedHandler.java:221)\n\tat
    // org.eclipse.jetty.server.handler.ContextHandler.doHandle(ContextHandler.java:1384)\n\tat
    // org.eclipse.jetty.server.handler.ScopedHandler.nextScope(ScopedHandler.java:176)\n\tat
    // org.eclipse.jetty.servlet.ServletHandler.doScope(ServletHandler.java:484)\n\tat
    // org.eclipse.jetty.server.session.SessionHandler.doScope(SessionHandler.java:1543)\n\tat
    // org.eclipse.jetty.server.handler.ScopedHandler.nextScope(ScopedHandler.java:174)\n\tat
    // org.eclipse.jetty.server.handler.ContextHandler.doScope(ContextHandler.java:1306)\n\tat
    // org.eclipse.jetty.server.handler.ScopedHandler.handle(ScopedHandler.java:129)\n\tat
    // org.eclipse.jetty.server.handler.HandlerWrapper.handle(HandlerWrapper.java:122)\n\tat
    // org.eclipse.jetty.rewrite.handler.RewriteHandler.handle(RewriteHandler.java:301)\n\tat
    // org.eclipse.jetty.server.handler.HandlerWrapper.handle(HandlerWrapper.java:122)\n\tat
    // org.eclipse.jetty.server.handler.gzip.GzipHandler.handle(GzipHandler.java:822)\n\tat
    // org.eclipse.jetty.server.handler.HandlerWrapper.handle(HandlerWrapper.java:122)\n\tat
    // org.eclipse.jetty.server.Server.handle(Server.java:563)\n\tat
    // org.eclipse.jetty.server.HttpChannel.lambda$handle$0(HttpChannel.java:505)\n\tat
    // org.eclipse.jetty.server.HttpChannel.dispatch(HttpChannel.java:762)\n\tat
    // org.eclipse.jetty.server.HttpChannel.handle(HttpChannel.java:497)\n\tat
    // org.eclipse.jetty.server.HttpConnection.onFillable(HttpConnection.java:282)\n\tat
    // org.eclipse.jetty.io.AbstractConnection$ReadCallback.succeeded(AbstractConnection.java:314)\n\tat
    // org.eclipse.jetty.io.FillInterest.fillable(FillInterest.java:100)\n\tat
    // org.eclipse.jetty.io.SelectableChannelEndPoint$1.run(SelectableChannelEndPoint.java:53)\n\tat
    // org.eclipse.jetty.util.thread.QueuedThreadPool.runJob(QueuedThreadPool.java:969)\n\tat
    // org.eclipse.jetty.util.thread.QueuedThreadPool$Runner.doRunJob(QueuedThreadPool.java:1194)\n\tat
    // org.eclipse.jetty.util.thread.QueuedThreadPool$Runner.run(QueuedThreadPool.java:1149)\n\tat
    // java.base/java.lang.Thread.run(Thread.java:833)\n",
    // "code":500
    // }
    // }

    // Supressed stack:
    // {
    // "responseHeader":{
    // "status":500,
    // "QTime":9
    // },
    // "error":{
    // "msg":"Stack trace should not be populated.",
    // "code":500
    // }
    // }

    final String url = solrRule.getBaseUrl().toString() + "/collection1/withError?q=*:*&wt=json";
    final HttpGet get = new HttpGet(url);
    var client = HttpClientUtil.createClient(null);
    try (CloseableHttpResponse response = client.execute(get)) {

      assertEquals(500, response.getStatusLine().getStatusCode());
      String responseJson = EntityUtils.toString(response.getEntity());
      assertFalse(responseJson.contains("\"trace\""));
      assertFalse(
          responseJson.contains("org.apache.solr.servlet.HideStackTraceTest$ErrorComponent"));
    } finally {
      HttpClientUtil.close(client);
    }
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
