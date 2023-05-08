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
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** SOLR-14886 : Suppress stack trace in Query response */
public class HideStackTraceTest extends SolrJettyTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
        Paths.get(solrHome.toString() + "/collection1/conf").resolve("solrconfig.xml"));
    createAndStartJetty(solrHome.toAbsolutePath().toString());
  }

  @AfterClass
  public static void after() throws Exception {
    if (solrHome != null) {
      cleanUpJettyHome(solrHome.toFile());
    }
  }

  @Test
  public void testHideStackTrace() throws IOException, SolrServerException {
    Http2SolrClient httpClient =
        new Http2SolrClient.Builder(jetty.getBaseUrl().toString() + DEFAULT_TEST_COLLECTION_NAME)
            .build();

    SolrQuery query = new SolrQuery();
    query.add(CommonParams.Q, "*:*");
    query.add("wt", "xml");

    @SuppressWarnings("serial")
    QueryRequest queryRequest =
        new QueryRequest(query) {
          @Override
          public String getPath() {
            return "/withError";
          }
        };
    NamedList<Object> response = httpClient.request(queryRequest, DEFAULT_TEST_COLLECTION_NAME);

    Assert.assertNotNull("Should have a response", response);
    log.info("received response: {}", response);

    Assert.assertNotNull("Should contain an error", response.get("error"));
    Assert.assertNull("Should not contain the trace", response.get("trace"));
  }

  public static class ErrorComponent extends SearchComponent {

    @Override
    public void prepare(ResponseBuilder rb) throws IOException {
      // prepared
    }

    @Override
    public void process(ResponseBuilder rb) throws IOException {
      rb.rsp.add("weight", Float.parseFloat("NumberFormatException with stack trace"));
    }

    @Override
    public String getDescription() {
      return null;
    }
  }
}
