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
package org.apache.solr.request;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** See SOLR-2854. */
@SuppressSSL // does not yet work with ssl yet - uses raw java.net.URL API rather than HttpClient
public class TestRemoteStreaming extends SolrTestCaseJ4 {

  @ClassRule public static SolrJettyTestRule solrJettyTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void beforeTest() throws Exception {
    System.setProperty("solr.requests.streaming.remote.enabled", "true");
    System.setProperty("solr.requests.streaming.body.enabled", "true");
    System.setProperty("solr.test.sys.prop2", "test");
    Path solrHomeDirectory =
        createTempDir(LuceneTestCase.getTestClass().getSimpleName()).toRealPath();
    Path collectionDirectory = solrHomeDirectory.resolve("collection1");
    Path confDir = collectionDirectory.resolve("conf");
    Files.createDirectories(collectionDirectory.resolve("data"));
    Files.createDirectories(confDir);
    Files.copy(
        SolrTestCaseJ4.TEST_PATH().resolve("solr.xml"),
        solrHomeDirectory.resolve("solr.xml"),
        StandardCopyOption.REPLACE_EXISTING);
    Path sourceConf =
        SolrTestCaseJ4.TEST_PATH().resolve("collection1").resolve("conf").toRealPath();
    FileUtils.copyDirectory(sourceConf.toFile(), confDir.toFile());
    Files.writeString(collectionDirectory.resolve("core.properties"), "name=collection1\n");
    solrJettyTestRule.startSolr(solrHomeDirectory, new Properties(), JettyConfig.builder().build());
  }

  @AfterClass
  public static void afterTestClass() throws Exception {
    System.clearProperty("solr.test.sys.prop2");
  }

  @Before
  public void doBefore() throws IOException, SolrServerException {
    // add document and commit, and ensure it's there
    SolrClient client = solrJettyTestRule.getSolrClient("collection1");
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "1234");
    client.add(doc);
    client.commit();
    assertTrue(searchFindsIt());
  }

  @Test
  public void testMakeDeleteAllUrl() throws Exception {
    assertTrue(searchFindsIt());
    attemptHttpGet(makeDeleteAllUrl());
    assertFalse(searchFindsIt());
  }

  @Test
  public void testStreamUrl() throws Exception {
    String streamUrl = solrJettyTestRule.getBaseUrl() + "/collection1/select?q=*:*&fl=id&wt=csv";

    String getUrl =
        solrJettyTestRule.getBaseUrl()
            + "/collection1/debug/dump?wt=xml&stream.url="
            + URLEncoder.encode(streamUrl, StandardCharsets.UTF_8);
    String content = attemptHttpGet(getUrl);
    assertTrue(content.contains("1234"));
  }

  private String attemptHttpGet(String getUrl) throws IOException {
    Object obj = URI.create(getUrl).toURL().getContent();
    if (obj instanceof InputStream) {
      try (InputStream inputStream = (InputStream) obj) {
        StringWriter strWriter = new StringWriter();
        new InputStreamReader(inputStream, StandardCharsets.UTF_8).transferTo(strWriter);
        return strWriter.toString();
      }
    }
    return null;
  }

  /** Do a select query with the stream.url. Solr should fail */
  @Test
  public void testNoUrlAccess() throws Exception {
    SolrQuery query = new SolrQuery();
    query.setQuery("*:*"); // for anything
    query.add("stream.url", makeDeleteAllUrl());
    SolrException se =
        expectThrows(
            SolrException.class, () -> solrJettyTestRule.getSolrClient("collection1").query(query));
    assertSame(ErrorCode.BAD_REQUEST, ErrorCode.getErrorCode(se.code()));
  }

  /** Compose an HTTP GET url that will delete all the data. */
  private String makeDeleteAllUrl() {
    String deleteQuery = "<delete><query>*:*</query></delete>";
    return solrJettyTestRule.getBaseUrl()
        + "/collection1/update?commit=true&stream.body="
        + URLEncoder.encode(deleteQuery, StandardCharsets.UTF_8);
  }

  private boolean searchFindsIt() throws SolrServerException, IOException {
    SolrQuery query = new SolrQuery();
    query.setQuery("id:1234");
    QueryResponse rsp = solrJettyTestRule.getSolrClient("collection1").query(query);
    return rsp.getResults().getNumFound() != 0;
  }
}
