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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** See SOLR-2854. */
@SuppressSSL // does not yet work with ssl yet - uses raw java.net.URL API rather than HttpClient
public class TestRemoteStreaming extends SolrJettyTestBase {

  private static JettySolrRunner jettySolrRunner;

  @BeforeClass
  public static void beforeTest() throws Exception {
    // this one has handleSelect=true which a test here needs
    File solrHomeDirectory = createTempDir(LuceneTestCase.getTestClass().getSimpleName()).toFile();
    setupJettyTestHome(solrHomeDirectory, "collection1");
    jettySolrRunner = createAndStartJetty(solrHomeDirectory.getAbsolutePath());
  }

  @AfterClass
  public static void afterTest() {}

  @Before
  public void doBefore() throws IOException, SolrServerException {
    // add document and commit, and ensure it's there
    SolrClient client = getSolrClient();
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
    String streamUrl =
        jettySolrRunner.getBaseUrl().toString()
            + "/"
            + DEFAULT_TEST_COLLECTION_NAME
            + "/select?q=*:*&fl=id&wt=csv";

    String getUrl =
        jettySolrRunner.getBaseUrl().toString()
            + "/"
            + DEFAULT_TEST_COLLECTION_NAME
            + "/debug/dump?wt=xml&stream.url="
            + URLEncoder.encode(streamUrl, "UTF-8");
    String content = attemptHttpGet(getUrl);
    assertTrue(content.contains("1234"));
  }

  private String attemptHttpGet(String getUrl) throws IOException {
    Object obj = new URL(getUrl).getContent();
    if (obj instanceof InputStream) {
      InputStream inputStream = (InputStream) obj;
      try {
        StringWriter strWriter = new StringWriter();
        new InputStreamReader(inputStream, StandardCharsets.UTF_8).transferTo(strWriter);
        return strWriter.toString();
      } finally {
        IOUtils.closeQuietly(inputStream);
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
    try (SolrClient solrClient = createNewSolrClient()) {
      SolrException se = expectThrows(SolrException.class, () -> solrClient.query(query));
      assertSame(ErrorCode.BAD_REQUEST, ErrorCode.getErrorCode(se.code()));
    }
  }

  /** Compose an HTTP GET url that will delete all the data. */
  private String makeDeleteAllUrl() throws UnsupportedEncodingException {
    String deleteQuery = "<delete><query>*:*</query></delete>";
    return jettySolrRunner.getBaseUrl().toString()
        + "/"
        + DEFAULT_TEST_COLLECTION_NAME
        + "/update?commit=true&stream.body="
        + URLEncoder.encode(deleteQuery, "UTF-8");
  }

  private boolean searchFindsIt() throws SolrServerException, IOException {
    SolrQuery query = new SolrQuery();
    query.setQuery("id:1234");
    QueryResponse rsp = getSolrClient().query(query);
    return rsp.getResults().getNumFound() != 0;
  }
}
