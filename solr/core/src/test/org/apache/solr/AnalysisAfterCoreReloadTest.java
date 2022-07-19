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
package org.apache.solr;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest.ACTION;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class AnalysisAfterCoreReloadTest extends SolrTestCaseJ4 {

  static final String collection = "collection1";

  @BeforeClass
  public static void beforeClass() throws Exception {
    String tmpSolrHome = createTempDir().toFile().getAbsolutePath();
    FileUtils.copyDirectory(new File(TEST_HOME()), new File(tmpSolrHome).getAbsoluteFile());
    initCore("solrconfig.xml", "schema.xml", new File(tmpSolrHome).getAbsolutePath());
  }

  @AfterClass
  public static void AfterClass() {}

  public void testStopwordsAfterCoreReload() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "42");
    doc.setField("teststop", "terma stopworda stopwordb stopwordc");

    // default stopwords - stopworda and stopwordb

    UpdateRequest up = new UpdateRequest();
    up.setAction(ACTION.COMMIT, true, true);
    up.add(doc);
    up.process(getSolrCore());

    SolrQuery q = new SolrQuery();
    QueryRequest r = new QueryRequest(q);
    q.setQuery("teststop:terma");
    assertEquals(1, r.process(getSolrCore()).getResults().size());

    q = new SolrQuery();
    r = new QueryRequest(q);
    q.setQuery("teststop:stopworda");
    assertEquals(0, r.process(getSolrCore()).getResults().size());

    q = new SolrQuery();
    r = new QueryRequest(q);
    q.setQuery("teststop:stopwordb");
    assertEquals(0, r.process(getSolrCore()).getResults().size());

    q = new SolrQuery();
    r = new QueryRequest(q);
    q.setQuery("teststop:stopwordc");
    assertEquals(1, r.process(getSolrCore()).getResults().size());

    // overwrite stopwords file with stopword list ["stopwordc"] and reload the core
    overwriteStopwords("stopwordc\n");
    h.getCoreContainer().reload(collection);

    up.process(getSolrCore());

    q = new SolrQuery();
    r = new QueryRequest(q);
    q.setQuery("teststop:terma");
    assertEquals(1, r.process(getSolrCore()).getResults().size());

    q = new SolrQuery();
    r = new QueryRequest(q);
    q.setQuery("teststop:stopworda");
    // stopworda is no longer a stopword
    assertEquals(1, r.process(getSolrCore()).getResults().size());

    q = new SolrQuery();
    r = new QueryRequest(q);
    q.setQuery("teststop:stopwordb");
    // stopwordb is no longer a stopword
    assertEquals(1, r.process(getSolrCore()).getResults().size());

    q = new SolrQuery();
    r = new QueryRequest(q);
    q.setQuery("teststop:stopwordc");
    // stopwordc should be a stopword
    assertEquals(0, r.process(getSolrCore()).getResults().size());
  }

  private void overwriteStopwords(String stopwords) throws IOException {
    try (SolrCore core = h.getCoreContainer().getCore(collection)) {
      Path configPath = core.getResourceLoader().getConfigPath();
      Files.move(configPath.resolve("stopwords.txt"), configPath.resolve("stopwords.txt.bak"));
      Files.write(configPath.resolve("stopwords.txt"), stopwords.getBytes(StandardCharsets.UTF_8));
    }
  }

  @Override
  public void tearDown() throws Exception {
    Path configPath;
    try (SolrCore core = h.getCoreContainer().getCore(collection)) {
      configPath = core.getResourceLoader().getConfigPath();
    }
    super.tearDown();
    Path backupFile = configPath.resolve("stopwords.txt.bak");
    if (Files.exists(backupFile)) {
      Files.move(backupFile, configPath.resolve("stopwords.txt"), REPLACE_EXISTING);
    }
  }

  protected SolrClient getSolrCore() {
    return new EmbeddedSolrServer(h.getCoreContainer(), collection);
  }
}
