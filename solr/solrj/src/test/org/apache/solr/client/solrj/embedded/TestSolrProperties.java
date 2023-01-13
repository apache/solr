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

import static org.apache.solr.SolrTestCaseJ4.getFile;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest.ACTION;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

/** Test properties in configuration files. */
public class TestSolrProperties extends SolrTestCase {

  protected static Path SOLR_HOME;
  protected static Path CONFIG_HOME;

  protected CoreContainer cores = null;

  @Rule public EmbeddedSolrServerTestRule solrClientTestRule = new EmbeddedSolrServerTestRule();

  @Rule public TestRule testRule = new SystemPropertiesRestoreRule();

  @BeforeClass
  public static void setUpHome() throws IOException {
    CONFIG_HOME = getFile("solrj/solr/shared").toPath().toAbsolutePath();
    SOLR_HOME = createTempDir("solrHome");
    FileUtils.copyDirectory(CONFIG_HOME.toFile(), SOLR_HOME.toFile());
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    System.setProperty(
        "configSetBaseDir", CONFIG_HOME.resolve("../configsets").normalize().toString());
    System.setProperty("coreRootDirectory", "."); // relative to Solr home

    // The index is always stored within a temporary directory
    File tempDir = createTempDir().toFile();

    File dataDir = new File(tempDir, "data1");
    File dataDir2 = new File(tempDir, "data2");
    System.setProperty("dataDir1", dataDir.getAbsolutePath());
    System.setProperty("dataDir2", dataDir2.getAbsolutePath());
    System.setProperty("tempDir", tempDir.getAbsolutePath());
    SolrTestCaseJ4.newRandomConfig();

    solrClientTestRule.startSolr(SOLR_HOME);

    cores = solrClientTestRule.getSolrClient().getCoreContainer();
  }

  protected SolrClient getSolrAdmin() {
    return solrClientTestRule.getAdminClient();
  }

  @Test
  public void testProperties() throws Exception {

    UpdateRequest up = new UpdateRequest();
    up.setAction(ACTION.COMMIT, true, true);
    up.deleteByQuery("*:*");
    up.process(solrClientTestRule.getSolrClient("core0"));
    up.process(solrClientTestRule.getSolrClient("core1"));
    up.clear();

    // Add something to each core
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "AAA");
    doc.setField("core0", "yup stopfra stopfrb stopena stopenb");

    // Add to core0
    up.add(doc);
    up.process(solrClientTestRule.getSolrClient("core0"));

    SolrTestCaseJ4.ignoreException("unknown field");

    // You can't add it to core1
    expectThrows(Exception.class, () -> up.process(solrClientTestRule.getSolrClient("core1")));

    // Add to core1
    doc.setField("id", "BBB");
    doc.setField("core1", "yup stopfra stopfrb stopena stopenb");
    doc.removeField("core0");
    up.add(doc);
    up.process(solrClientTestRule.getSolrClient("core1"));

    // You can't add it to core1
    SolrTestCaseJ4.ignoreException("core0");
    expectThrows(Exception.class, () -> up.process(solrClientTestRule.getSolrClient("core0")));
    SolrTestCaseJ4.resetExceptionIgnores();

    // now Make sure AAA is in 0 and BBB in 1
    SolrQuery q = new SolrQuery();
    QueryRequest r = new QueryRequest(q);
    q.setQuery("id:AAA");
    assertEquals(1, r.process(solrClientTestRule.getSolrClient("core0")).getResults().size());
    assertEquals(0, r.process(solrClientTestRule.getSolrClient("core1")).getResults().size());

    // Now test Changing the default core
    assertEquals(
        1,
        (solrClientTestRule.getSolrClient("core0"))
            .query(new SolrQuery("id:AAA"))
            .getResults()
            .size());
    assertEquals(
        0,
        (solrClientTestRule.getSolrClient("core0"))
            .query(new SolrQuery("id:BBB"))
            .getResults()
            .size());

    assertEquals(
        0,
        (solrClientTestRule.getSolrClient("core1"))
            .query(new SolrQuery("id:AAA"))
            .getResults()
            .size());
    assertEquals(
        1,
        (solrClientTestRule.getSolrClient("core1"))
            .query(new SolrQuery("id:BBB"))
            .getResults()
            .size());

    // Now test reloading it should have a newer open time
    String name = "core0";
    SolrClient coreadmin = getSolrAdmin();
    CoreAdminResponse mcr = CoreAdminRequest.getStatus(name, coreadmin);
    long before = mcr.getStartTime(name).getTime();
    CoreAdminRequest.reloadCore(name, coreadmin);

    mcr = CoreAdminRequest.getStatus(name, coreadmin);
    long after = mcr.getStartTime(name).getTime();
    assertTrue("should have more recent time: " + after + "," + before, after > before);
  }
}
