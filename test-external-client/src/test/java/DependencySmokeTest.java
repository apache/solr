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

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrClientTestRule;
import org.junit.ClassRule;
import org.junit.Test;

/** Smoke test that solr-solrj and solr-test-framework are usable from an external project. */
public class DependencySmokeTest extends SolrTestCase {

  @ClassRule public static SolrClientTestRule solrRule = new EmbeddedSolrServerTestRule();

  @Test
  public void testSolrjAndTestFrameworkAreUsable() throws Exception {
    solrRule.startSolr();

    // We should look like any consumer outside the Solr source tree (see SOLR-18092).
    assertNull("expected no Solr source tree", ExternalPaths.SOURCE_HOME);

    // ...which also means ExternalPaths gives no configSet; the smoke-test runner supplies one.
    String configSetDir = System.getProperty("smoke.configset.dir");
    assertTrue(
        "smoke.configset.dir must point to a configSet dir; run via checkTestExternalClient.py",
        configSetDir != null && Files.isDirectory(Path.of(configSetDir)));
    solrRule.newCollection().withConfigSet(Path.of(configSetDir)).create();

    SolrClient client = solrRule.getSolrClient();
    client.add(new SolrInputDocument("id", "1"));
    client.commit();
    assertEquals(1, client.query(new SolrQuery("*:*")).getResults().getNumFound());
  }
}
