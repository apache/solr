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
package org.apache.solr.update;

import java.io.File;
import java.nio.file.Path;
import org.apache.lucene.tests.mockfile.FilterPath;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.apache.solr.util.SolrClientTestRule;
import org.junit.ClassRule;

public class CustomTLogDirTest extends SolrTestCaseJ4 {

  @ClassRule
  public static final SolrClientTestRule solrClientTestRule = new EmbeddedSolrServerTestRule();

  public void test() throws Exception {
    Path solrHome = LuceneTestCase.createTempDir();
    solrClientTestRule.startSolr(solrHome);

    SolrClient client = solrClientTestRule.getSolrClient();

    Path coreRootDir = ((EmbeddedSolrServer) client).getCoreContainer().getCoreRootDirectory();

    Path instanceDir = FilterPath.unwrap(coreRootDir.resolve(DEFAULT_TEST_COLLECTION_NAME));

    Path ulogDir;
    Path resolvedTlogDir;
    String type;
    switch (random().nextInt(5)) {
      case 0:
        type = "external";
        ulogDir = LuceneTestCase.createTempDir();
        // absolute path spec that falls outside of the instance and data dirs for the
        // associated core, is assumed to already by namespaced by purpose (tlog). We
        // expect it to be further namespaced by core name.
        resolvedTlogDir = ulogDir.resolve(DEFAULT_TEST_COLLECTION_NAME);
        break;
      case 1:
        type = "relative";
        ulogDir = Path.of("relativeUlogDir");
        // relative dir path spec is taken to be relative to instance dir, so we expect it
        // to be namespaced by purpose within the core.
        resolvedTlogDir = instanceDir.resolve(ulogDir).resolve("tlog");
        break;
      case 2:
        type = "illegal_relative";
        ulogDir = Path.of("../");
        // relative dir path specs should not be able to "escape" the core-scoped instance dir;
        // check that this config is unsuccessful
        resolvedTlogDir = null;
        break;
      case 3:
        type = "absolute_subdir";
        ulogDir = instanceDir.resolve("absoluteUlogDir");
        // an absolute dir path spec, if it is contained within the instance dir, is taken
        // to already be namespaced to the core. We expect the tlog dir to be namespaced by
        // purpose (tlog) within core, just as is the case with relative path spec.
        resolvedTlogDir = ulogDir.resolve("tlog");
        break;
      case 4:
        if (random().nextBoolean()) {
          type = "default";
          ulogDir = null;
        } else {
          type = "explicit_default";
          ulogDir = instanceDir.resolve("data");
        }
        // whether the normal default ulog dir spec `[instanceDir]/data` is configured
        // implicitly or explicitly, we expect to find the tlog in the same place:
        resolvedTlogDir = instanceDir.resolve("data/tlog");
        break;
      default:
        throw new IllegalStateException();
    }

    Path configSet = LuceneTestCase.createTempDir();
    System.setProperty("solr.test.sys.prop2", "proptwo");
    if (ulogDir != null) {
      System.setProperty("solr.ulog.dir", ulogDir.toString()); // picked up from `solrconfig.xml`
    }
    SolrTestCaseJ4.copyMinConf(configSet.toFile(), null, "solrconfig.xml");

    SolrClientTestRule.NewCollectionBuilder builder =
        solrClientTestRule.newCollection().withConfigSet(configSet.toString());
    // resolvedTlogDir = instanceDir.resolve("data/tlog"); // legacy impl _always_ resulted in this
    if (resolvedTlogDir == null) {
      expectThrows(Exception.class, type, builder::create);
      return;
    } else {
      builder.create();
    }

    addDocs(client); // add some docs to populate tlog

    File[] list =
        resolvedTlogDir.toFile().listFiles((f) -> f.isFile() && f.getName().startsWith("tlog."));

    assertNotNull(type, list);
    assertEquals(type, 1, list.length);
  }

  private static void addDocs(SolrClient client) throws Exception {
    client.add(sdoc("id", "1"));
    client.add(sdoc("id", "2"));
    client.add(sdoc("id", "3"));
    client.commit();
  }
}
