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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.tests.mockfile.FilterPath;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.apache.solr.util.SolrClientTestRule;
import org.junit.ClassRule;

public class CustomTLogDirTest extends SolrTestCaseJ4 {

  @ClassRule
  public static final SolrClientTestRule solrClientTestRule =
      new EmbeddedSolrServerTestRule() {
        @Override
        protected void before() {
          System.setProperty("solr.directoryFactory", "solr.NRTCachingDirectoryFactory");
          solrClientTestRule.startSolr(LuceneTestCase.createTempDir());
        }
      };

  private static final AtomicInteger collectionIdx = new AtomicInteger();

  public void testExternal() throws Exception {
    String collectionName = "coll" + collectionIdx.getAndIncrement();
    SolrClient client = solrClientTestRule.getSolrClient(collectionName);

    Path coreRootDir = ((EmbeddedSolrServer) client).getCoreContainer().getCoreRootDirectory();

    Path instanceDir = FilterPath.unwrap(coreRootDir.resolve(collectionName));

    Path ulogDir = LuceneTestCase.createTempDir();
    // absolute path spec that falls outside of the instance and data dirs for the
    // associated core, is assumed to already by namespaced by purpose (tlog). We
    // expect it to be further namespaced by core name.
    Path resolvedTlogDir = ulogDir.resolve(collectionName);
    validateTlogPath(client, instanceDir, ulogDir, resolvedTlogDir);
  }

  public void testRelative() throws Exception {
    String collectionName = "coll" + collectionIdx.getAndIncrement();
    SolrClient client = solrClientTestRule.getSolrClient(collectionName);

    Path coreRootDir = ((EmbeddedSolrServer) client).getCoreContainer().getCoreRootDirectory();

    Path instanceDir = FilterPath.unwrap(coreRootDir.resolve(collectionName));

    Path ulogDir = Path.of("relativeUlogDir");
    // relative dir path spec is taken to be relative to instance dir, so we expect it
    // to be namespaced by purpose within the core.
    Path resolvedTlogDir = instanceDir.resolve(ulogDir).resolve("tlog");
    validateTlogPath(client, instanceDir, ulogDir, resolvedTlogDir);
  }

  public void testIllegalRelative() throws Exception {
    Path ulogDir = Path.of("../");

    Path configSet = LuceneTestCase.createTempDir();
    System.setProperty("enable.update.log", "true");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    System.setProperty("solr.ulog.dir", ulogDir.toString()); // picked up from `solrconfig.xml`
    SolrTestCaseJ4.copyMinConf(configSet.toFile(), null, "solrconfig.xml");

    // relative dir path specs should not be able to "escape" the core-scoped instance dir;
    // check that this config is unsuccessful
    expectThrows(
        Exception.class,
        () ->
            solrClientTestRule
                .newCollection("illegal")
                .withConfigSet(configSet.toString())
                .create());
  }

  public void testAbsoluteSubdir() throws Exception {
    String collectionName = "coll" + collectionIdx.getAndIncrement();
    SolrClient client = solrClientTestRule.getSolrClient(collectionName);

    Path coreRootDir = ((EmbeddedSolrServer) client).getCoreContainer().getCoreRootDirectory();

    Path instanceDir = FilterPath.unwrap(coreRootDir.resolve(collectionName));

    Path ulogDir = instanceDir.resolve("absoluteUlogDir");
    // an absolute dir path spec, if it is contained within the instance dir, is taken
    // to already be namespaced to the core. We expect the tlog dir to be namespaced by
    // purpose (tlog) within core, just as is the case with relative path spec.
    Path resolvedTlogDir = ulogDir.resolve("tlog");
    validateTlogPath(client, instanceDir, ulogDir, resolvedTlogDir);
  }

  public void testDefault() throws Exception {
    String collectionName = "coll" + collectionIdx.getAndIncrement();
    SolrClient client = solrClientTestRule.getSolrClient(collectionName);

    Path coreRootDir = ((EmbeddedSolrServer) client).getCoreContainer().getCoreRootDirectory();

    Path instanceDir = FilterPath.unwrap(coreRootDir.resolve(collectionName));

    // whether the normal default ulog dir spec `[instanceDir]/data` is configured
    // implicitly or explicitly, we expect to find the tlog in the same place:
    Path resolvedTlogDir = instanceDir.resolve("data/tlog");
    validateTlogPath(client, instanceDir, null, resolvedTlogDir);
  }

  public void testExplicitDefault() throws Exception {
    String collectionName = "coll" + collectionIdx.getAndIncrement();
    SolrClient client = solrClientTestRule.getSolrClient(collectionName);

    Path coreRootDir = ((EmbeddedSolrServer) client).getCoreContainer().getCoreRootDirectory();

    Path instanceDir = FilterPath.unwrap(coreRootDir.resolve(collectionName));

    Path ulogDir = instanceDir.resolve("data");
    // whether the normal default ulog dir spec `[instanceDir]/data` is configured
    // implicitly or explicitly, we expect to find the tlog in the same place:
    Path resolvedTlogDir = instanceDir.resolve("data/tlog");
    validateTlogPath(client, instanceDir, ulogDir, resolvedTlogDir);
  }

  private static void validateTlogPath(
      SolrClient client, Path instanceDir, Path ulogDir, Path resolvedTlogDir) throws Exception {
    Path configSet = LuceneTestCase.createTempDir();
    System.setProperty("enable.update.log", "true");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    if (ulogDir != null) {
      System.setProperty("solr.ulog.dir", ulogDir.toString()); // picked up from `solrconfig.xml`
    }
    SolrTestCaseJ4.copyMinConf(configSet.toFile(), null, "solrconfig.xml");

    String collectionName = instanceDir.getFileName().toString();

    solrClientTestRule.newCollection(collectionName).withConfigSet(configSet.toString()).create();

    // resolvedTlogDir = instanceDir.resolve("data/tlog"); // legacy impl _always_ resulted in this

    // add some docs to populate tlog
    client.add(sdoc("id", "1"));
    client.add(sdoc("id", "2"));
    client.add(sdoc("id", "3"));
    client.commit();

    File[] list =
        resolvedTlogDir.toFile().listFiles((f) -> f.isFile() && f.getName().startsWith("tlog."));

    assertNotNull(list);
    assertEquals(1, list.length);
    CoreContainer cc = ((EmbeddedSolrServer) client).getCoreContainer();
    cc.unload(collectionName, true, true, true);
    assertFalse(resolvedTlogDir.toFile().exists());
  }
}
