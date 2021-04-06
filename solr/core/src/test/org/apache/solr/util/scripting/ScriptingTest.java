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

package org.apache.solr.util.scripting;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

public class ScriptingTest extends SolrCloudTestCase {

  private static final String COLLECTION = ScriptingTest.class.getSimpleName() + "_collection";

  private static SolrCloudManager cloudManager;
  private static CoreContainer cc;

  @BeforeClass
  public static void setupCluster() throws Exception {
    // either set this, or copy the scripts into each node's solrHome dir
    System.setProperty("solr.allow.unsafe.resourceloading", "true");
    configureCluster(3)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    cc = cluster.getJettySolrRunner(0).getCoreContainer();
    cloudManager = cc.getZkController().getSolrCloudManager();
  }

  @After
  public void cleanup() throws Exception {
    cluster.deleteAllCollections();
    System.clearProperty("initScript");
    System.clearProperty("shutdownScript");
    System.clearProperty("testErrorHandling");
    System.clearProperty("solr.allow.unsafe.resourceloading");
  }

  String testBasicScript =
      "# simple comment\n" +
          "// another comment\n" +
          "      \n" +
          "set key=myNode&value=${_random_node_}\n" +
          "request /admin/collections?action=CREATE&name=" + COLLECTION + "&numShards=2&nrtReplicas=2\n" +
          // this one is tricky - retrieve object by index because we don't know the key, and
          // then it's a map Entry, so retrieve its 'value' because again we don't know the key
          "set key=oneCore&value=${_last_result_/result/success[0]/value/core}\n" +
          "wait_collection collection=" + COLLECTION + "&shards=2&replicas=2\n" +
          "set key=myNode&value=${_random_node_}\n" +
          "request /admin/collections?action=ADDREPLICA&collection=" + COLLECTION + "&shard=shard1&node=${myNode}\n" +
          "set key=myNode&value=${_live_nodes_[0]}\n" +
          "request /admin/collections?action=ADDREPLICA&collection=" + COLLECTION + "&shard=shard1&node=${myNode}\n" +
          "loop iterations=2\n" +
          "  request /admin/collections?action=ADDREPLICA&collection=" + COLLECTION + "&shard=shard2&node=${myNode}\n" +
          "  set key=oneCore&value=${_last_result_/result/success[0]/value/core}\n" +
          "  wait_replica collection=" + COLLECTION + "&core=${oneCore}&state=ACTIVE\n" +
          "  log key=_last_result_/result[0]/key&format=******%20Replica%20name:%20{}%20******\n" +
          "end\n" +
          "dump\n";

  @Test
  public void testBasicScript() throws Exception {
    Script script = Script.parse(cluster.getSolrClient(), null, testBasicScript);
    script.run();
    DocCollection coll = cloudManager.getClusterStateProvider().getCollection(COLLECTION);
    assertEquals("numShards", 2, coll.getSlices().size());
    assertEquals("num replicas shard1", 4, coll.getSlice("shard1").getReplicas().size());
    assertEquals("num replicas shard2", 4, coll.getSlice("shard2").getReplicas().size());
  }

  @Test
  public void testLoadResource() throws Exception {
    String scriptName = "initScript.txt";
    JettySolrRunner jetty = cluster.getRandomJetty(random());
    String nodeName = jetty.getNodeName();
    SolrResourceLoader loader = jetty.getCoreContainer().getResourceLoader();
    File scriptFile = new File(TEST_PATH().resolve(scriptName).toString());
//    File destFile = new File(jetty.getSolrHome(), scriptName);
//    FileUtils.copyFile(scriptFile, destFile);
    Script script = Script.parseResource(loader, cluster.getSolrClient(), nodeName, scriptFile.toString());
    script.run();
    DocCollection coll = cloudManager.getClusterStateProvider().getCollection(COLLECTION);
    assertEquals("numShards", 2, coll.getSlices().size());
    assertEquals("num replicas shard1", 4, coll.getSlice("shard1").getReplicas().size());
    assertEquals("num replicas shard2", 4, coll.getSlice("shard2").getReplicas().size());
  }

  public void testErrorHandling() throws Exception {
    String scriptName = "initScript2.txt";
    File scriptFile = new File(TEST_PATH().resolve(scriptName).toString());
    System.setProperty("initScript", scriptFile.toString());
    JettySolrRunner jetty = cluster.startJettySolrRunner();
    if (!jetty.getCoreContainer().isShutDown()) {
      fail("should have failed - error handling set in the script to FATAL by default");
    }
    cluster.stopJettySolrRunner(jetty);
    // abort the script
    System.setProperty("testErrorHandling", "ABORT");
    jetty = cluster.startJettySolrRunner();
    cluster.waitForNode(jetty, 10);
    assertFalse("node should be running when error handling is ABORT", jetty.getCoreContainer().isShutDown());
    ClusterState clusterState = cloudManager.getClusterStateProvider().getClusterState();
    assertNull("there should be no 'foobar' collection", clusterState.getCollectionOrNull("foobar"));
    cluster.stopJettySolrRunner(jetty);

    // ignore errors and continue
    System.setProperty("testErrorHandling", "IGNORE");
    jetty = cluster.startJettySolrRunner();
    cluster.waitForNode(jetty, 10);
    assertFalse("node should be running when error handling is IGNORE", jetty.getCoreContainer().isShutDown());
    clusterState = cloudManager.getClusterStateProvider().getClusterState();
    assertNotNull("there should be 'foobar' collection", clusterState.getCollectionOrNull("foobar"));

    // test shutdown script
    scriptFile = new File(TEST_PATH().resolve("shutdownScript.txt").toString());
    System.setProperty("shutdownScript", scriptFile.toString());
    cluster.stopJettySolrRunner(jetty);
    cluster.waitForJettyToStop(jetty);
    clusterState = cloudManager.getClusterStateProvider().getClusterState();
    assertNull("there should be no 'foobar' collection", clusterState.getCollectionOrNull("foobar"));
  }
}
