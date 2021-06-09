/* * Licensed to the Apache Software Foundation (ASF) under one or more
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
package org.apache.solr.ltr;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.util.RestTestHarness;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.AfterClass;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class TestLTROnSolrCloudBase extends TestRerankBase {

  MiniSolrCloudCluster solrCluster;
  String solrconfig = "solrconfig-ltr.xml";
  String schema = "schema.xml";
  SortedMap<ServletHolder,String> extraServlets = null;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    extraServlets = setupTestInit(solrconfig, schema, true);
    System.setProperty("enable.update.log", "true");

    int numberOfShards = random().nextInt(4)+1;
    int numberOfReplicas = random().nextInt(2)+1;

    setupSolrCluster(numberOfShards, numberOfReplicas);
  }

  void setupSolrCluster(int numShards, int numReplicas) throws Exception {
    setupSolrCluster(numShards, numReplicas, numShards * numReplicas);
  }

  void setupSolrCluster(int numShards, int numReplicas, int numServers) throws Exception {
    JettyConfig jc = buildJettyConfig("/solr");
    jc = JettyConfig.builder(jc).withServlets(extraServlets).build();
    solrCluster = new MiniSolrCloudCluster(numServers, tmpSolrHome.toPath(), jc);
    File configDir = tmpSolrHome.toPath().resolve("collection1/conf").toFile();
    solrCluster.uploadConfigSet(configDir.toPath(), "conf1");

    solrCluster.getSolrClient().setDefaultCollection(COLLECTION);

    createCollection(COLLECTION, "conf1", numShards, numReplicas);
    indexDocuments(COLLECTION);
    for (JettySolrRunner solrRunner : solrCluster.getJettySolrRunners()) {
      if (!solrRunner.getCoreContainer().getCores().isEmpty()){
        String coreName = solrRunner.getCoreContainer().getCores().iterator().next().getName();
        restTestHarness = new RestTestHarness(() -> solrRunner.getBaseUrl().toString() + "/" + coreName);
        break;
      }
    }
    loadModelsAndFeatures();
  }

  private void createCollection(String name, String config, int numShards, int numReplicas)
      throws Exception {
    CollectionAdminResponse response;
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(name, config, numShards, numReplicas);
    response = create.process(solrCluster.getSolrClient());

    if (response.getStatus() != 0 || response.getErrorMessages() != null) {
      fail("Could not create collection. Response" + response.toString());
    }
    solrCluster.waitForActiveCollection(name, numShards, numShards * numReplicas);
  }

  void indexDocument(String collection, String id, String title, String description, int popularity)
    throws Exception{
    // force subclasses to implement this so that it is explicit in each test, how the values are set in a doc
    throw new UnsupportedOperationException();
  }

  void indexDocuments(final String collection) throws Exception {
    indexDocuments(collection, "a1", "bloom");
  }

  private void indexDocuments(final String collection, String title, String description)
       throws Exception {
    final int collectionSize = 8;
    // put documents in random order to check that advanceExact is working correctly
    List<Integer> docIds = IntStream.rangeClosed(1, collectionSize).boxed().collect(toList());
    Collections.shuffle(docIds, random());

    int docCounter = 1;
    for (int docId : docIds) {
      final int popularity = docId;
      indexDocument(collection, String.valueOf(docId), title, description, popularity);
      // maybe commit in the middle in order to check that everything works fine for multi-segment case
      if (docCounter == collectionSize / 2 && random().nextBoolean()) {
        solrCluster.getSolrClient().commit(collection);
      }
      docCounter++;
    }
    solrCluster.getSolrClient().commit(collection, true, true);
  }

  void loadModelsAndFeatures() throws Exception {
    // force subclasses to implement this so that the used features and model(s) are explicit in each test
    throw new UnsupportedOperationException();
  }

  void reloadCollection(String collection) throws Exception {
    CollectionAdminRequest.Reload reloadRequest = CollectionAdminRequest.reloadCollection(collection);
    CollectionAdminResponse response = reloadRequest.process(solrCluster.getSolrClient());
    assertEquals(0, response.getStatus());
    assertTrue(response.isSuccess());
  }

  @Override
  public void tearDown() throws Exception {
    restTestHarness.close();
    restTestHarness = null;
    solrCluster.shutdown();
    super.tearDown();
  }

  @AfterClass
  public static void after() throws Exception {
    if (null != tmpSolrHome) {
      FileUtils.deleteDirectory(tmpSolrHome);
      tmpSolrHome = null;
    }
    aftertest();
    System.clearProperty("managed.schema.mutable");
  }
}
