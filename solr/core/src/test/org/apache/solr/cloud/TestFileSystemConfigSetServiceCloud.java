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
package org.apache.solr.cloud;

import java.nio.file.Path;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.core.FileSystemConfigSetService;
import org.apache.solr.core.SolrCore;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

/** {@link FileSystemConfigSetService} configured in solr.xml, used in SolrCloud. */
public class TestFileSystemConfigSetServiceCloud extends SolrCloudTestCase {

  private static final String CONFIG_NAME = "cloud-minimal";

  @BeforeClass
  public static void setupCluster() throws Exception {
    // read-only use of the test configsets, which include _default so it isn't bootstrapped
    Path configSetBaseDir = configset(CONFIG_NAME).getParent().getParent();
    System.setProperty("configSetBaseDir", configSetBaseDir.toString());

    String solrXml =
        MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML.replace(
            "<solr>\n",
            "<solr>\n  <str name=\"configSetService\">"
                + FileSystemConfigSetService.class.getName()
                + "</str>\n");
    configureCluster(2).withSolrXml(solrXml).configure();
  }

  @Test
  public void testCreateIndexQueryReload() throws Exception {
    String collection = "fsConfigColl";
    CollectionAdminRequest.createCollection(collection, CONFIG_NAME, 1, 2)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 1, 2);

    assertConfigSetOfReplicas(collection);

    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "1");
    new UpdateRequest().add(doc).commit(cluster.getSolrClient(), collection);
    assertEquals(
        1L,
        cluster.getSolrClient().query(collection, new SolrQuery("*:*")).getResults().getNumFound());

    CollectionAdminRequest.reloadCollection(collection).process(cluster.getSolrClient());
    assertConfigSetOfReplicas(collection);
    assertEquals(
        1L,
        cluster.getSolrClient().query(collection, new SolrQuery("*:*")).getResults().getNumFound());
  }

  private void assertConfigSetOfReplicas(String collection) {
    for (Replica replica : getCollectionState(collection).replicaStream().toList()) {
      JettySolrRunner jetty = cluster.getReplicaJetty(replica);
      try (SolrCore core = jetty.getCoreContainer().getCore(replica.getCoreName())) {
        assertEquals(CONFIG_NAME, core.getCoreDescriptor().getConfigSet());
      }
    }
  }
}
