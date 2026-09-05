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
package org.apache.solr.cli;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * SOLR-18403: SnapshotExportTool must export the state of the named snapshot, not the live index at
 * export time.
 */
public class SnapshotExportToolTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.security.allow.paths", "*");
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @Test
  public void testExportUsesSnapshotStateNotLiveIndex() throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    String collection = "snapshotexporttest";
    CollectionAdminRequest.createCollection(collection, "conf", 1, 1).process(client);
    cluster.waitForActiveCollection(collection, 1, 1);

    // index 5 docs, commit, snapshot at this point
    for (int i = 0; i < 5; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "doc-" + i);
      client.add(collection, doc);
    }
    client.commit(collection);

    String snapshotName = "export-test-snap";
    new CollectionAdminRequest.CreateSnapshot(collection, snapshotName).process(client);

    // index 5 MORE docs (6-10) and commit -- live index now has 10, snapshot still reflects 5
    for (int i = 5; i < 10; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "doc-" + i);
      client.add(collection, doc);
    }
    client.commit(collection);

    assertEquals(10, client.query(collection, params("q", "*:*")).getResults().getNumFound());

    String backupLocation = createTempDir().toString();
    SnapshotExportTool tool = new SnapshotExportTool(new DefaultToolRuntime());
    tool.exportSnapshot(client, collection, snapshotName, backupLocation, null, null);

    String restoredCollection = collection + "_restored";
    CollectionAdminRequest.Restore restore =
        CollectionAdminRequest.restoreCollection(restoredCollection, snapshotName)
            .setLocation(backupLocation);
    assertEquals(0, restore.process(client).getStatus());
    cluster.waitForActiveCollection(restoredCollection, 1, 1);

    assertEquals(
        "export must reflect the snapshot's 5-doc state, not the live index's 10",
        5,
        client.query(restoredCollection, params("q", "*:*")).getResults().getNumFound());
  }
}
