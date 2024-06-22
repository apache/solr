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
package org.apache.solr.handler.admin.api;

import java.util.ArrayList;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.CreateCoreSnapshotResponse;
import org.apache.solr.client.api.model.DeleteSnapshotResponse;
import org.apache.solr.client.api.model.ListCoreSnapshotsResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CoreSnapshotAPITest extends SolrTestCaseJ4 {

  private CoreSnapshot coreSnapshotAPI;

  @BeforeClass
  public static void initializeCoreAndRequestFactory() throws Exception {
    initCore("solrconfig.xml", "schema.xml");

    lrf = h.getRequestFactory("/api", 0, 10);
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();

    SolrQueryRequest solrQueryRequest = req();
    SolrQueryResponse solrQueryResponse = new SolrQueryResponse();
    CoreContainer coreContainer = h.getCoreContainer();
    CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker =
        new CoreAdminHandler.CoreAdminAsyncTracker();

    coreSnapshotAPI =
        new CoreSnapshot(solrQueryRequest, solrQueryResponse, coreContainer, coreAdminAsyncTracker);
  }

  private List<String> snapshotsToCleanup = new ArrayList<>();

  @After
  public void deleteSnapshots() throws Exception {
    for (String snapshotName : snapshotsToCleanup) {
      coreSnapshotAPI.deleteSnapshot(coreName, snapshotName, null);
    }

    snapshotsToCleanup.clear();
  }

  @Test
  public void testCreateSnapshotReturnsValidResponse() throws Exception {
    final String snapshotName = "my-new-snapshot";

    final CreateCoreSnapshotResponse response =
        coreSnapshotAPI.createSnapshot(coreName, snapshotName, null);
    snapshotsToCleanup.add(snapshotName);

    assertEquals(coreName, response.core);
    assertEquals("my-new-snapshot", response.commitName);
    assertNotNull(response.indexDirPath);
    assertEquals(Long.valueOf(1L), response.generation);
    assertFalse(response.files.isEmpty());
  }

  @Test
  public void testReportsErrorWhenCreatingSnapshotForNonexistentCore() {
    final String nonExistentCoreName = "non-existent";

    final SolrException solrException =
        expectThrows(
            SolrException.class,
            () -> {
              coreSnapshotAPI.createSnapshot(nonExistentCoreName, "my-new-snapshot", null);
            });
    assertEquals(400, solrException.code());
    assertTrue(
        "Exception message differed from expected: " + solrException.getMessage(),
        solrException.getMessage().contains("Unable to locate core " + nonExistentCoreName));
  }

  @Test
  public void testListSnapshotsReturnsValidResponse() throws Exception {
    final String snapshotNameBase = "my-new-snapshot-";

    for (int i = 0; i < 5; i++) {
      final String snapshotName = snapshotNameBase + i;
      coreSnapshotAPI.createSnapshot(coreName, snapshotName, null);
      snapshotsToCleanup.add(snapshotName);
    }

    final ListCoreSnapshotsResponse response = coreSnapshotAPI.listSnapshots(coreName);

    assertEquals(5, response.snapshots.size());
  }

  @Test
  public void testReportsErrorWhenListingSnapshotsForNonexistentCore() {
    final String nonExistentCoreName = "non-existent";

    final SolrException solrException =
        expectThrows(
            SolrException.class,
            () -> {
              coreSnapshotAPI.listSnapshots(nonExistentCoreName);
            });
    assertEquals(400, solrException.code());
    assertTrue(
        "Exception message differed from expected: " + solrException.getMessage(),
        solrException.getMessage().contains("Unable to locate core " + nonExistentCoreName));
  }

  @Test
  public void testDeleteSnapshotReturnsValidResponse() throws Exception {
    final String snapshotName = "my-new-snapshot";

    coreSnapshotAPI.createSnapshot(coreName, snapshotName, null);

    final DeleteSnapshotResponse deleteResponse =
        coreSnapshotAPI.deleteSnapshot(coreName, snapshotName, null);

    assertEquals(coreName, deleteResponse.coreName);
    assertEquals(snapshotName, deleteResponse.commitName);

    final ListCoreSnapshotsResponse response = coreSnapshotAPI.listSnapshots(coreName);

    assertEquals(0, response.snapshots.size());
  }

  @Test
  public void testReportsErrorWhenDeletingSnapshotForNonexistentCore() {
    final String nonExistentCoreName = "non-existent";

    final SolrException solrException =
        expectThrows(
            SolrException.class,
            () -> {
              coreSnapshotAPI.deleteSnapshot(nonExistentCoreName, "non-existent-snapshot", null);
            });
    assertEquals(400, solrException.code());
    assertTrue(
        "Exception message differed from expected: " + solrException.getMessage(),
        solrException.getMessage().contains("Unable to locate core " + nonExistentCoreName));
  }
}
