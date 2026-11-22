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

import static org.apache.solr.core.CoreContainer.ALLOW_PATHS_SYSPROP;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.api.model.RestoreStatusResponse;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.ReplicationApi;
import org.apache.solr.client.solrj.request.ReplicationBackupsApi;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.handler.BackupStatusChecker;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.apache.solr.util.TimeOut;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class RestoreAndDeleteBackupAPITest extends SolrTestCase {
  private static final String CORE_NAME = "restoreAndDeleteBackupApiTestCore";
  private static final int WAIT_SECONDS = 30;
  private static final int INITIAL_DOCS = 5;
  private static final int EXTRA_DOCS = 3;

  @ClassRule public static final SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void setupSolr() throws Exception {
    System.setProperty(ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    solrTestRule.startSolr(createTempDir());
    solrTestRule.newCollection(CORE_NAME).withConfigSet(ExternalPaths.DEFAULT_CONFIGSET).create();
  }

  @Test
  public void testRestoreByNameFromDefaultLocation() throws Exception {
    final String backupName = "defaultLocation";
    resetIndex(INITIAL_DOCS);
    createBackup(backupName, null);
    addDocs(INITIAL_DOCS, EXTRA_DOCS);
    assertEquals(INITIAL_DOCS + EXTRA_DOCS, countDocs());

    var request = new ReplicationApi.RestoreIndex(CORE_NAME);
    request.setName(backupName);
    assertNull(request.process(client()).error);

    waitForRestoreSuccess();
    assertEquals(INITIAL_DOCS, countDocs());
  }

  //  @Test
  //  public void testRestoreByNameFromExplicitLocation() throws Exception {
  //    final String backupName = "explicitLocation";
  //    final Path location = allowedTempDir();
  //    resetIndex(INITIAL_DOCS);
  //    createBackup(backupName, location);
  //    addDocs(INITIAL_DOCS, EXTRA_DOCS);
  //
  //    var request = new ReplicationApi.RestoreIndex(CORE_NAME);
  //    request.setName(backupName);
  //    request.setLocation(location.toString());
  //    assertNull(request.process(client()).error);
  //
  //    waitForRestoreSuccess();
  //    assertEquals(INITIAL_DOCS, countDocs());
  //  }

  //  @Test
  //  public void testRestoreOfUnknownLocationIsRejected() {
  //    var request = new ReplicationApi.RestoreIndex(CORE_NAME);
  //    request.setName("doesNotExist");
  //    request.setLocation(createTempDir().toString());
  //    final RemoteSolrException ex =
  //        expectThrows(RemoteSolrException.class, () -> request.process(client()));
  //    // Not under the allowed paths
  //    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
  //  }

  @Test
  public void testDeleteBackupFromDefaultLocation() throws Exception {
    final String backupName = "deleteDefaultLocation";
    resetIndex(INITIAL_DOCS);
    createBackup(backupName, null);

    var request = new ReplicationBackupsApi.DeleteBackup(CORE_NAME, backupName);
    assertNull(request.process(client()).error);

    new BackupStatusChecker(client()).waitForBackupDeletionSuccess(backupName, WAIT_SECONDS);
  }

  @Test
  public void testDeleteBackupFromExplicitLocation() throws Exception {
    final String backupName = "deleteExplicitLocation";
    final Path location = allowedTempDir();
    resetIndex(INITIAL_DOCS);
    createBackup(backupName, location);
    assertTrue(location.resolve("snapshot." + backupName).toFile().isDirectory());

    var request = new ReplicationBackupsApi.DeleteBackup(CORE_NAME, backupName);
    request.setLocation(location.toString());
    assertNull(request.process(client()).error);

    final TimeOut timeOut = new TimeOut(WAIT_SECONDS, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    timeOut.waitFor(
        "backup directory was not deleted",
        () -> !location.resolve("snapshot." + backupName).toFile().exists());
  }

  @Test
  public void testDeleteUnknownBackupReturnsBadRequest() {
    var request = new ReplicationBackupsApi.DeleteBackup(CORE_NAME, "doesNotExist");
    final RemoteSolrException ex =
        expectThrows(RemoteSolrException.class, () -> request.process(client()));
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, ex.code());
  }

  private static SolrClient client() {
    return solrTestRule.getSolrClient(CORE_NAME);
  }

  private static Path allowedTempDir() {
    final Path dir = createTempDir();
    solrTestRule.getJetty().getCoreContainer().getAllowPaths().add(dir);
    return dir;
  }

  /**
   * Uses the v1 backup command, since only it records the backup status that {@link
   * BackupStatusChecker} polls.
   */
  private static void createBackup(String name, Path location) throws Exception {
    final ModifiableSolrParams params =
        new ModifiableSolrParams().set("command", "backup").set("name", name);
    if (location != null) {
      params.set("location", location.toString());
    }
    new GenericSolrRequest(
            SolrRequest.METHOD.GET, "/replication", SolrRequest.SolrRequestType.ADMIN, params)
        .setRequiresCollection(true)
        .process(client());
    new BackupStatusChecker(client()).waitForBackupSuccess(name, WAIT_SECONDS);
  }

  private static void waitForRestoreSuccess() throws Exception {
    final TimeOut timeOut = new TimeOut(WAIT_SECONDS, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    timeOut.waitFor(
        "restore did not succeed",
        () -> {
          try {
            var status = new ReplicationApi.FetchRestoreStatus(CORE_NAME).process(client());
            return status.restoreStatus.status == RestoreStatusResponse.RestoreState.SUCCESS;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  private static void resetIndex(int numDocs) throws Exception {
    client().deleteByQuery("*:*");
    addDocs(0, numDocs);
  }

  private static void addDocs(int startId, int count) throws Exception {
    for (int i = startId; i < startId + count; i++) {
      final SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      client().add(doc);
    }
    client().commit();
  }

  private static long countDocs() throws Exception {
    return client().query(new SolrQuery("*:*")).getResults().getNumFound();
  }
}
