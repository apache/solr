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

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_ID;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_LOCATION;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_PURGE_UNUSED;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_REPOSITORY;
import static org.apache.solr.common.params.CoreAdminParams.MAX_NUM_BACKUP_POINTS;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.PurgeUnusedFilesRequestBody;
import org.apache.solr.common.SolrException;
import org.junit.Test;

/** Unit tests for {@link DeleteCollectionBackup} */
public class DeleteCollectionBackupAPITest extends SolrTestCaseJ4 {
  @Test
  public void testReportsErrorIfBackupNameMissing() {
    // Single delete
    {
      final SolrException thrown =
          expectThrows(
              SolrException.class,
              () -> {
                final var api = new DeleteCollectionBackup(null, null, null);
                api.deleteSingleBackupById(
                    null, "someBackupId", "someLocation", "someRepository", "someAsyncId");
              });

      assertEquals(400, thrown.code());
      assertEquals("Missing required parameter: name", thrown.getMessage());
    }

    // Multi delete
    {
      final SolrException thrown =
          expectThrows(
              SolrException.class,
              () -> {
                final var api = new DeleteCollectionBackup(null, null, null);
                api.deleteMultipleBackupsByRecency(
                    null, 123, "someLocation", "someRepository", "someAsyncId");
              });

      assertEquals(400, thrown.code());
      assertEquals("Missing required parameter: name", thrown.getMessage());
    }

    // Garbage collect unused files
    {
      final var requestBody = new PurgeUnusedFilesRequestBody();
      requestBody.location = "someLocation";
      requestBody.repositoryName = "someRepository";
      requestBody.async = "someAsyncId";
      final SolrException thrown =
          expectThrows(
              SolrException.class,
              () -> {
                final var api = new DeleteCollectionBackup(null, null, null);
                api.garbageCollectUnusedBackupFiles(null, requestBody);
              });

      assertEquals(400, thrown.code());
      assertEquals("Missing required parameter: name", thrown.getMessage());
    }
  }

  @Test
  public void testDeletionByIdReportsErrorIfIdMissing() {
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              final var api = new DeleteCollectionBackup(null, null, null);
              api.deleteSingleBackupById(
                  "someBackupName", null, "someLocation", "someRepository", "someAsyncId");
            });

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: backupId", thrown.getMessage());
  }

  @Test
  public void testMultiVersionDeletionReportsErrorIfRetainParamMissing() {
    final SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              final var api = new DeleteCollectionBackup(null, null, null);
              api.deleteMultipleBackupsByRecency(
                  "someBackupName", null, "someLocation", "someRepository", "someAsyncId");
            });

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: retainLatest", thrown.getMessage());
  }

  // The message created in this test isn't valid in practice, since it contains mutually-exclusive
  // parameters, but that doesn't matter for the purposes of this test.
  @Test
  public void testCreateRemoteMessageAllParams() {
    final var remoteMessage =
        DeleteCollectionBackup.createRemoteMessage(
                "someBackupName",
                "someBackupId",
                123,
                true,
                "someLocation",
                "someRepository",
                "someAsyncId")
            .getProperties();

    assertEquals(8, remoteMessage.size());
    assertEquals("deletebackup", remoteMessage.get(QUEUE_OPERATION));
    assertEquals("someBackupName", remoteMessage.get(NAME));
    assertEquals("someBackupId", remoteMessage.get(BACKUP_ID));
    assertEquals(Integer.valueOf(123), remoteMessage.get(MAX_NUM_BACKUP_POINTS));
    assertEquals(Boolean.TRUE, remoteMessage.get(BACKUP_PURGE_UNUSED));
    assertEquals("someLocation", remoteMessage.get(BACKUP_LOCATION));
    assertEquals("someRepository", remoteMessage.get(BACKUP_REPOSITORY));
    assertEquals("someAsyncId", remoteMessage.get(ASYNC));
  }

  @Test
  public void testCreateRemoteMessageOnlyRequiredParams() {
    final var remoteMessage =
        DeleteCollectionBackup.createRemoteMessage(
                "someBackupName", "someBackupId", null, null, null, null, null)
            .getProperties();

    assertEquals(3, remoteMessage.size());
    assertEquals("deletebackup", remoteMessage.get(QUEUE_OPERATION));
    assertEquals("someBackupName", remoteMessage.get(NAME));
    assertEquals("someBackupId", remoteMessage.get(BACKUP_ID));
  }
}
