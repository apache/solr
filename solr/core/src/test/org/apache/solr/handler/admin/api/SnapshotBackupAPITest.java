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

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler.ReplicationHandlerConfig;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link SnapshotBackupAPI}. */
public class SnapshotBackupAPITest extends SolrTestCase {

  private SolrCore solrCore;
  private ReplicationHandlerConfig replicationHandlerConfig;
  private SnapshotBackupAPI snapshotBackupApi;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setupMocks() {
    resetMocks();
    snapshotBackupApi = new SnapshotBackupAPI(solrCore, replicationHandlerConfig);
  }

  @Test
  public void testMissingBody() throws Exception {
    final SolrException expected =
        expectThrows(
            SolrException.class,
            () -> {
              snapshotBackupApi.createBackup(null);
            });
    assertEquals(400, expected.code());
    assertEquals("Required request-body is missing", expected.getMessage());
  }

  @Test
  public void testSuccessfulBackupCommand() throws Exception {
    when(replicationHandlerConfig.getNumberBackupsToKeep()).thenReturn(11);
    final var backupRequestBody = new SnapshotBackupAPI.BackupReplicationRequestBody();
    backupRequestBody.name = "test";
    backupRequestBody.numberToKeep = 7;

    final var responseBody =
        new TrackingSnapshotBackupAPI(solrCore, replicationHandlerConfig)
            .createBackup(backupRequestBody);

    assertEquals(7, TrackingSnapshotBackupAPI.numberToKeep.get());
    assertEquals(11, TrackingSnapshotBackupAPI.numberBackupsToKeep.get());
  }

  private void resetMocks() {
    solrCore = mock(SolrCore.class);
    replicationHandlerConfig = mock(ReplicationHandlerConfig.class);
    when(replicationHandlerConfig.getNumberBackupsToKeep()).thenReturn(5);
  }

  private static class TrackingSnapshotBackupAPI extends SnapshotBackupAPI {

    private static final AtomicInteger numberToKeep = new AtomicInteger();
    private static final AtomicInteger numberBackupsToKeep = new AtomicInteger();

    @Inject
    public TrackingSnapshotBackupAPI(
        SolrCore solrCore, ReplicationHandlerConfig replicationHandlerConfig) {
      super(solrCore, replicationHandlerConfig);
    }

    @Override
    protected void doSnapShoot(
        int numberToKeep,
        int numberBackupsToKeep,
        String location,
        String repoName,
        String commitName,
        String name,
        SolrCore solrCore,
        Consumer<NamedList<?>> resultConsumer) {
      TrackingSnapshotBackupAPI.numberToKeep.set(numberToKeep);
      TrackingSnapshotBackupAPI.numberBackupsToKeep.set(numberBackupsToKeep);
    }
  }
}
