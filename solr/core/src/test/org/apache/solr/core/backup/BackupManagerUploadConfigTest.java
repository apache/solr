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
package org.apache.solr.core.backup;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for {@link BackupManager#uploadConfigDir} covering the case where an object-store
 * backup (e.g. S3) has no explicit directory marker but the config files are still present.
 */
public class BackupManagerUploadConfigTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void setUpClass() {
    assumeWorkingMockito();
  }

  private static final URI BACKUP_PATH = URI.create("s3://bucket/backup/");
  private static final URI ZK_STATE_URI = URI.create("s3://bucket/backup/zk_backup/");
  private static final URI CONFIG_DIR_URI =
      URI.create("s3://bucket/backup/zk_backup/configs/myconfig/");
  private static final URI SOLRCONFIG_URI =
      URI.create("s3://bucket/backup/zk_backup/configs/myconfig/solrconfig.xml");

  /**
   * When the directory marker object is absent (S3 "no-marker" scenario) but config files exist
   * under the prefix, {@code uploadConfigDir} must succeed and upload those files.
   *
   * <p>This is the regression test for the case where a backup was copied through a local
   * filesystem and re-uploaded to S3, losing the zero-byte directory marker objects in the process.
   */
  @Test
  public void testUploadConfigDir_withoutDirectoryMarker_succeedsWhenFilesPresent()
      throws Exception {
    BackupRepository mockRepo = mock(BackupRepository.class);
    ZkStateReader mockZkStateReader = mock(ZkStateReader.class);
    ConfigSetService mockConfigSetService = mock(ConfigSetService.class);

    // Simulate S3: no marker object → exists() is false, but listAll() finds the files
    when(mockRepo.exists(CONFIG_DIR_URI)).thenReturn(false);
    when(mockRepo.listAll(CONFIG_DIR_URI)).thenReturn(new String[] {"solrconfig.xml"});

    // resolveDirectory chain used by getZkStateDir() and uploadConfigDir()
    when(mockRepo.resolveDirectory(BACKUP_PATH, BackupManager.ZK_STATE_DIR))
        .thenReturn(ZK_STATE_URI);
    when(mockRepo.resolveDirectory(ZK_STATE_URI, BackupManager.CONFIG_STATE_DIR, "myconfig"))
        .thenReturn(CONFIG_DIR_URI);

    // resolve() used by uploadConfigToSolrCloud() to build the per-file URI
    when(mockRepo.resolve(CONFIG_DIR_URI, "solrconfig.xml")).thenReturn(SOLRCONFIG_URI);
    when(mockRepo.getPathType(SOLRCONFIG_URI)).thenReturn(BackupRepository.PathType.FILE);

    // Return minimal valid XML bytes so FileTypeMagicUtil does not reject the file
    byte[] configBytes = "<config/>".getBytes(StandardCharsets.UTF_8);
    IndexInput mockInput = mock(IndexInput.class);
    when(mockInput.length()).thenReturn((long) configBytes.length);
    doAnswer(
            invocation -> {
              byte[] dest = invocation.getArgument(0);
              int offset = invocation.getArgument(1);
              System.arraycopy(configBytes, 0, dest, offset, configBytes.length);
              return null;
            })
        .when(mockInput)
        .readBytes(any(), eq(0), eq(configBytes.length));
    when(mockRepo.openInput(CONFIG_DIR_URI, "solrconfig.xml", IOContext.DEFAULT))
        .thenReturn(mockInput);

    BackupManager backupManager = BackupManager.forBackup(mockRepo, mockZkStateReader, BACKUP_PATH);

    // Must not throw even though exists() returned false
    backupManager.uploadConfigDir("myconfig", "restoredConfig", mockConfigSetService);

    // The config file must have been uploaded to the target configset
    verify(mockConfigSetService)
        .uploadFileToConfig(
            eq("restoredConfig"), eq("solrconfig.xml"), any(byte[].class), eq(false));
  }

  /**
   * When neither a directory marker nor any files exist under the config prefix, {@code
   * uploadConfigDir} must throw {@link IllegalArgumentException}.
   */
  @Test
  public void testUploadConfigDir_throwsWhenTrulyAbsent() throws Exception {
    BackupRepository mockRepo = mock(BackupRepository.class);
    ZkStateReader mockZkStateReader = mock(ZkStateReader.class);
    ConfigSetService mockConfigSetService = mock(ConfigSetService.class);

    URI missingConfigDir = URI.create("s3://bucket/backup/zk_backup/configs/nonexistent/");

    when(mockRepo.exists(missingConfigDir)).thenReturn(false);
    when(mockRepo.listAll(missingConfigDir)).thenReturn(new String[0]);

    when(mockRepo.resolveDirectory(BACKUP_PATH, BackupManager.ZK_STATE_DIR))
        .thenReturn(ZK_STATE_URI);
    when(mockRepo.resolveDirectory(ZK_STATE_URI, BackupManager.CONFIG_STATE_DIR, "nonexistent"))
        .thenReturn(missingConfigDir);

    BackupManager backupManager = BackupManager.forBackup(mockRepo, mockZkStateReader, BACKUP_PATH);

    assertThrows(
        IllegalArgumentException.class,
        () -> backupManager.uploadConfigDir("nonexistent", "target", mockConfigSetService));
  }

  /**
   * When the directory marker IS present (the normal case), {@code uploadConfigDir} uses the fast
   * path via {@code exists()} and must still upload files correctly.
   */
  @Test
  public void testUploadConfigDir_withDirectoryMarker_succeedsNormally() throws Exception {
    BackupRepository mockRepo = mock(BackupRepository.class);
    ZkStateReader mockZkStateReader = mock(ZkStateReader.class);
    ConfigSetService mockConfigSetService = mock(ConfigSetService.class);

    // Marker present → exists() returns true, listAll() should not be called
    when(mockRepo.exists(CONFIG_DIR_URI)).thenReturn(true);
    when(mockRepo.listAll(CONFIG_DIR_URI)).thenReturn(new String[] {"solrconfig.xml"});

    when(mockRepo.resolveDirectory(BACKUP_PATH, BackupManager.ZK_STATE_DIR))
        .thenReturn(ZK_STATE_URI);
    when(mockRepo.resolveDirectory(ZK_STATE_URI, BackupManager.CONFIG_STATE_DIR, "myconfig"))
        .thenReturn(CONFIG_DIR_URI);

    when(mockRepo.resolve(CONFIG_DIR_URI, "solrconfig.xml")).thenReturn(SOLRCONFIG_URI);
    when(mockRepo.getPathType(SOLRCONFIG_URI)).thenReturn(BackupRepository.PathType.FILE);

    byte[] configBytes = "<config/>".getBytes(StandardCharsets.UTF_8);
    IndexInput mockInput = mock(IndexInput.class);
    when(mockInput.length()).thenReturn((long) configBytes.length);
    doAnswer(
            invocation -> {
              byte[] dest = invocation.getArgument(0);
              int offset = invocation.getArgument(1);
              System.arraycopy(configBytes, 0, dest, offset, configBytes.length);
              return null;
            })
        .when(mockInput)
        .readBytes(any(), eq(0), eq(configBytes.length));
    when(mockRepo.openInput(CONFIG_DIR_URI, "solrconfig.xml", IOContext.DEFAULT))
        .thenReturn(mockInput);

    BackupManager backupManager = BackupManager.forBackup(mockRepo, mockZkStateReader, BACKUP_PATH);

    backupManager.uploadConfigDir("myconfig", "restoredConfig", mockConfigSetService);

    verify(mockConfigSetService)
        .uploadFileToConfig(
            eq("restoredConfig"), eq("solrconfig.xml"), any(byte[].class), eq(false));
  }
}
