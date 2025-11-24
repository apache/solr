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
package org.apache.solr.azureblob;

import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class AzureBlobIncrementalBackupTest extends AbstractAzureBlobClientTest {

  @Test
  public void testIncrementalBackup() throws Exception {
    String backupPath = "incremental-backup-test/";

    createBackup(backupPath + "backup1/", "Initial backup content");
    createBackup(backupPath + "backup2/", "Incremental backup content");

    assertTrue("Initial backup should exist", client.pathExists(backupPath + "backup1/"));
    assertTrue("Incremental backup should exist", client.pathExists(backupPath + "backup2/"));
  }

  @Test
  public void testBackupWithMultipleFiles() throws Exception {
    String backupPath = "multi-file-backup-test/";
    String[] files = {"file1.txt", "file2.txt", "file3.txt"};
    String[] contents = {"Content 1", "Content 2", "Content 3"};

    for (int i = 0; i < files.length; i++) {
      pushContent(backupPath + files[i], contents[i]);
    }

    for (String file : files) {
      assertTrue("File should exist: " + file, client.pathExists(backupPath + file));
    }
  }

  @Test
  public void testBackupWithNestedDirectories() throws Exception {
    String backupPath = "nested-backup-test/";
    String[] dirs = {
      backupPath + "level1/", backupPath + "level1/level2/", backupPath + "level1/level2/level3/"
    };

    for (String dir : dirs) {
      client.createDirectory(dir);
    }

    pushContent(backupPath + "root-file.txt", "Root file content");
    pushContent(backupPath + "level1/mid-file.txt", "Mid file content");
    pushContent(backupPath + "level1/level2/level3/deep-file.txt", "Deep file content");

    assertTrue("Root file should exist", client.pathExists(backupPath + "root-file.txt"));
    assertTrue("Mid file should exist", client.pathExists(backupPath + "level1/mid-file.txt"));
    assertTrue(
        "Deep file should exist",
        client.pathExists(backupPath + "level1/level2/level3/deep-file.txt"));
  }

  @Test
  public void testBackupRestore() throws Exception {
    String backupPath = "backup-restore-test/";
    String restorePath = "restore-test/";
    String originalContent = "Original backup content";

    pushContent(backupPath + "backup-file.txt", originalContent);

    try (var input = client.pullStream(backupPath + "backup-file.txt");
        var output = client.pushStream(restorePath + "restored-file.txt")) {
      byte[] buffer = new byte[1024];
      int bytesRead;
      while ((bytesRead = input.read(buffer)) != -1) {
        output.write(buffer, 0, bytesRead);
      }
    }

    assertTrue("Restored file should exist", client.pathExists(restorePath + "restored-file.txt"));

    try (var input = client.pullStream(restorePath + "restored-file.txt")) {
      byte[] buffer = new byte[1024];
      int bytesRead = input.read(buffer);
      String restoredContent = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
      assertEquals("Restored content should match", originalContent, restoredContent);
    }
  }

  @Test
  public void testBackupWithLargeFiles() throws Exception {
    String backupPath = "large-file-backup-test/";
    StringBuilder contentBuilder = new StringBuilder();
    for (int i = 0; i < 10000; i++) {
      contentBuilder.append("This is line ").append(i).append(" of the large backup file.\n");
    }
    String largeContent = contentBuilder.toString();

    pushContent(backupPath + "large-backup.txt", largeContent);

    assertTrue(
        "Large backup file should exist", client.pathExists(backupPath + "large-backup.txt"));
    assertEquals(
        "Large file length should match",
        largeContent.length(),
        client.length(backupPath + "large-backup.txt"));
  }

  @Test
  public void testBackupWithBinaryFiles() throws Exception {
    String backupPath = "binary-backup-test/";
    byte[] binaryData = new byte[1024];
    for (int i = 0; i < binaryData.length; i++) {
      binaryData[i] = (byte) (i % 256);
    }

    pushContent(backupPath + "binary-backup.bin", binaryData);

    assertTrue(
        "Binary backup file should exist", client.pathExists(backupPath + "binary-backup.bin"));
    assertEquals(
        "Binary file length should match",
        binaryData.length,
        client.length(backupPath + "binary-backup.bin"));
  }

  @Test
  public void testBackupCleanup() throws Exception {
    String backupPath = "backup-cleanup-test/";

    for (int i = 1; i <= 5; i++) {
      pushContent(backupPath + "backup" + i + "/backup-file.txt", "Backup " + i + " content");
    }

    for (int i = 1; i <= 5; i++) {
      assertTrue(
          "Backup " + i + " should exist", client.pathExists(backupPath + "backup" + i + "/"));
    }

    for (int i = 1; i <= 2; i++) {
      client.deleteDirectory(backupPath + "backup" + i + "/");
    }

    for (int i = 1; i <= 2; i++) {
      assertFalse(
          "Old backup " + i + " should not exist",
          client.pathExists(backupPath + "backup" + i + "/"));
    }
    for (int i = 3; i <= 5; i++) {
      assertTrue(
          "Recent backup " + i + " should exist",
          client.pathExists(backupPath + "backup" + i + "/"));
    }
  }

  @Test
  public void testBackupWithMetadata() throws Exception {
    String backupPath = "metadata-backup-test/";

    pushContent(
        backupPath + "backup-metadata.json",
        "{\"timestamp\":\"2023-01-01T00:00:00Z\",\"version\":\"1.0\"}");
    pushContent(backupPath + "backup-data.txt", "Backup data content");

    assertTrue(
        "Metadata file should exist", client.pathExists(backupPath + "backup-metadata.json"));
    assertTrue("Data file should exist", client.pathExists(backupPath + "backup-data.txt"));
  }

  @Test
  public void testConcurrentBackups() throws Exception {
    String backupPath = "concurrent-backup-test/";
    String[] backupNames = {"backup1", "backup2", "backup3"};
    String[] contents = {"Content 1", "Content 2", "Content 3"};

    for (int i = 0; i < backupNames.length; i++) {
      pushContent(backupPath + backupNames[i] + "/backup-file.txt", contents[i]);
    }

    for (String backupName : backupNames) {
      assertTrue(
          "Backup should exist: " + backupName, client.pathExists(backupPath + backupName + "/"));
    }
  }

  private void createBackup(String backupPath, String content) throws AzureBlobException {
    client.createDirectory(backupPath);
    pushContent(backupPath + "backup-file.txt", content);
  }
}
