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
package org.apache.solr.blob;

import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class BlobIncrementalBackupTest extends AbstractBlobClientTest {

  @Test
  public void testIncrementalBackup() throws Exception {
    String backupPath = "incremental-backup-test/";

    // Create initial backup
    createBackup(backupPath + "backup1/", "Initial backup content");

    // Create incremental backup
    createBackup(backupPath + "backup2/", "Incremental backup content");

    // Verify both backups exist
    assertTrue("Initial backup should exist", client.pathExists(backupPath + "backup1/"));
    assertTrue("Incremental backup should exist", client.pathExists(backupPath + "backup2/"));
  }

  @Test
  public void testBackupWithMultipleFiles() throws Exception {
    String backupPath = "multi-file-backup-test/";

    // Create backup with multiple files
    String[] files = {"file1.txt", "file2.txt", "file3.txt"};
    String[] contents = {"Content 1", "Content 2", "Content 3"};

    for (int i = 0; i < files.length; i++) {
      pushContent(backupPath + files[i], contents[i]);
    }

    // Verify all files exist
    for (String file : files) {
      assertTrue("File should exist: " + file, client.pathExists(backupPath + file));
    }
  }

  @Test
  public void testBackupWithNestedDirectories() throws Exception {
    String backupPath = "nested-backup-test/";

    // Create nested directory structure
    String[] dirs = {
      backupPath + "level1/", backupPath + "level1/level2/", backupPath + "level1/level2/level3/"
    };

    for (String dir : dirs) {
      client.createDirectory(dir);
    }

    // Add files at different levels
    pushContent(backupPath + "root-file.txt", "Root file content");
    pushContent(backupPath + "level1/mid-file.txt", "Mid file content");
    pushContent(backupPath + "level1/level2/level3/deep-file.txt", "Deep file content");

    // Verify structure
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

    // Create backup
    String originalContent = "Original backup content";
    pushContent(backupPath + "backup-file.txt", originalContent);

    // Simulate restore by copying content
    try (var input = client.pullStream(backupPath + "backup-file.txt");
        var output = client.pushStream(restorePath + "restored-file.txt")) {

      byte[] buffer = new byte[1024];
      int bytesRead;
      while ((bytesRead = input.read(buffer)) != -1) {
        output.write(buffer, 0, bytesRead);
      }
    }

    // Verify restore
    assertTrue("Restored file should exist", client.pathExists(restorePath + "restored-file.txt"));

    // Verify content
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

    // Create large file
    StringBuilder contentBuilder = new StringBuilder();
    for (int i = 0; i < 10000; i++) {
      contentBuilder.append("This is line ").append(i).append(" of the large backup file.\n");
    }
    String largeContent = contentBuilder.toString();

    pushContent(backupPath + "large-backup.txt", largeContent);

    // Verify large file
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

    // Create binary file
    byte[] binaryData = new byte[1024];
    for (int i = 0; i < binaryData.length; i++) {
      binaryData[i] = (byte) (i % 256);
    }

    pushContent(backupPath + "binary-backup.bin", binaryData);

    // Verify binary file
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

    // Create multiple backups
    for (int i = 1; i <= 5; i++) {
      pushContent(backupPath + "backup" + i + "/backup-file.txt", "Backup " + i + " content");
    }

    // Verify all backups exist
    for (int i = 1; i <= 5; i++) {
      assertTrue(
          "Backup " + i + " should exist", client.pathExists(backupPath + "backup" + i + "/"));
    }

    // Cleanup old backups (keep only last 3)
    for (int i = 1; i <= 2; i++) {
      client.deleteDirectory(backupPath + "backup" + i + "/");
    }

    // Verify cleanup
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

    // Create backup with metadata files
    pushContent(
        backupPath + "backup-metadata.json",
        "{\"timestamp\":\"2023-01-01T00:00:00Z\",\"version\":\"1.0\"}");
    pushContent(backupPath + "backup-data.txt", "Backup data content");

    // Verify metadata files
    assertTrue(
        "Metadata file should exist", client.pathExists(backupPath + "backup-metadata.json"));
    assertTrue("Data file should exist", client.pathExists(backupPath + "backup-data.txt"));
  }

  @Test
  public void testConcurrentBackups() throws Exception {
    String backupPath = "concurrent-backup-test/";

    // Simulate concurrent backups
    String[] backupNames = {"backup1", "backup2", "backup3"};
    String[] contents = {"Content 1", "Content 2", "Content 3"};

    // Create backups concurrently (simulated)
    for (int i = 0; i < backupNames.length; i++) {
      pushContent(backupPath + backupNames[i] + "/backup-file.txt", contents[i]);
    }

    // Verify all backups exist
    for (String backupName : backupNames) {
      assertTrue(
          "Backup should exist: " + backupName, client.pathExists(backupPath + backupName + "/"));
    }
  }

  private void createBackup(String backupPath, String content) throws BlobException {
    client.createDirectory(backupPath);
    pushContent(backupPath + "backup-file.txt", content);
  }
}
