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

import org.junit.Test;

public class AzureBlobPathsTest extends AbstractAzureBlobClientTest {

  @Test
  public void testPathExists() throws Exception {
    String path = "path-exists-test-" + java.util.UUID.randomUUID() + ".txt";

    // Initially should not exist
    assertFalse("Path should not exist initially", client.pathExists(path));

    // Create file
    pushContent(path, "test content");

    // Should exist now
    assertTrue("Path should exist after creation", client.pathExists(path));
  }

  @Test
  public void testDirectoryExists() throws Exception {
    String dirPath = "test-directory-" + java.util.UUID.randomUUID() + "/";

    // Initially should not exist
    assertFalse("Directory should not exist initially", client.pathExists(dirPath));

    // Create directory
    client.createDirectory(dirPath);

    // Should exist now
    assertTrue("Directory should exist after creation", client.pathExists(dirPath));
  }

  @Test
  public void testIsDirectory() throws Exception {
    String dirPath = "is-directory-test/";
    String filePath = "is-directory-test.txt";

    // Create directory
    client.createDirectory(dirPath);
    assertTrue("Should be a directory", client.isDirectory(dirPath));

    // Create file
    pushContent(filePath, "test content");
    assertFalse("Should not be a directory", client.isDirectory(filePath));
  }

  @Test
  public void testFileLength() throws Exception {
    String path = "file-length-test.txt";
    String content = "File length test content";

    // Create file
    pushContent(path, content);

    // Check length
    assertEquals("File length should match", content.length(), client.length(path));
  }

  @Test
  public void testDirectoryLength() throws Exception {
    String dirPath = "directory-length-test/";

    // Create directory
    client.createDirectory(dirPath);

    // Should throw exception when getting length of directory
    try {
      client.length(dirPath);
      fail("Should throw exception when getting length of directory");
    } catch (AzureBlobException e) {
      // Expected
    }
  }

  @Test
  public void testListDirectory() throws Exception {
    String dirPath = "list-directory-test/";

    // Create directory
    client.createDirectory(dirPath);

    // Initially should be empty
    String[] files = client.listDir(dirPath);
    assertEquals("Directory should be empty initially", 0, files.length);

    // Add some files
    String[] fileNames = {"file1.txt", "file2.txt", "subdir/"};
    for (String fileName : fileNames) {
      String fullPath = dirPath + fileName;
      if (fileName.endsWith("/")) {
        client.createDirectory(fullPath);
      } else {
        pushContent(fullPath, "Content of " + fileName);
      }
    }

    // List directory contents
    files = client.listDir(dirPath);
    assertEquals("Should list all files and directories", fileNames.length, files.length);

    // Verify all files are listed
    for (String fileName : fileNames) {
      boolean found = false;
      for (String listedFile : files) {
        if (fileName.equals(listedFile)) {
          found = true;
          break;
        }
      }
      assertTrue("Should find file: " + fileName, found);
    }
  }

  @Test
  public void testListAll() throws Exception {
    String dirPath = "list-all-test/";

    // Create directory structure
    client.createDirectory(dirPath);
    client.createDirectory(dirPath + "subdir1/");
    client.createDirectory(dirPath + "subdir2/");

    pushContent(dirPath + "file1.txt", "Content 1");
    pushContent(dirPath + "file2.txt", "Content 2");
    pushContent(dirPath + "subdir1/file3.txt", "Content 3");
    pushContent(dirPath + "subdir2/file4.txt", "Content 4");

    // List all files recursively
    java.util.Set<String> allFiles = new java.util.HashSet<>();
    listAllRecursive(dirPath, allFiles);

    // Should find all files
    assertTrue("Should find file1.txt", allFiles.contains(dirPath + "file1.txt"));
    assertTrue("Should find file2.txt", allFiles.contains(dirPath + "file2.txt"));
    assertTrue("Should find subdir1/file3.txt", allFiles.contains(dirPath + "subdir1/file3.txt"));
    assertTrue("Should find subdir2/file4.txt", allFiles.contains(dirPath + "subdir2/file4.txt"));
  }

  private void listAllRecursive(String dirPath, java.util.Set<String> allFiles)
      throws AzureBlobException {
    String[] files = client.listDir(dirPath);
    for (String file : files) {
      String fullPath = dirPath + file;
      if (file.endsWith("/")) {
        // It's a directory
        allFiles.add(fullPath);
        listAllRecursive(fullPath, allFiles);
      } else {
        // It's a file
        allFiles.add(fullPath);
      }
    }
  }

  @Test
  public void testDeleteFile() throws Exception {
    String path = "delete-file-test.txt";

    // Create file
    pushContent(path, "test content");
    assertTrue("File should exist", client.pathExists(path));

    // Delete file
    client.delete(java.util.Set.of(path));

    // Should not exist anymore
    assertFalse("File should not exist after deletion", client.pathExists(path));
  }

  @Test
  public void testDeleteDirectory() throws Exception {
    String dirPath = "delete-directory-test/";
    String filePath = dirPath + "nested-file.txt";

    // Create directory and file
    client.createDirectory(dirPath);
    pushContent(filePath, "nested content");

    assertTrue("Directory should exist", client.pathExists(dirPath));
    assertTrue("File should exist", client.pathExists(filePath));

    // Delete directory
    client.deleteDirectory(dirPath);

    // Should not exist anymore
    assertFalse("Directory should not exist after deletion", client.pathExists(dirPath));
    assertFalse("File should not exist after deletion", client.pathExists(filePath));
  }

  @Test
  public void testDeleteNonExistentFile() throws Exception {
    String path = "non-existent-file.txt";

    // Should not exist
    assertFalse("File should not exist", client.pathExists(path));

    // Delete non-existent file should not throw exception
    client.delete(java.util.Set.of(path));
  }

  @Test
  public void testDeleteNonExistentDirectory() throws Exception {
    String dirPath = "non-existent-directory/";

    // Should not exist
    assertFalse("Directory should not exist", client.pathExists(dirPath));

    // Delete non-existent directory should not throw exception
    client.deleteDirectory(dirPath);
  }

  @Test
  public void testNestedDirectories() throws Exception {
    String rootDir = "nested-test/";
    String subDir1 = rootDir + "subdir1/";
    String subDir2 = rootDir + "subdir2/";
    String deepDir = subDir1 + "deepdir/";

    // Create nested directory structure
    client.createDirectory(rootDir);
    client.createDirectory(subDir1);
    client.createDirectory(subDir2);
    client.createDirectory(deepDir);

    // Verify all directories exist
    assertTrue("Root directory should exist", client.pathExists(rootDir));
    assertTrue("Sub directory 1 should exist", client.pathExists(subDir1));
    assertTrue("Sub directory 2 should exist", client.pathExists(subDir2));
    assertTrue("Deep directory should exist", client.pathExists(deepDir));

    // Add files to different levels
    pushContent(rootDir + "root-file.txt", "Root file content");
    pushContent(subDir1 + "sub-file.txt", "Sub file content");
    pushContent(deepDir + "deep-file.txt", "Deep file content");

    // Verify files exist
    assertTrue("Root file should exist", client.pathExists(rootDir + "root-file.txt"));
    assertTrue("Sub file should exist", client.pathExists(subDir1 + "sub-file.txt"));
    assertTrue("Deep file should exist", client.pathExists(deepDir + "deep-file.txt"));
  }

  @Test
  public void testPathSanitization() throws Exception {
    // Test various path formats
    String[] testPaths = {
      "simple-file.txt",
      "/leading-slash.txt",
      "trailing-slash/",
      "/both-slashes/",
      "nested/path/file.txt",
      "//double-slash.txt",
      "  spaced-file.txt  ",
      "special-chars!@#$%^&*().txt"
    };

    for (String testPath : testPaths) {
      try {
        String sanitizedPath = client.sanitizedPath(testPath);
        assertNotNull("Sanitized path should not be null", sanitizedPath);
        assertFalse("Sanitized path should not start with slash", sanitizedPath.startsWith("/"));
      } catch (AzureBlobException e) {
        // Some paths might be invalid, which is expected
      }
    }
  }

  @Test
  public void testFilePathSanitization() throws Exception {
    // Test file path sanitization
    String[] validFilePaths = {
      "simple-file.txt", "nested/path/file.txt", "file-with-dashes.txt", "file_with_underscores.txt"
    };

    for (String filePath : validFilePaths) {
      try {
        String sanitizedPath = client.sanitizedFilePath(filePath);
        assertNotNull("Sanitized file path should not be null", sanitizedPath);
        assertFalse("Sanitized file path should not end with slash", sanitizedPath.endsWith("/"));
      } catch (AzureBlobException e) {
        fail("Valid file path should not throw exception: " + filePath);
      }
    }

    // Test invalid file paths
    String[] invalidFilePaths = {"file-with-trailing-slash/", "", "   "};

    for (String filePath : invalidFilePaths) {
      try {
        client.sanitizedFilePath(filePath);
        fail("Invalid file path should throw exception: " + filePath);
      } catch (AzureBlobException e) {
        // Expected
      }
    }
  }

  @Test
  public void testDirectoryPathSanitization() throws Exception {
    // Test directory path sanitization
    String[] testDirPaths = {
      "simple-dir", "nested/path/dir", "dir-with-dashes", "dir_with_underscores"
    };

    for (String dirPath : testDirPaths) {
      try {
        String sanitizedPath = client.sanitizedDirPath(dirPath);
        assertNotNull("Sanitized directory path should not be null", sanitizedPath);
        assertTrue("Sanitized directory path should end with slash", sanitizedPath.endsWith("/"));
      } catch (AzureBlobException e) {
        fail("Valid directory path should not throw exception: " + dirPath);
      }
    }
  }
}
