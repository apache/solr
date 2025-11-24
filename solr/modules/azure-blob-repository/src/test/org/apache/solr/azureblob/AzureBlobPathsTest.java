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

    assertFalse("Path should not exist initially", client.pathExists(path));

    pushContent(path, "test content");

    assertTrue("Path should exist after creation", client.pathExists(path));
  }

  @Test
  public void testDirectoryExists() throws Exception {
    String dirPath = "test-directory-" + java.util.UUID.randomUUID() + "/";

    assertFalse("Directory should not exist initially", client.pathExists(dirPath));

    client.createDirectory(dirPath);

    assertTrue("Directory should exist after creation", client.pathExists(dirPath));
  }

  @Test
  public void testIsDirectory() throws Exception {
    String dirPath = "is-directory-test/";
    String filePath = "is-directory-test.txt";

    client.createDirectory(dirPath);
    assertTrue("Should be a directory", client.isDirectory(dirPath));

    pushContent(filePath, "test content");
    assertFalse("Should not be a directory", client.isDirectory(filePath));
  }

  @Test
  public void testFileLength() throws Exception {
    String path = "file-length-test.txt";
    String content = "File length test content";

    pushContent(path, content);

    assertEquals("File length should match", content.length(), client.length(path));
  }

  @Test
  public void testDirectoryLength() throws Exception {
    String dirPath = "directory-length-test/";

    client.createDirectory(dirPath);

    expectThrows(AzureBlobException.class, () -> client.length(dirPath));
  }

  @Test
  public void testListDirectory() throws Exception {
    String dirPath = "list-directory-test/";

    client.createDirectory(dirPath);

    String[] files = client.listDir(dirPath);
    assertEquals("Directory should be empty initially", 0, files.length);

    String[] fileNames = {"file1.txt", "file2.txt", "subdir/"};
    for (String fileName : fileNames) {
      String fullPath = dirPath + fileName;
      if (fileName.endsWith("/")) {
        client.createDirectory(fullPath);
      } else {
        pushContent(fullPath, "Content of " + fileName);
      }
    }

    files = client.listDir(dirPath);
    assertEquals("Should list all files and directories", fileNames.length, files.length);

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

    client.createDirectory(dirPath);
    client.createDirectory(dirPath + "subdir1/");
    client.createDirectory(dirPath + "subdir2/");

    pushContent(dirPath + "file1.txt", "Content 1");
    pushContent(dirPath + "file2.txt", "Content 2");
    pushContent(dirPath + "subdir1/file3.txt", "Content 3");
    pushContent(dirPath + "subdir2/file4.txt", "Content 4");

    java.util.Set<String> allFiles = new java.util.HashSet<>();
    listAllRecursive(dirPath, allFiles);

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

    pushContent(path, "test content");
    assertTrue("File should exist", client.pathExists(path));

    client.delete(java.util.Set.of(path));

    assertFalse("File should not exist after deletion", client.pathExists(path));
  }

  @Test
  public void testDeleteDirectory() throws Exception {
    String dirPath = "delete-directory-test/";
    String filePath = dirPath + "nested-file.txt";

    client.createDirectory(dirPath);
    pushContent(filePath, "nested content");

    assertTrue("Directory should exist", client.pathExists(dirPath));
    assertTrue("File should exist", client.pathExists(filePath));

    client.deleteDirectory(dirPath);

    assertFalse("Directory should not exist after deletion", client.pathExists(dirPath));
    assertFalse("File should not exist after deletion", client.pathExists(filePath));
  }

  @Test
  public void testDeleteNonExistentFile() throws Exception {
    String path = "non-existent-file.txt";

    assertFalse("File should not exist", client.pathExists(path));

    client.delete(java.util.Set.of(path));
  }

  @Test
  public void testDeleteNonExistentDirectory() throws Exception {
    String dirPath = "non-existent-directory/";

    assertFalse("Directory should not exist", client.pathExists(dirPath));

    client.deleteDirectory(dirPath);
  }

  @Test
  public void testNestedDirectories() throws Exception {
    String rootDir = "nested-test/";
    String subDir1 = rootDir + "subdir1/";
    String subDir2 = rootDir + "subdir2/";
    String deepDir = subDir1 + "deepdir/";

    client.createDirectory(rootDir);
    client.createDirectory(subDir1);
    client.createDirectory(subDir2);
    client.createDirectory(deepDir);

    assertTrue("Root directory should exist", client.pathExists(rootDir));
    assertTrue("Sub directory 1 should exist", client.pathExists(subDir1));
    assertTrue("Sub directory 2 should exist", client.pathExists(subDir2));
    assertTrue("Deep directory should exist", client.pathExists(deepDir));

    pushContent(rootDir + "root-file.txt", "Root file content");
    pushContent(subDir1 + "sub-file.txt", "Sub file content");
    pushContent(deepDir + "deep-file.txt", "Deep file content");

    assertTrue("Root file should exist", client.pathExists(rootDir + "root-file.txt"));
    assertTrue("Sub file should exist", client.pathExists(subDir1 + "sub-file.txt"));
    assertTrue("Deep file should exist", client.pathExists(deepDir + "deep-file.txt"));
  }

  @Test
  public void testPathSanitization() throws Exception {
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
      String sanitizedPath = client.sanitizedPath(testPath);
      assertNotNull("Sanitized path should not be null", sanitizedPath);
      assertFalse("Sanitized path should not start with slash", sanitizedPath.startsWith("/"));
    }
  }

  @Test
  public void testFilePathSanitization() throws Exception {
    String[] validFilePaths = {
      "simple-file.txt", "nested/path/file.txt", "file-with-dashes.txt", "file_with_underscores.txt"
    };

    for (String filePath : validFilePaths) {
      String sanitizedPath = client.sanitizedFilePath(filePath);
      assertNotNull("Sanitized file path should not be null", sanitizedPath);
      assertFalse("Sanitized file path should not end with slash", sanitizedPath.endsWith("/"));
    }

    String[] invalidFilePaths = {"file-with-trailing-slash/", "", "   "};

    for (String filePath : invalidFilePaths) {
      final String path = filePath;
      expectThrows(AzureBlobException.class, () -> client.sanitizedFilePath(path));
    }
  }

  @Test
  public void testDirectoryPathSanitization() throws Exception {
    String[] testDirPaths = {
      "simple-dir", "nested/path/dir", "dir-with-dashes", "dir_with_underscores"
    };

    for (String dirPath : testDirPaths) {
      String sanitizedPath = client.sanitizedDirPath(dirPath);
      assertNotNull("Sanitized directory path should not be null", sanitizedPath);
      assertTrue("Sanitized directory path should end with slash", sanitizedPath.endsWith("/"));
    }
  }
}
