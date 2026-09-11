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

import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;

public class AzureBlobPathsTest extends AbstractAzureBlobClientTest {

  /** {@code pathExists()} returns false before push, true after. */
  @Test
  public void testPathExists() throws Exception {
    String path = "path-exists-test-" + UUID.randomUUID() + ".txt";

    assertFalse("Path should not exist initially", client.pathExists(path));

    pushContent(path, "test content");

    assertTrue("Path should exist after creation", client.pathExists(path));
  }

  /** {@code pathExists()} reports a freshly-created directory marker as present. */
  @Test
  public void testDirectoryExists() throws Exception {
    String dirPath = "test-directory-" + UUID.randomUUID() + "/";

    assertFalse("Directory should not exist initially", client.pathExists(dirPath));

    client.createDirectory(dirPath);

    assertTrue("Directory should exist after creation", client.pathExists(dirPath));
  }

  /** {@code isDirectory()} distinguishes directory markers from regular blobs. */
  @Test
  public void testIsDirectory() throws Exception {
    String dirPath = "is-directory-test/";
    String filePath = "is-directory-test.txt";

    client.createDirectory(dirPath);
    assertTrue("Should be a directory", client.isDirectory(dirPath));

    pushContent(filePath, "test content");
    assertFalse("Should not be a directory", client.isDirectory(filePath));
  }

  /** {@code length()} returns the exact byte count of the uploaded content. */
  @Test
  public void testFileLength() throws Exception {
    String path = "file-length-test.txt";
    String content = "File length test content";

    pushContent(path, content);

    assertEquals("File length should match", content.length(), client.length(path));
  }

  /** {@code length()} on a directory throws — directory markers have no meaningful size. */
  @Test
  public void testDirectoryLength() throws Exception {
    String dirPath = "directory-length-test/";

    client.createDirectory(dirPath);

    expectThrows(AzureBlobException.class, () -> client.length(dirPath));
  }

  /**
   * {@code listDir()} returns immediate children only — both files and sub-directories, with
   * sub-directory entries returned as bare names (no trailing delimiter).
   */
  @Test
  public void testListDirectory() throws Exception {
    String dirPath = "list-directory-test/";

    client.createDirectory(dirPath);

    String[] files = client.listDir(dirPath);
    assertEquals("Directory should be empty initially", 0, files.length);

    String[] toCreate = {"file1.txt", "file2.txt", "subdir/"};
    for (String fileName : toCreate) {
      String fullPath = dirPath + fileName;
      if (fileName.endsWith("/")) {
        client.createDirectory(fullPath);
      } else {
        pushContent(fullPath, "Content of " + fileName);
      }
    }

    files = client.listDir(dirPath);
    assertEquals("Should list all files and directories", toCreate.length, files.length);

    // Directory children are listed without a trailing slash.
    String[] expectedNames = {"file1.txt", "file2.txt", "subdir"};
    for (String expected : expectedNames) {
      boolean found = false;
      for (String listedFile : files) {
        if (expected.equals(listedFile)) {
          found = true;
          break;
        }
      }
      assertTrue("Should find entry: " + expected, found);
    }
  }

  /** Recursive walk via {@code listDir()} reaches files nested under multiple sub-directories. */
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

    Set<String> allFiles = new HashSet<>();
    listAllRecursive(dirPath, allFiles);

    assertTrue("Should find file1.txt", allFiles.contains(dirPath + "file1.txt"));
    assertTrue("Should find file2.txt", allFiles.contains(dirPath + "file2.txt"));
    assertTrue("Should find subdir1/file3.txt", allFiles.contains(dirPath + "subdir1/file3.txt"));
    assertTrue("Should find subdir2/file4.txt", allFiles.contains(dirPath + "subdir2/file4.txt"));
  }

  /** Happy path: {@code delete()} of a single existing file removes it. */
  @Test
  public void testDeleteFile() throws Exception {
    String path = "delete-file-test.txt";

    pushContent(path, "test content");
    assertTrue("File should exist", client.pathExists(path));

    client.delete(Set.of(path));

    assertFalse("File should not exist after deletion", client.pathExists(path));
  }

  /** {@code deleteDirectory()} recursively removes a directory and its contents. */
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

  /** Strict {@code delete()}: a single missing path raises {@link AzureBlobNotFoundException}. */
  @Test
  public void testDeleteNonExistentFile() throws Exception {
    String path = "non-existent-file.txt";

    assertFalse("File should not exist", client.pathExists(path));

    AzureBlobNotFoundException thrown =
        expectThrows(AzureBlobNotFoundException.class, () -> client.delete(Set.of(path)));
    assertTrue(
        "Exception message should reference the missing path: " + thrown.getMessage(),
        thrown.getMessage().contains(path));
  }

  /** Lenient {@code deleteDirectory()}: a missing directory is a silent no-op (no exception). */
  @Test
  public void testDeleteNonExistentDirectory() throws Exception {
    String dirPath = "non-existent-directory/";

    assertFalse("Directory should not exist", client.pathExists(dirPath));

    client.deleteDirectory(dirPath);
  }

  /**
   * Deeply-nested directories + files are all observable via {@code pathExists()} after creation.
   */
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

  /** {@code sanitizedPath()} strips leading slashes from a variety of input shapes. */
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

  /**
   * {@code sanitizedFilePath()} accepts valid file paths and rejects trailing-slash / blank input.
   */
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

  /** {@code sanitizedDirPath()} always appends a trailing slash to dir-shaped input. */
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

  /** {@code createURI()} normalizes plain, leading-slash, and already-schemed locations alike. */
  @Test
  public void testCreateUri() throws Exception {
    try (AzureBlobBackupRepository repository = new AzureBlobBackupRepository()) {
      assertEquals("/loc", repository.createURI("loc").getPath());
      assertEquals("/loc", repository.createURI("/loc").getPath());
      assertEquals("/loc", repository.createURI("blob:/loc").getPath());
      assertEquals("blob", repository.createURI("loc").getScheme());

      // createDirectoryURI appends a trailing slash.
      assertEquals("/loc/", repository.createDirectoryURI("loc").getPath());
    }
  }

  /** {@code resolve()} joins nested components in order under the base path. */
  @Test
  public void testResolveNestedComponents() throws Exception {
    try (AzureBlobBackupRepository repository = new AzureBlobBackupRepository()) {
      URI base = repository.createURI("loc");

      assertEquals("/loc/a/b/c", repository.resolve(base, "a", "b", "c").getPath());
    }
  }

  /** Redundant slashes in components are collapsed by {@code resolve()} (no {@code //}). */
  @Test
  public void testResolveCollapsesRedundantSlashes() throws Exception {
    try (AzureBlobBackupRepository repository = new AzureBlobBackupRepository()) {
      URI base = repository.createURI("loc");

      URI resolved = repository.resolve(base, "a/", "b");
      assertEquals("/loc/a/b", resolved.getPath());
      assertFalse("resolved path should not contain '//'", resolved.getPath().contains("//"));
    }
  }

  /** {@code resolveDirectory()} guarantees a trailing slash on the final component. */
  @Test
  public void testResolveDirectoryAppendsTrailingSlash() throws Exception {
    try (AzureBlobBackupRepository repository = new AzureBlobBackupRepository()) {
      URI base = repository.createURI("loc");

      assertEquals("/loc/sub/", repository.resolveDirectory(base, "sub").getPath());
    }
  }

  /**
   * {@code resolveDirectory()} with no components keeps a single trailing slash and never produces
   * a doubled separator.
   */
  @Test
  public void testResolveDirectoryEmptyComponents() throws Exception {
    try (AzureBlobBackupRepository repository = new AzureBlobBackupRepository()) {
      URI base = repository.createDirectoryURI("loc");

      URI resolved = repository.resolveDirectory(base);
      assertEquals("/loc/", resolved.getPath());
      assertFalse("resolved path should not contain '//'", resolved.getPath().contains("//"));
    }
  }

  private void listAllRecursive(String dirPath, Set<String> allFiles) throws AzureBlobException {
    String[] files = client.listDir(dirPath);
    for (String file : files) {
      String fullPath = dirPath + file;
      // listDir returns bare names, so probe the type to decide whether to recurse.
      if (client.isDirectory(fullPath)) {
        String dirFullPath = fullPath + "/";
        allFiles.add(dirFullPath);
        listAllRecursive(dirFullPath, allFiles);
      } else {
        allFiles.add(fullPath);
      }
    }
  }
}
