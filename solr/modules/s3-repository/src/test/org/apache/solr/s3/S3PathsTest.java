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
package org.apache.solr.s3;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.Test;

/** Test creating and deleting objects at different paths. */
public class S3PathsTest extends AbstractS3ClientTest {

  /** The root must always exist. */
  @Test
  public void testRoot() throws S3Exception {
    assertTrue(client.pathExists("/"));
  }

  /** Simple tests with files. */
  @Test
  public void testFiles() throws S3Exception {
    assertFalse(client.pathExists("simple-file"));
    assertFalse(client.pathExists("/simple-file/"));

    pushContent("/simple-file", "blah");
    assertTrue("File should exist without a leading slash", client.pathExists("simple-file"));
    assertTrue("File should exist with a leading slash", client.pathExists("/simple-file"));

    assertFalse("File should not be considered a directory", client.isDirectory("/simple-file"));
  }

  /** Simple tests with a directory. */
  @Test
  public void testDirectory() throws S3Exception {
    // createDirectory is a no-op on S3 (no marker objects are written).
    // A prefix is treated as a directory if it has children — write one to establish the prefix.
    client.createDirectory("/simple-directory"); // no-op
    pushContent("/simple-directory/a-file", "content");

    assertFalse(
        "Bare name without trailing slash is not a key", client.pathExists("/simple-directory"));
    assertTrue(
        "Dir should exist without a leading slash", client.pathExists("simple-directory/a-file"));
    assertTrue(
        "Prefix with children should be considered a directory",
        client.isDirectory("simple-directory/"));
    assertTrue(
        "Leading slash should be irrelevant for determining if dir is a dir",
        client.isDirectory("/simple-directory/"));
  }

  /** Happy path of deleting a directory */
  @Test
  public void testDeleteDirectory() throws S3Exception {

    client.createDirectory("/delete-dir");

    pushContent("/delete-dir/file1", "file1");
    pushContent("/delete-dir/file2", "file2");

    client.deleteDirectory("/delete-dir");

    assertFalse("dir should no longer exist after deletion", client.pathExists("/delete-dir/"));
    assertFalse(
        "files in dir should be recursively deleted", client.pathExists("/delete-dir/file1"));
    assertFalse(
        "files in dir should be recursively deleted", client.pathExists("/delete-dir/file2"));
  }

  /** Ensure directory deletion is recursive. */
  @Test
  public void testDeleteDirectoryMultipleLevels() throws S3Exception {

    client.createDirectory("/delete-dir");
    pushContent("/delete-dir/file1", "file1");

    client.createDirectory("/delete-dir/sub-dir1");
    pushContent("/delete-dir/sub-dir1/file2", "file2");

    client.createDirectory("/delete-dir/sub-dir1/sub-dir2");
    pushContent("/delete-dir/sub-dir1/sub-dir2/file3", "file3");

    client.deleteDirectory("/delete-dir");

    // All files and subdirs in /delete-dir should no longer exist
    assertFalse(client.pathExists("/delete-dir/"));
    assertFalse(client.pathExists("/delete-dir/file1"));
    assertFalse(client.pathExists("/delete-dir/sub-dir1/"));
    assertFalse(client.pathExists("/delete-dir/sub-dir1/file2"));
    assertFalse(client.pathExists("/delete-dir/sub-dir1/sub-dir2/"));
    assertFalse(client.pathExists("/delete-dir/sub-dir1/sub-dir2/file3"));
  }

  /**
   * S3StorageClient batches deletes (1000 per request) to adhere to S3's hard limit. Since the
   * S3Mock does not enforce this limitation, however, the exact batch size doesn't matter here: all
   * we're really testing is that the partition logic works and doesn't miss any files.
   */
  @Test
  public void testDeleteBatching() throws S3Exception {

    client.createDirectory("/delete-dir");

    List<String> pathsToDelete = new ArrayList<>();
    for (int i = 0; i < 101; i++) {
      String path = "delete-dir/file" + i;
      pathsToDelete.add(path);
      pushContent(path, "foo");
    }

    client.deleteObjects(pathsToDelete, 10);
    for (String path : pathsToDelete) {
      assertFalse("file " + path + " does exist", client.pathExists(path));
    }
  }

  @Test
  public void testDeleteMultipleFiles() throws S3Exception {

    client.createDirectory("/my");
    pushContent("/my/file1", "file1");
    pushContent("/my/file2", "file2");
    pushContent("/my/file3", "file3");

    client.delete(List.of("/my/file1", "my/file3"));

    assertFalse(client.pathExists("/my/file1"));
    assertFalse(client.pathExists("/my/file3"));

    // Other files with same prefix should be there
    assertTrue(
        "Deletes to file1 and file3 should not affect file2", client.pathExists("/my/file2"));
  }

  /** Test deleting a directory which is the prefix of another objects (without deleting them). */
  @Test
  public void testDeletePrefix() throws S3Exception {

    client.createDirectory("/my");
    pushContent("/my/file", "file");

    pushContent("/my-file1", "file1");
    pushContent("/my-file2", "file2");

    client.deleteDirectory("/my");

    // Deleted directory and its file should be gone
    assertFalse(client.pathExists("/my/file"));
    assertFalse(client.pathExists("/my"));

    // Other files with same prefix should be there
    assertTrue(client.pathExists("/my-file1"));
    assertTrue(client.pathExists("/my-file2"));
  }

  /**
   * Test that a directory containing objects but lacking an explicit directory marker (i.e. a
   * "virtual directory") is still recognised as a directory. This mirrors the situation that arises
   * after an {@code aws s3 sync} round-trip: {@code aws s3 sync} skips directory marker objects
   * (keys ending with {@code /}) when downloading, so they are never re-uploaded and are absent in
   * the target bucket.
   */
  @Test
  public void testVirtualDirectoryWithoutMarker() throws S3Exception {
    // Since createDirectory is a no-op, there are never marker objects.
    // Write files directly under a prefix to establish virtual directories.
    pushContent("/virtual-dir/file1", "file1");
    pushContent("/virtual-dir/sub-dir/file2", "file2");

    assertTrue(
        "A path with objects under it should be considered a virtual directory",
        client.isDirectory("/virtual-dir"));
    assertTrue(
        "A nested path with objects under it should be considered a virtual directory",
        client.isDirectory("/virtual-dir/sub-dir"));
    assertFalse(
        "A path with no objects and no marker should not be a directory",
        client.isDirectory("/virtual-dir/empty-sub-dir"));
  }

  /** Check listing objects of a directory. */
  @Test
  public void testListDir() throws S3Exception {

    client.createDirectory("/list-dir");
    client.createDirectory("/list-dir/sub-dir");
    pushContent("/list-dir/file", "file");
    pushContent("/list-dir/sub-dir/file", "file");

    // These files have same prefix in name, but should not be returned
    pushContent("/list-dir-file1", "file1");
    pushContent("/list-dir-file2", "file2");

    String[] items = client.listDir("/list-dir");
    assertEquals(
        "listDir returned a different set of files than expected",
        Set.of("file", "sub-dir"),
        Set.of(items));
  }
}
