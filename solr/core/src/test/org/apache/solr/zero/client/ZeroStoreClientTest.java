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

package org.apache.solr.zero.client;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.repository.LocalFileSystemRepository;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.zero.exception.ZeroException;
import org.apache.solr.zero.metadata.ZeroMetadataController;
import org.apache.solr.zero.metadata.ZeroStoreShardMetadata;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Unit tests for {@link ZeroStoreClient} */
public class ZeroStoreClientTest extends SolrTestCaseJ4 {

  private static final String TEST_COLLECTION_NAME = "collection1";
  private static final String TEST_SHARD_NAME_1 = "shard1";
  private static final String TEST_SHARD_NAME_2 = "shard2";
  private static final byte[] EMPTY_BYTES_ARR = {};

  @Rule public TemporaryFolder tempDir = new TemporaryFolder();

  private static Path zeroStoreRootPath;
  private static ZeroStoreClient zeroStoreClient;

  @BeforeClass
  public static void setUpZeroStoreClient() {
    zeroStoreRootPath = createTempDir("tempDir").resolve("LocalZeroStore");
    zeroStoreRootPath.toFile().mkdirs();
    NamedList<String> args = new NamedList<>();
    args.add("location", zeroStoreRootPath.toString());
    LocalFileSystemRepository repo = new LocalFileSystemRepository();
    repo.init(args);
    zeroStoreClient = new ZeroStoreClient(repo, new SolrMetricManager(), 5, 5);
  }

  @After
  public void cleanUp() throws Exception {
    File zeroPath = zeroStoreRootPath.toFile();
    FileUtils.cleanDirectory(zeroPath);
  }

  @AfterClass
  public static void tearDownClient() {
    zeroStoreClient.shutdown();
    zeroStoreClient = null;
  }

  @Test
  public void testListZeroFiles() throws Exception {
    List<ZeroFile> expectedPaths = new ArrayList<>();
    expectedPaths.add(
        new ZeroFile(
            pushStream(
                TEST_COLLECTION_NAME,
                TEST_SHARD_NAME_1,
                new ByteArrayInputStream(EMPTY_BYTES_ARR),
                "xxx",
                "xxx.rAnDomSuFfIx")));
    expectedPaths.add(
        pushStream(
            TEST_COLLECTION_NAME,
            TEST_SHARD_NAME_1,
            new ByteArrayInputStream(EMPTY_BYTES_ARR),
            "yyy",
            "yyy.rAnDomSuFfIx"));
    expectedPaths.add(
        pushStream(
            TEST_COLLECTION_NAME,
            TEST_SHARD_NAME_2,
            new ByteArrayInputStream(EMPTY_BYTES_ARR),
            "zzzz",
            "zzzz.rAnDomSuFfIx"));
    expectedPaths.add(
        pushStream(
            TEST_COLLECTION_NAME,
            TEST_SHARD_NAME_2,
            new ByteArrayInputStream(EMPTY_BYTES_ARR),
            "5678",
            "5678.rAnDomSuFfIx"));

    pushStream(
        "s_test_collection_nomatch",
        "s_test_core_nomatch",
        new ByteArrayInputStream(EMPTY_BYTES_ARR),
        "1234",
        "1234.rAnDomSuFfIx");

    // the common prefix is s_test_core_name for these Zero store files
    Set<ZeroFile> zeroShardFiles =
        zeroStoreClient.listShardZeroFiles(TEST_COLLECTION_NAME, TEST_SHARD_NAME_1);
    zeroShardFiles.addAll(
        zeroStoreClient.listShardZeroFiles(TEST_COLLECTION_NAME, TEST_SHARD_NAME_2));

    Set<ZeroFile> zeroCollectionFiles =
        zeroStoreClient.listCollectionZeroFiles(TEST_COLLECTION_NAME);

    Assert.assertEquals(expectedPaths.size(), zeroShardFiles.size());
    Assert.assertTrue(zeroShardFiles.toString(), zeroShardFiles.containsAll(expectedPaths));

    Assert.assertEquals(expectedPaths.size(), zeroCollectionFiles.size());
    Assert.assertTrue(
        zeroCollectionFiles.toString(), zeroCollectionFiles.containsAll(expectedPaths));
  }

  /**
   * Simple test of pushing content to Zero store then verifying it can be retrieved from there with
   * preserved content.
   */
  @Test
  public void testPushPullIdentity() throws Exception {
    // We might leave local file behind if creating pulled fails, taking that risk :)
    File local = File.createTempFile("myFile", ".txt");
    File pulled = File.createTempFile("myPulledFile", ".txt");
    try {
      // Write binary data
      byte[] bytesWritten = {
        0, -1, 5, 10, 32, 127, -15, 20, 0, -100, 40, 0, 0, 0, (byte) Instant.now().toEpochMilli()
      };

      FileUtils.writeByteArrayToFile(local, bytesWritten);

      ZeroFile.WithLocal localZeroFile =
          pushStream(
              TEST_COLLECTION_NAME,
              TEST_SHARD_NAME_1,
              new ByteArrayInputStream(bytesWritten),
              local.getName(),
              ZeroMetadataController.buildFileName(local.getName(), "rAnDomSuFfIx"));

      // Now pull the file
      try (IndexInput ii = zeroStoreClient.pullStream(localZeroFile)) {
        FileOutputStream os = new FileOutputStream(pulled.getAbsolutePath());
        ZeroStoreClient.readIndexInput(ii, os);
        os.close();
      }

      // Verify pulled and local are the same
      byte[] bytesLocal = Files.readAllBytes(local.toPath());
      byte[] bytesPulled = Files.readAllBytes(pulled.toPath());

      Assert.assertArrayEquals(
          "Content pulled from Zero store should be identical to value previously pushed",
          bytesLocal,
          bytesPulled);
      Assert.assertEquals(
          "Pulled content expected same size as content initially written",
          bytesPulled.length,
          bytesWritten.length);
    } finally {
      IOUtils.deleteFilesIgnoringExceptions(local.toPath(), pulled.toPath());
    }
  }

  /** Test we can delete a Zero store file */
  @Test
  public void testDeleteZeroStoreFile() throws Exception {
    byte[] bytesWritten = {0, 1, 2, 5};

    // Create a file and push it to Zero store
    ZeroFile.WithLocal localZeroFile =
        pushStream(
            TEST_COLLECTION_NAME,
            TEST_SHARD_NAME_1,
            new ByteArrayInputStream(bytesWritten),
            "rrtr",
            "rrtr.rAnDomSuFfIx");

    // Pull it back (actual pulled content tested in pushPullIdentity(), not testing here).
    zeroStoreClient.pullStream(localZeroFile).close();

    // Delete the Zero store
    zeroStoreClient.deleteZeroFiles(List.of(localZeroFile));

    // Pull it again, this should fail as the file no longer exists
    Assert.assertTrue(pullDoesFail(zeroStoreClient, localZeroFile));
  }

  /** Test that we can push, check existence of, and pull the shard metadata from the Zero store */
  @Test
  public void testPushPullShardMetadata() throws Exception {
    zeroStoreRootPath.resolve(TEST_COLLECTION_NAME).resolve(TEST_SHARD_NAME_1).toFile().mkdirs();
    ZeroStoreShardMetadata pushedZcm = new ZeroStoreShardMetadata(19L);
    ZeroFile.WithLocal shardMetadataLocalZeroFile =
        new ZeroFile.WithLocal(
            TEST_COLLECTION_NAME, TEST_SHARD_NAME_1, "shard.metadata", "shard.metadata.xxx");
    Assert.assertNull(zeroStoreClient.pullShardMetadata(shardMetadataLocalZeroFile));

    zeroStoreClient.pushShardMetadata(shardMetadataLocalZeroFile, pushedZcm);
    Assert.assertTrue(zeroStoreClient.shardMetadataExists(shardMetadataLocalZeroFile));
    ZeroStoreShardMetadata pulledZcm =
        zeroStoreClient.pullShardMetadata(shardMetadataLocalZeroFile);

    Assert.assertEquals(pushedZcm, pulledZcm);
  }

  private boolean pullDoesFail(ZeroStoreClient zeroStoreClient, ZeroFile.WithLocal localZeroFile) {
    try {
      IndexInput ii = zeroStoreClient.pullStream(localZeroFile);
      ii.close();
      return false;
    } catch (IOException | ZeroException e) {
      // Expected exception: ZeroStoreClient should
      // throw an exception if we try to delete a file that is no longer there.
      return true;
    }
  }

  @Test
  public void testPushStreamClosesInputStream() throws Exception {
    String str = "test";
    AtomicBoolean isClosed = new AtomicBoolean();
    InputStream input =
        new ByteArrayInputStream(str.getBytes(Charset.defaultCharset())) {
          @Override
          public void close() {
            isClosed.set(true);
          }
        };
    pushStream(TEST_COLLECTION_NAME, TEST_SHARD_NAME_1, input, "x", "x.rAnDomSuFfIx");
    Assert.assertTrue(isClosed.get());
  }

  /** Test we can batch delete Zero store files */
  @Test
  public void testBatchDeleteZeroStoreFiles() throws Exception {
    byte[] bytesWritten = {0, 1, 2, 5};

    // Create some files
    ZeroFile.WithLocal localZeroFile1 =
        pushStream(
            TEST_COLLECTION_NAME,
            TEST_SHARD_NAME_1,
            new ByteArrayInputStream(bytesWritten),
            "azerty",
            "azerty.rAnDomSuFfIx");
    ZeroFile.WithLocal localZeroFile2 =
        pushStream(
            TEST_COLLECTION_NAME,
            TEST_SHARD_NAME_1,
            new ByteArrayInputStream(bytesWritten),
            "qsdf",
            "qsdf.rAnDomSuFfIx");
    ZeroFile.WithLocal localZeroFile3 =
        pushStream(
            TEST_COLLECTION_NAME,
            TEST_SHARD_NAME_1,
            new ByteArrayInputStream(bytesWritten),
            "wxcv",
            "wxcv.rAnDomSuFfIx");

    // deleteZeroFiles should still return true if the specified file does not exist
    ZeroFile.ToDelete zeroFile4 =
        new ZeroFile.ToDelete("collectionDoesNotExist", "shardDoesNotExist", "fileDoesNotExist.xx");
    Assert.assertTrue(assertDeleteSucceeds(Set.of(zeroFile4)));

    // pull them back to verify they were created (an exception will occur if we failed to push for
    // some reason)
    zeroStoreClient.pullStream(localZeroFile1).close();
    zeroStoreClient.pullStream(localZeroFile2).close();
    zeroStoreClient.pullStream(localZeroFile3).close();

    // perform a batch delete
    Assert.assertTrue(
        assertDeleteSucceeds(Set.of(localZeroFile1, localZeroFile2, localZeroFile3, zeroFile4)));

    // Pull them again, this should fail as the files no longer exists
    Assert.assertTrue(pullDoesFail(zeroStoreClient, localZeroFile1));
    Assert.assertTrue(pullDoesFail(zeroStoreClient, localZeroFile2));
    Assert.assertTrue(pullDoesFail(zeroStoreClient, localZeroFile3));
  }

  @Test
  public void testDeleteNonExistentCoreNoOp() {
    zeroStoreClient.deleteZeroFiles(
        Collections.singleton(
            new ZeroFile.ToDelete("does-not-exist", "does-not-exist", "does-not-exist.xxx")));
  }

  @Test
  public void testDeleteFiles() throws Exception {
    String shardPrefix = "empty-shard-";
    String collectionName = "collection1";

    List<String> shardNames = new ArrayList<>();
    for (int i = 0; i < 1001; i++) {
      shardNames.add(shardPrefix + i);
    }

    List<ZeroFile> zeroFiles = new ArrayList<>();
    for (String shardName : shardNames) {
      ZeroFile.WithLocal localZeroFile =
          new ZeroFile.WithLocal(collectionName, shardName, "foo", "foo.xxx");
      zeroFiles.add(localZeroFile);
      pushContent(localZeroFile, "");
    }

    int nbCoreZeroFiles =
        shardNames.stream()
            .map(
                shardName -> {
                  return zeroStoreClient.listShardZeroFiles(collectionName, shardName).size();
                })
            .mapToInt(Integer::intValue)
            .sum();

    Assert.assertEquals(1001, nbCoreZeroFiles);

    zeroStoreClient.deleteZeroFiles(zeroFiles);

    int nbCoreZeroFilesAfter =
        shardNames.stream()
            .map(
                shardName -> {
                  return zeroStoreClient.listShardZeroFiles(collectionName, shardName).size();
                })
            .mapToInt(Integer::intValue)
            .sum();

    Assert.assertEquals(0, nbCoreZeroFilesAfter);
  }

  /** Write and read a simple file (happy path). */
  @Test
  public void testBasicWriteRead() throws Exception {
    ZeroFile.WithLocal f = new ZeroFile.WithLocal("foo", "bar", "baz", "baz.xxx");
    pushContent(f, "oh my zero");

    try (IndexInput ii = zeroStoreClient.pullStream(f)) {
      OutputStream os = new ByteArrayOutputStream();
      ZeroStoreClient.readIndexInput(ii, os);
      String content = os.toString();
      os.close();
      assertEquals("oh my zero", content);
    }
  }

  /** Check writing a file with no path. */
  @Test
  public void testWriteNoPath() {
    assertThrows(
        "Should not be able to write content to an empty path",
        ZeroException.class,
        () -> pushContent(new ZeroFile.WithLocal("", "", "test", "test.xxx"), "my content"));
  }

  /** Check reading a file with no path. */
  @Test
  public void testReadNoPath() {
    assertThrows(
        "Should not be able to read content from an empty path",
        ZeroException.class,
        () -> {
          zeroStoreClient.pullStream(new ZeroFile.WithLocal("", "", "", ""));
        });
  }

  /** Test writing over an existing file and overriding the content. */
  @Test
  public void testWriteOverFile() throws Exception {
    ZeroFile.WithLocal localZeroFile =
        new ZeroFile.WithLocal("collection_override", "shard_override", "foo", "foo.xxx");
    pushContent(localZeroFile, "old content");
    pushContent(localZeroFile, "new content");

    IndexInput ii = zeroStoreClient.pullStream(localZeroFile);

    OutputStream os = new ByteArrayOutputStream();
    ZeroStoreClient.readIndexInput(ii, os);
    String content = os.toString();
    os.close();
    assertEquals("File contents should have been overridden", "new content", content);
  }

  private boolean assertDeleteSucceeds(Collection<ZeroFile> keys) {
    try {
      zeroStoreClient.deleteZeroFiles(keys);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Helper method to push a string to S3.
   *
   * @param localZeroFile Destination path in S3.
   * @param content Arbitrary content for the test.
   */
  private void pushContent(ZeroFile.WithLocal localZeroFile, String content) throws ZeroException {
    zeroStoreRootPath
        .resolve(localZeroFile.getCollectionName())
        .resolve(localZeroFile.getShardName())
        .toFile()
        .mkdirs();
    zeroStoreClient.pushStream(
        localZeroFile, new ByteArrayInputStream(content.getBytes(Charset.defaultCharset())));
  }

  private ZeroFile.WithLocal pushStream(
      String collectionName,
      String shardName,
      InputStream is,
      String solrFileName,
      String zeroFileName)
      throws ZeroException {

    try {
      zeroStoreRootPath.resolve(collectionName).resolve(shardName).toFile().mkdirs();
      ZeroFile.WithLocal localZeroFile =
          new ZeroFile.WithLocal(collectionName, shardName, solrFileName, zeroFileName);
      zeroStoreClient.pushStream(localZeroFile, is);
      return localZeroFile;
    } catch (Exception ex) {
      throw new ZeroException(ex);
    }
  }
}
