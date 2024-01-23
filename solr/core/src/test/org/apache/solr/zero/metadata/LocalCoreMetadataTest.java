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
package org.apache.solr.zero.metadata;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.store.Directory;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.zero.client.ZeroFile;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link LocalCoreMetadata}. */
public class LocalCoreMetadataTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void setupTests() {
    assumeWorkingMockito();
  }

  /**
   * Tests that resolve returns a result with files to push when a null {@link
   * ZeroStoreShardMetadata} is passed in but {@link LocalCoreMetadata} is present
   */
  @Test
  public void testPushNeededWhenZCMNull() throws Exception {
    final long localFileSize = 10;
    final long localFileChecksum = 100;

    ZeroFile.WithLocal latestCommitFile =
        newZeroFileWithLocal(getSolrSegmentFileName("1"), localFileSize, localFileChecksum);

    // local core metadata
    final MetadataComparisonResult metadataComparisonResult =
        new MetadataComparisonResultBuilder(new ZeroStoreShardMetadata(), 1L)
            .addLatestCommitFile(latestCommitFile)
            .build(true);

    // do resolution
    assertZeroStoreResolutionResult(
        List.of(latestCommitFile), Collections.emptySet(), false, metadataComparisonResult);
  }

  /**
   * Test resolve of both {@link ZeroStoreShardMetadata} and {@link LocalCoreMetadata} contains
   * files to push only
   */
  @Test
  public void testFilesToPushResolution() throws Exception {
    final long fileSize = 10;
    final long checksum = 100;

    ZeroFile.WithLocal expectedFileToPush =
        newZeroFileWithLocal(getSolrFileName("1"), fileSize, checksum);

    ZeroFile.WithLocal solrSegment =
        newZeroFileWithLocal(getSolrSegmentFileName("1"), fileSize, checksum);

    final ZeroStoreShardMetadata zeroMetadata = new ZeroStoreShardMetadata(1L);
    zeroMetadata.addFile(solrSegment);

    // do resolution
    final MetadataComparisonResult metadataComparisonResult =
        new MetadataComparisonResultBuilder(zeroMetadata, 1L)
            .addLatestCommitFile(solrSegment)
            .addLatestCommitFile(expectedFileToPush)
            .build(true);

    assertZeroStoreResolutionResult(
        List.of(expectedFileToPush), Collections.emptySet(), false, metadataComparisonResult);
  }

  /**
   * Test resolve of both {@link ZeroStoreShardMetadata} and {@link LocalCoreMetadata} contains
   * files to pull only
   */
  @Test
  public void testFilesToPullResolution() throws Exception {
    final long fileSize = 10;
    final long checksum = 100;

    ZeroFile.WithLocal expectedFileToPull =
        newZeroFileWithLocal(getSolrFileName("1"), fileSize, checksum);

    final ZeroStoreShardMetadata zeroMetadata = new ZeroStoreShardMetadata(1L);
    zeroMetadata.addFile(newZeroFileWithLocal(getSolrSegmentFileName("1"), fileSize, checksum));
    zeroMetadata.addFile(expectedFileToPull);

    // do resolution
    final MetadataComparisonResult metadataComparisonResult =
        new MetadataComparisonResultBuilder(zeroMetadata, 1L)
            .addLatestCommitFile(
                newZeroFileWithLocal(getSolrSegmentFileName("1"), fileSize, checksum))
            .build(false);

    assertZeroStoreResolutionResult(
        Collections.emptySet(), List.of(expectedFileToPull), false, metadataComparisonResult);
  }

  /**
   * Test resolve of both {@link ZeroStoreShardMetadata} and {@link LocalCoreMetadata} contains both
   * files to push and pull
   */
  @Test
  public void testFilesToPushPullResolution() throws Exception {
    final long fileSize = 10;
    final long checksum = 100;

    ZeroFile.WithLocal expectedFileToPush =
        newZeroFileWithLocal(getSolrFileName("3"), fileSize, checksum);
    ZeroFile.WithLocal expectedFileToPull =
        newZeroFileWithLocal(getSolrFileName("2"), fileSize, checksum);
    final ZeroStoreShardMetadata zeroMetadata = new ZeroStoreShardMetadata(1L);
    zeroMetadata.addFile(newZeroFileWithLocal(getSolrSegmentFileName("1"), fileSize, checksum));
    zeroMetadata.addFile(expectedFileToPull);

    // do resolution
    final MetadataComparisonResultBuilder serverMetadataBuilder =
        new MetadataComparisonResultBuilder(zeroMetadata, 1L)
            .addLatestCommitFile(
                newZeroFileWithLocal(getSolrSegmentFileName("1"), fileSize, checksum))
            .addLatestCommitFile(expectedFileToPush);

    final MetadataComparisonResult metadataComparisonResultPush = serverMetadataBuilder.build(true);
    assertZeroStoreResolutionResult(
        List.of(expectedFileToPush), Collections.emptySet(), false, metadataComparisonResultPush);

    final MetadataComparisonResult metadataComparisonResultPull =
        serverMetadataBuilder.build(false);
    assertZeroStoreResolutionResult(
        Collections.emptySet(), List.of(expectedFileToPull), false, metadataComparisonResultPull);
  }

  /** Tests that local generation number being higher than Zero store's resolves into a conflict */
  @Test
  public void testHigherLocalGenerationResolution() throws Exception {
    final long fileSize = 10;
    final long checksum = 100;

    ZeroFile.WithLocal expectedFileToPush =
        newZeroFileWithLocal(getSolrSegmentFileName("2"), fileSize, checksum);

    final ZeroStoreShardMetadata zeroMetadata = new ZeroStoreShardMetadata(1L);
    zeroMetadata.addFile(newZeroFileWithLocal(getSolrSegmentFileName("1"), fileSize, checksum));
    zeroMetadata.addFile(newZeroFileWithLocal(getSolrFileName("1"), fileSize, checksum));

    // do resolution
    final MetadataComparisonResultBuilder serverMetadataBuilder =
        new MetadataComparisonResultBuilder(zeroMetadata, 2L)
            .addLatestCommitFile(expectedFileToPush)
            .addLatestCommitFile(newZeroFileWithLocal(getSolrFileName("1"), fileSize, checksum));

    final MetadataComparisonResult metadataComparisonResultPush = serverMetadataBuilder.build(true);

    assertZeroStoreResolutionResult(
        List.of(expectedFileToPush), Collections.emptySet(), false, metadataComparisonResultPush);

    final MetadataComparisonResult metadataComparisonResultPull =
        serverMetadataBuilder.build(false);

    assertZeroStoreResolutionResult(
        Collections.emptySet(), zeroMetadata.getZeroFiles(), true, metadataComparisonResultPull);
  }

  /**
   * Tests that file belonging to latest commit point being present both locally and in Zero store
   * but with different checksum resolves into a conflict
   */
  @Test
  public void testFileFromLatestCommitConflictsResolution() throws Exception {
    final long fileSize = 10;

    final ZeroStoreShardMetadata zeroMetadata = new ZeroStoreShardMetadata(1L);
    zeroMetadata.addFile(newZeroFileWithLocal(getSolrSegmentFileName("1"), fileSize, 100));

    // do resolution
    final MetadataComparisonResult metadataComparisonResult =
        new MetadataComparisonResultBuilder(zeroMetadata, 1L)
            .addLatestCommitFile(newZeroFileWithLocal(getSolrSegmentFileName("1"), fileSize, 99))
            .build(false);

    assertZeroStoreResolutionResult(
        Collections.emptySet(), zeroMetadata.getZeroFiles(), true, metadataComparisonResult);
  }

  /**
   * Tests that a file not in current commit point being present locally and a similarly named one
   * in Zero store resolves into a conflict
   */
  @Test
  public void testFileLocalNotInCommitConflictsResolution() throws Exception {
    final long fileSize = 10;
    final long checksum = 100;

    ZeroFile.WithLocal expectedFileToPush =
        newZeroFileWithLocal(getSolrSegmentFileName("2"), fileSize, checksum);

    final String conflictingFileSolrName = "randomFileName";

    final ZeroStoreShardMetadata zeroMetadata = new ZeroStoreShardMetadata(3L);
    zeroMetadata.addFile(newZeroFileWithLocal(getSolrSegmentFileName("3"), fileSize, checksum));
    zeroMetadata.addFile(newZeroFileWithLocal(conflictingFileSolrName, fileSize, 100));

    // do resolution
    final MetadataComparisonResultBuilder serverMetadataBuilder =
        new MetadataComparisonResultBuilder(zeroMetadata, 2L)
            .addLatestCommitFile(expectedFileToPush)
            .addLocalFile(conflictingFileSolrName);

    final MetadataComparisonResult metadataComparisonResultPush = serverMetadataBuilder.build(true);
    assertZeroStoreResolutionResult(
        List.of(expectedFileToPush), Collections.emptySet(), false, metadataComparisonResultPush);

    final MetadataComparisonResult metadataComparisonResultPull =
        serverMetadataBuilder.build(false);
    assertZeroStoreResolutionResult(
        Collections.emptySet(), zeroMetadata.getZeroFiles(), true, metadataComparisonResultPull);
  }

  private static void assertZeroStoreResolutionResult(
      Collection<ZeroFile.WithLocal> expectedFilesToPush,
      Collection<ZeroFile.WithLocal> expectedFilesToPull,
      boolean expectedLocalConflictingWithZero,
      MetadataComparisonResult actual) {
    assertCollections("filesToPull", expectedFilesToPull, actual.getFilesToPull());
    // assertCollections("filesToPush", expectedFilesToPush, actual.getFilesToPush());

    assertCollections(
        "filesToPush",
        expectedFilesToPush.stream()
            .map(ZeroFile.WithLocal::getSolrFileName)
            .collect(Collectors.toSet()),
        actual.getFilesToPush().stream()
            .map(ZeroFile.WithLocal::getSolrFileName)
            .collect(Collectors.toSet()));
    assertCollections(
        "filesToPush",
        expectedFilesToPush.stream()
            .map(ZeroFile.WithLocal::getFileSize)
            .collect(Collectors.toSet()),
        actual.getFilesToPush().stream()
            .map(ZeroFile.WithLocal::getFileSize)
            .collect(Collectors.toSet()));
    assertCollections(
        "filesToPush",
        expectedFilesToPush.stream()
            .map(ZeroFile.WithLocal::getChecksum)
            .collect(Collectors.toSet()),
        actual.getFilesToPush().stream()
            .map(ZeroFile.WithLocal::getChecksum)
            .collect(Collectors.toSet()));

    assertEquals(
        "localConflictingWithZero",
        expectedLocalConflictingWithZero,
        actual.isLocalConflictingWithZero());
  }

  /** Creates a segment file name. */
  private String getSolrSegmentFileName(String suffix) {
    return "segments_" + suffix;
  }

  /** Creates a solr file name. */
  private String getSolrFileName(String suffix) {
    return "_" + suffix + ".cfs";
  }

  /**
   * Asserts that two collection are same according to {@link Set#equals(Object)} semantics i.e.
   * same size and same elements irrespective of order.
   */
  private static <T> void assertCollections(
      String elementType, Collection<T> expected, Collection<T> actual) {
    assertEquals("Wrong number of " + elementType, expected.size(), actual.size());
    assertEquals(elementType + " are not same", new HashSet<>(expected), new HashSet<>(actual));
  }

  private ZeroFile.WithLocal newZeroFileWithLocal(
      String solrFileName, long fileSize, long checksum) {
    return new ZeroFile.WithLocal(
        "collectionName",
        "shardName",
        solrFileName,
        ZeroMetadataController.buildFileName(
            solrFileName, ZeroStoreShardMetadata.generateMetadataSuffix()),
        fileSize,
        checksum);
  }

  /** A builder for {@link LocalCoreMetadata} that uses mocking */
  private static class MetadataComparisonResultBuilder {
    private final long generation;
    private final Set<String> latestCommitFiles;
    private final Map<String, ZeroFile.WithLocal> fullFilesDescription;

    private final Set<String> allLocalFiles;
    private final ZeroStoreShardMetadata shardMetadata;

    MetadataComparisonResultBuilder(ZeroStoreShardMetadata shardMetadata, Long generation) {
      this.generation = generation;
      latestCommitFiles = new HashSet<>();
      allLocalFiles = new HashSet<>();
      fullFilesDescription = new HashMap<>();
      this.shardMetadata = shardMetadata;
    }

    MetadataComparisonResultBuilder addLatestCommitFile(ZeroFile.WithLocal file) {
      this.latestCommitFiles.add(file.getSolrFileName());
      addLocalFile(file.getSolrFileName());
      fullFilesDescription.put(file.getSolrFileName(), file);
      return this;
    }

    MetadataComparisonResultBuilder addLocalFile(String filename) {
      this.allLocalFiles.add(filename);
      return this;
    }

    public MetadataComparisonResult build(boolean forPush) throws Exception {
      LocalCoreMetadata localCoreMetadata =
          new LocalCoreMetadata(null) {
            @Override
            public String getCoreName() {
              return "coreName";
            }

            @Override
            public String getCollectionName() {
              return "collectionName";
            }

            @Override
            public String getShardName() {
              return "shadName";
            }

            @Override
            public long getGeneration() {
              return generation;
            }

            @Override
            public Collection<String> getLatestCommitFiles() {
              return Collections.unmodifiableSet(latestCommitFiles);
            }

            @Override
            public Set<String> getAllFiles() {
              return Collections.unmodifiableSet(allLocalFiles);
            }

            @Override
            public Directory getCoreDirectory() {
              return null;
            }
          };
      ZeroMetadataController metadataController =
          new ZeroMetadataController(null) {
            @Override
            protected ZeroFile.WithLocal getNewZeroLocalFile(
                Directory coreDir,
                String fileName,
                String collectionName,
                String shardName,
                String suffix) {
              return fullFilesDescription.get(fileName);
            }

            @Override
            protected boolean areDistantAndLocalFileDifferent(
                Directory coreDir,
                ZeroFile.WithLocal distantFile,
                String localFileName,
                String coreName) {
              ZeroFile.WithLocal localFile = fullFilesDescription.get(localFileName);
              return (localFile.getFileSize() != distantFile.getFileSize()
                  || localFile.getChecksum() != distantFile.getChecksum());
            }
          };

      if (forPush) {
        localCoreMetadata.readMetadata(true, false);
        return metadataController.diffMetadataForPush(
            localCoreMetadata, shardMetadata, ZeroStoreShardMetadata.generateMetadataSuffix());
      } else {
        localCoreMetadata.readMetadata(false, true);
        return metadataController.diffMetadataforPull(localCoreMetadata, shardMetadata);
      }
    }
  }
}
