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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.commons.codec.binary.Hex;
import org.apache.lucene.store.Directory;
import org.apache.solr.zero.client.ZeroFile;

/**
 * Class capturing the differences between a local Solr core metadata {@link LocalCoreMetadata} and
 * the Zero store metadata of the corresponding shard {@link ZeroStoreShardMetadata}.
 */
public class MetadataComparisonResult {

  private final String metadataSuffix;

  /** See {@link LocalCoreMetadata#generation} */
  private final long localGeneration;

  private final long distantGeneration;

  /** Zero store files that need to be pulled */
  private final Collection<ZeroFile.WithLocal> filesToPull;

  /** Zero store files that need to be pushed */
  private final Collection<ZeroFile.WithLocal> filesToPush;

  /** Zero store files that need to be deleted */
  private final Collection<ZeroFile.WithLocal> filesToDelete;

  /**
   * True if the local index contents conflict with contents to be pulled from the Zero store. If
   * they conflict the core will be moved to a new index dir when pulling Zero store contents Two
   * cases that result in a conflict: 1. local index is at higher generation number than Zero store
   * generation number 2. index file with given name exists in both places but with a different size
   * or checksum
   */
  private final boolean localConflictingWithZero;

  /**
   * Hash of the directory content used to make sure the content doesn't change as we proceed to
   * pull new files from Zero store (if we need to pull new files from Zero store)
   */
  private final String directoryHash;

  /**
   * Result of a comparison of a local core and the Zero store version of the corresponding shard to
   * decide how to align the two. Used when the goal is to pull newer content from the Zero store.
   *
   * @param localConflictingWithZero when true, some local files and Zero store files conflict.
   *     After pulling, the core will be switched to a new directory.
   */
  public MetadataComparisonResult(
      long localGeneration,
      long distantGeneration,
      String directoryHash,
      boolean localConflictingWithZero,
      Collection<ZeroFile.WithLocal> filesToPull) {
    this.localGeneration = localGeneration;
    this.distantGeneration = distantGeneration;
    this.directoryHash = directoryHash;
    this.filesToPush = Collections.emptySet();
    this.filesToDelete = Collections.emptySet();
    this.filesToPull = filesToPull;
    this.localConflictingWithZero = localConflictingWithZero;
    this.metadataSuffix = null;
  }

  /**
   * Result of a comparison of a local core and the Zero store version of the corresponding shard to
   * decide how to align the two. Used when the goal is to push local updates to the Zero store.
   */
  public MetadataComparisonResult(
      long localGeneration,
      long distantGeneration,
      String directoryHash,
      String metadataSuffix,
      Collection<ZeroFile.WithLocal> filesToPush,
      Collection<ZeroFile.WithLocal> filesToDelete) {
    this.localGeneration = localGeneration;
    this.distantGeneration = distantGeneration;
    this.directoryHash = directoryHash;
    this.metadataSuffix = metadataSuffix;
    this.filesToPush = filesToPush;
    this.filesToDelete = filesToDelete;
    this.filesToPull = Collections.emptySet();
    this.localConflictingWithZero = false;
  }

  public Collection<ZeroFile.WithLocal> getFilesToPush() {
    return filesToPush;
  }

  public Collection<ZeroFile.WithLocal> getFilesToPull() {
    return filesToPull;
  }

  public Collection<ZeroFile.WithLocal> getFilesToDelete() {
    return filesToDelete;
  }

  public boolean isLocalConflictingWithZero() {
    return localConflictingWithZero;
  }

  public long getLocalGeneration() {
    return this.localGeneration;
  }

  public long getDistantGeneration() {
    return this.distantGeneration;
  }

  public String getMetadataSuffix() {
    return metadataSuffix;
  }

  /**
   * Returns {@code true} if the contents of the directory passed into this method is identical to
   * the contents of the directory of the Solr core of this instance, taken at instance creation
   * time. If the directory hash was not computed at the instance creation time, then we throw an
   * IllegalStateException indicating a programming error.
   *
   * <p>Passing in the Directory (expected to be the directory of the same core used during
   * construction) because it seems safer than trying to get it again here...
   *
   * @throws IllegalStateException if this instance was not created with a computed directoryHash
   */
  public boolean isSameDirectoryContent(Directory coreDir)
      throws NoSuchAlgorithmException, IOException {
    if (directoryHash == null) {
      throw new IllegalStateException("Directory hash was not computed for the given core");
    }
    return directoryHash.equals(getSolrDirectoryHash(coreDir, coreDir.listAll()));
  }

  /**
   * Computes a hash of a Solr Directory in order to make sure the directory doesn't change as we
   * pull content into it (if we need to pull content into it)
   */
  private String getSolrDirectoryHash(Directory coreDir, String[] filesNames)
      throws NoSuchAlgorithmException, IOException {
    MessageDigest digest =
        MessageDigest.getInstance("sha1"); // not sure MD5 is available in Solr jars

    // Computing the hash requires items to be submitted in the same order...
    Arrays.sort(filesNames);

    for (String fileName : filesNames) {
      // .lock files come and go. Ignore them (we're closing the Index Writer before adding any
      // pulled files to the Core)
      if (!fileName.endsWith(".lock")) {
        // Hash the file name and file size so we can tell if any file has changed (or files
        // appeared or vanished)
        digest.update(fileName.getBytes(StandardCharsets.UTF_8));
        try {
          digest.update(
              Long.toString(coreDir.fileLength(fileName)).getBytes(StandardCharsets.UTF_8));
        } catch (FileNotFoundException | NoSuchFileException fnf) {
          // The file was deleted between the listAll() and the check, use an impossible size to not
          // match a digest
          // for which the file is completely present or completely absent (which will cause this
          // hash to never match that directory again).
          digest.update(Long.toString(-42).getBytes(StandardCharsets.UTF_8));
        }
      }
    }

    return new String(Hex.encodeHex(digest.digest()));
  }
}
