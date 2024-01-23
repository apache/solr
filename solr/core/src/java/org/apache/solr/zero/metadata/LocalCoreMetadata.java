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
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.codec.binary.Hex;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.zero.exception.ZeroException;
import org.apache.solr.zero.process.CorePuller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Object capturing the metadata of a Solr core on a Solr node. */
public class LocalCoreMetadata {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * When an instance of this class is created for pushing data to the Zero store, reserve the
   * commit point for a short while to give the caller time to save it while it works on it. Can't
   * save it here directly as it would be awkward to try to release it on all execution paths.
   */
  private static final long RESERVE_COMMIT_DURATION = TimeUnit.SECONDS.toMillis(1L);

  private static final int MAX_ATTEMPTS_TO_CAPTURE_COMMIT_POINT = 5;

  /**
   * Files composing the core. They are referenced from the core's current commit point's segments_N
   * file which is ALSO included in this collection.
   */
  private Collection<String> latestCommitFiles;

  /**
   * Names of all files (all of them, no exception) in the local index directory. These files do not
   * matter when pushing contents to Zeo store but they do matter if Zero store content being pulled
   * conflicts with them.
   */
  private Set<String> allFiles;

  /**
   * Hash of the directory content used to make sure the content doesn't change as we proceed to
   * pull new files from the Zero store (if we need to pull new files from there)
   */
  private String directoryHash;

  /**
   * Generation number of the local index. This generation number is only meant to identify a
   * scenario where local index generation number is higher than what we have in Zero store. In that
   * scenario we would switch index to a new directory when pulling contents from the Zero store.
   * Because in the presence of higher generation number locally, simply pulling the content of the
   * Zero store for the core does not work (Solr favors the highest generation locally...). It is
   * also used for saving (reserving) local commit while doing pushes.
   */
  private long generation;

  protected final SolrCore core;

  /** Given a core name, builds the local metadata */
  public LocalCoreMetadata(SolrCore core) {

    this.core = core;
  }

  public void readMetadata(boolean reserveCommit, boolean captureDirHash) throws Exception {

    Directory coreDir = getCoreDirectory();
    if (coreDir == null) return;

    try {
      Set<String> latestCommitBuilder;
      IndexCommit latestCommit;
      int attempt = 1;
      // we don't have an atomic way of capturing a commit point or taking a snapshot of the
      // entire index directory to compute a directory hash. In the commit point case, there
      // is a slight chance of losing files between getting a latest commit and reserving it.
      // Likewise, there is a chance of losing files between the time we list all file names
      // in the directory and computing the directory hash. Therefore, we try to capture the
      // commit point and compute the directory hash in a loop with maximum number of attempts.
      String hash = null;
      String[] fileNames;
      while (true) {
        try {
          // Work around possible bug returning same file multiple times by using a set here
          // See org.apache.solr.handler.ReplicationHandler.getFileList()
          latestCommitBuilder = new HashSet<>();
          latestCommit = tryCapturingLatestCommit(latestCommitBuilder, reserveCommit);

          // Capture now the hash and verify again after files have been pulled and before the
          // directory is updated (or before the index is switched to use a new directory) to
          // make sure there are no local changes at the same time that might lead to a
          // corruption in case of interaction with the download or might be a sign of other
          // problems (it is not expected that indexing can happen on a local directory of a
          // ZERO replica if that replica is not up-to-date with the Zero store version).
          fileNames = coreDir.listAll();
          if (captureDirHash) {
            hash = getSolrDirectoryHash(coreDir, fileNames);
          }
          break;
        } catch (FileNotFoundException | NoSuchFileException ex) {
          attempt++;
          if (attempt > MAX_ATTEMPTS_TO_CAPTURE_COMMIT_POINT) {
            throw ex;
          }
          String reason = ex.getMessage();
          log.error(
              "Failed to capture directory snapshot for either commit point or entire directory listing: core={} attempt={} reason={}",
              getCoreName(),
              attempt,
              reason);
        }
      }

      generation = latestCommit.getGeneration();
      latestCommitFiles = Collections.unmodifiableSet(latestCommitBuilder);
      directoryHash = hash;

      // Need to inventory all local files in case files that need to be pulled from Zero store
      // conflict
      // with them.
      allFiles = Set.of(fileNames);
    } finally {
      releaseCoreDirectory(coreDir);
    }
  }

  private IndexCommit tryCapturingLatestCommit(
      Set<String> latestCommitBuilder, boolean reserveCommit) throws ZeroException, IOException {
    IndexDeletionPolicyWrapper deletionPolicy = core.getDeletionPolicy();
    IndexCommit latestCommit = deletionPolicy.getLatestCommit();
    if (latestCommit == null) {
      throw new ZeroException("Core " + getCoreName() + " has no available commit point");
    }

    if (reserveCommit) {
      // Caller will save the commit point shortly. See CorePushPull.pushFilesToZeroStore()
      deletionPolicy.setReserveDuration(latestCommit.getGeneration(), RESERVE_COMMIT_DURATION);
    }
    // Note we add here all segment related files as well as the commit point's segments_N file
    // Commit points do not contain lock (write.lock) files.
    latestCommitBuilder.addAll(latestCommit.getFileNames());
    return latestCommit;
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

  protected String getCoreName() {
    return this.core.getName();
  }

  protected String getCollectionName() {
    return core.getCoreDescriptor().getCollectionName();
  }

  protected String getShardName() {
    return core.getCoreDescriptor().getCloudDescriptor().getShardId();
  }

  public long getGeneration() {
    return this.generation;
  }

  public String getDirectoryHash() {
    return directoryHash;
  }

  public Set<String> getAllFiles() {
    return allFiles;
  }

  public Collection<String> getLatestCommitFiles() {
    return latestCommitFiles;
  }

  @Override
  public String toString() {
    return "collectionName="
        + getCollectionName()
        + " shardName="
        + getShardName()
        + " coreName="
        + getCoreName()
        + " generation="
        + generation;
  }

  public Directory getCoreDirectory() throws IOException {
    return CorePuller.getDirectory(core, core.getIndexDir());
  }

  public void releaseCoreDirectory(Directory coreDir) throws IOException {
    if (coreDir != null) core.getDirectoryFactory().release(coreDir);
  }
}
