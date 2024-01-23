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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.client.solrj.cloud.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.BadVersionException;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.VersionedData;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.zero.client.ZeroFile;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class that manages metadata for Zero Index-based collections in Solr Cloud and ZooKeeper. */
public class ZeroMetadataController {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * This is the initial value written to ZooKeeper for the {@link #SUFFIX_NODE_NAME} for a shard.
   * See also {@link MetadataCacheManager#METADATA_SUFFIX_CACHE_INITIAL_VALUE} used as the initial
   * value in the cache but not written to ZK.
   */
  public static final String METADATA_NODE_DEFAULT_VALUE = "DeFauLT";

  public static final String SUFFIX_NODE_NAME = "metadataSuffix";

  /**
   * This file captures the shard metadata stored in the Zero store with each shard. When present
   * locally (before being pushed to the Zero store), that file represents the metadata of the local
   * core. Core vs shard, there is some naming ambiguity but eventually the two mean the same thing
   * in the context of this file.
   */
  public static final String SHARD_METADATA_ZERO_FILENAME = "shard.metadata";

  private static final String SEGMENTS_N_PREFIX = "segments_";

  private final SolrCloudManager cloudManager;

  public ZeroMetadataController(SolrCloudManager cloudManager) {
    this.cloudManager = cloudManager;
  }

  public boolean hasDefaultNodeSuffix(ZeroMetadataVersion shardMetadataVersion) {
    return METADATA_NODE_DEFAULT_VALUE.equals(shardMetadataVersion.getMetadataSuffix());
  }

  public boolean hasDefaultNodeSuffix(MetadataCacheManager.MetadataCacheEntry coreMetadata) {
    return METADATA_NODE_DEFAULT_VALUE.equals(
        coreMetadata.getMetadataVersion().getMetadataSuffix());
  }

  /**
   * Creates a new metadataSuffix node if it doesn't exist for the shard and overwrites it if it
   * does. This is only used when creating a shard, not when updating it (after indexing)
   */
  public void createMetadataNode(String collectionName, String shardName) {
    ClusterState clusterState = cloudManager.getClusterStateProvider().getClusterState();
    DocCollection collection = clusterState.getCollection(collectionName);
    if (!collection.isZeroIndex()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Can't create a metadataNode for collection "
              + collectionName
              + " that is not"
              + " of type Zero for shard "
              + shardName);
    }

    String metadataPath = getMetadataBasePath(collectionName, shardName) + "/" + SUFFIX_NODE_NAME;
    Map<String, Object> nodeProps = new HashMap<>();
    nodeProps.put(SUFFIX_NODE_NAME, METADATA_NODE_DEFAULT_VALUE);

    createPersistentNode(metadataPath, Utils.toJSON(nodeProps));
  }

  /**
   * If the update is successful, the returned {@link ZeroMetadataVersion} will contain the new
   * version as well as the value of the data just written.
   *
   * @param value the value to be written to ZooKeeper
   * @param version the ZooKeeper node version to conditionally update on. Except in tests, this is
   *     never -1 (that allows the update to succeed regardless of existing node version)
   */
  public ZeroMetadataVersion updateMetadataValueWithVersion(
      String collectionName, String shardName, String value, int version) {
    String metadataPath = getMetadataBasePath(collectionName, shardName) + "/" + SUFFIX_NODE_NAME;
    try {
      Map<String, Object> nodeProps = new HashMap<>();
      nodeProps.put(SUFFIX_NODE_NAME, value);

      VersionedData data =
          cloudManager
              .getDistribStateManager()
              .setAndGetResult(metadataPath, Utils.toJSON(nodeProps), version);
      Map<?, ?> nodeUserData = (Map<?, ?>) Utils.fromJSON(data.getData());
      String metadataSuffix = (String) nodeUserData.get(ZeroMetadataController.SUFFIX_NODE_NAME);
      return new ZeroMetadataVersion(metadataSuffix, data.getVersion());
    } catch (BadVersionException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Error updating path: " + metadataPath + " due to mismatching versions",
          e);
    } catch (NoSuchElementException | KeeperException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Error updating path: " + metadataPath + " in ZooKeeper",
          e);
    }
  }

  /** Reads the {@link ZeroMetadataVersion} for the shard from ZooKeeper. */
  public ZeroMetadataVersion readMetadataValue(String collectionName, String shardName) {
    String metadataPath = getMetadataBasePath(collectionName, shardName) + "/" + SUFFIX_NODE_NAME;
    try {
      VersionedData data = cloudManager.getDistribStateManager().getData(metadataPath, null);
      Map<?, ?> nodeUserData = (Map<?, ?>) Utils.fromJSON(data.getData());
      String metadataSuffix = (String) nodeUserData.get(ZeroMetadataController.SUFFIX_NODE_NAME);
      return new ZeroMetadataVersion(metadataSuffix, data.getVersion());
    } catch (IOException | NoSuchElementException | KeeperException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Error reading data from path: " + metadataPath + " in ZooKeeper",
          e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Error reading data from path: " + metadataPath + " in ZooKeeper",
          e);
    }
  }

  /** Removes the metadata node on zookeeper for the given collection and shard name. */
  public void cleanUpMetadataNodes(String collectionName, String shardName) {
    String metadataPath = getMetadataBasePath(collectionName, shardName) + "/" + SUFFIX_NODE_NAME;
    try {
      cloudManager.getDistribStateManager().removeRecursively(metadataPath, true, true);
    } catch (Exception e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Error deleting path " + metadataPath + " in Zookeeper",
          e);
    }
  }

  private void createPersistentNode(String path, byte[] data) {
    try {
      // if path already exists, overwrite it
      cloudManager.getDistribStateManager().makePath(path, data, CreateMode.PERSISTENT, false);
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Error creating path " + path + " in Zookeeper", e);
    } catch (IOException | AlreadyExistsException | KeeperException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Error creating path " + path + " in Zookeeper", e);
    }
  }

  protected String getMetadataBasePath(String collectionName, String shardName) {
    return ZkStateReader.COLLECTIONS_ZKNODE
        + "/"
        + collectionName
        + "/"
        + ZkStateReader.SHARD_LEADERS_ZKNODE
        + "/"
        + shardName;
  }

  public MetadataComparisonResult diffMetadataForPush(
      LocalCoreMetadata localMetadata, ZeroStoreShardMetadata distantMetadata, String suffix)
      throws Exception {
    Directory coreDir = localMetadata.getCoreDirectory();
    try {
      checkSegmentFilesIntegrity(localMetadata, distantMetadata);
      return diffMetadataForPush(coreDir, localMetadata, distantMetadata, suffix);
    } finally {
      localMetadata.releaseCoreDirectory(coreDir);
    }
  }

  public MetadataComparisonResult diffMetadataforPull(
      LocalCoreMetadata localMetadata, ZeroStoreShardMetadata distantMetadata) throws Exception {
    Directory coreDir = localMetadata.getCoreDirectory();
    try {
      checkSegmentFilesIntegrity(localMetadata, distantMetadata);
      return diffMetadataForPull(coreDir, localMetadata, distantMetadata);
    } finally {
      localMetadata.releaseCoreDirectory(coreDir);
    }
  }

  private MetadataComparisonResult diffMetadataForPull(
      Directory coreDir, LocalCoreMetadata localMetadata, ZeroStoreShardMetadata distantMetadata) {

    boolean localConflictingWithZero = false;

    Map<String, ZeroFile.WithLocal> zeroFilesMissingLocally =
        getZeroFilesMissingLocally(localMetadata, distantMetadata);
    Map<String, ZeroFile.WithLocal> latestCommitFilesAlreadyOnZeroButDifferent =
        getLatestCommitFilesAlreadyOnZeroButDifferent(coreDir, localMetadata, distantMetadata);
    if (!latestCommitFilesAlreadyOnZeroButDifferent.isEmpty()) {
      localConflictingWithZero = true;
      zeroFilesMissingLocally = distantMetadata.getZeroFilesAsMap();
    }

    Map<String, ZeroFile.WithLocal> filesOnBothSideButNotInLatestCommit =
        getFilesOnBothSideButNotInLatestCommit(localMetadata, zeroFilesMissingLocally);
    if (!filesOnBothSideButNotInLatestCommit.isEmpty()) {
      // We are resolving metadata for a PULL, it means we expect the local core to be behind or
      // at the Zero version. We therefore do not expect local files not in the commit point that
      // are still in the Zero store commit point. It might mean the local core did some indexing
      // and merged segments then dropped from the commit point.
      //
      // This can happen if the core was the leader, indexed locally but then failed to push to the
      // Zero store, and is now pulling again after another replica did a push. In these case we do
      // want to align on the Zero store version.
      localConflictingWithZero = true;
      zeroFilesMissingLocally = distantMetadata.getZeroFilesAsMap();

      filesOnBothSideButNotInLatestCommit
          .values()
          .forEach(
              bf -> {
                if (log.isInfoEnabled()) {
                  log.info(
                      "File exists locally outside of current commit point. collectionName={} shardName={}"
                          + " coreName={}  solrFileName={} zeroFileName={}",
                      localMetadata.getCollectionName(),
                      localMetadata.getShardName(),
                      localMetadata.getCoreName(),
                      bf.getSolrFileName(),
                      bf.getZeroFileName());
                }
              });
    }

    // If local index generation is higher than Zero's we will download to new directory because
    // if both segments_N files are present in same directory Solr will open the higher one (and
    // we really want the lower one from the Zero store as we're currently pulling content from
    // there).
    if (!distantMetadata.getZeroFiles().isEmpty()
        && localMetadata.getGeneration() > distantMetadata.getGeneration()) {
      localConflictingWithZero = true;
      zeroFilesMissingLocally = distantMetadata.getZeroFilesAsMap();
      if (log.isInfoEnabled()) {
        log.info(
            "local generation higher than Zero store.  coreName={} localGeneration={} zeroGeneration={}",
            localMetadata.getCoreName(),
            localMetadata.getGeneration(),
            distantMetadata.getGeneration());
      }
    }

    return new MetadataComparisonResult(
        localMetadata.getGeneration(),
        distantMetadata.getGeneration(),
        localMetadata.getDirectoryHash(),
        localConflictingWithZero,
        zeroFilesMissingLocally.values());
  }

  private MetadataComparisonResult diffMetadataForPush(
      Directory coreDir,
      LocalCoreMetadata localMetadata,
      ZeroStoreShardMetadata distantMetadata,
      String newSuffix) {

    Collection<ZeroFile.WithLocal> filesToPush =
        getLocalFilesMissingOnZero(localMetadata, distantMetadata).stream()
            .map(
                f ->
                    getNewZeroLocalFile(
                        coreDir,
                        f,
                        localMetadata.getCollectionName(),
                        localMetadata.getShardName(),
                        newSuffix))
            .collect(Collectors.toSet());
    Collection<ZeroFile.WithLocal> filesToDelete =
        getZeroFilesMissingLocally(localMetadata, distantMetadata).values();
    return new MetadataComparisonResult(
        localMetadata.getGeneration(),
        distantMetadata.getGeneration(),
        localMetadata.getDirectoryHash(),
        newSuffix,
        filesToPush,
        filesToDelete);
  }

  /**
   * @return intersection(all local files, distant files - latest committed files)
   */
  private Map<String, ZeroFile.WithLocal> getFilesOnBothSideButNotInLatestCommit(
      LocalCoreMetadata localMetadata, Map<String, ZeroFile.WithLocal> zeroFilesMissingLocally) {

    Map<String, ZeroFile.WithLocal> filesOnBothSideButNotInLatestCommit =
        new HashMap<>(zeroFilesMissingLocally);
    filesOnBothSideButNotInLatestCommit.keySet().retainAll(localMetadata.getAllFiles());
    return filesOnBothSideButNotInLatestCommit;
  }

  /**
   * @return intersection(distant files, latest committed files).filter(size or checksum different)
   */
  private Map<String, ZeroFile.WithLocal> getLatestCommitFilesAlreadyOnZeroButDifferent(
      Directory coreDir, LocalCoreMetadata localMetadata, ZeroStoreShardMetadata distantZeroFiles) {
    Map<String, ZeroFile.WithLocal> latestCommitFilesAlreadyOnZero =
        distantZeroFiles.getZeroFilesAsMap();
    latestCommitFilesAlreadyOnZero.keySet().retainAll(localMetadata.getLatestCommitFiles());
    return latestCommitFilesAlreadyOnZero.entrySet().stream()
        .filter(
            f ->
                areDistantAndLocalFileDifferent(
                    coreDir, f.getValue(), f.getKey(), localMetadata.getCoreName()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * @return distant files - latest committed files
   */
  private Map<String, ZeroFile.WithLocal> getZeroFilesMissingLocally(
      LocalCoreMetadata localMetadata, ZeroStoreShardMetadata distantZeroFiles) {

    Map<String, ZeroFile.WithLocal> zeroFilesMissingLocally = distantZeroFiles.getZeroFilesAsMap();
    zeroFilesMissingLocally.keySet().removeAll(localMetadata.getLatestCommitFiles());
    return zeroFilesMissingLocally;
  }

  /**
   * @return latest committed files - distant files
   */
  private Set<String> getLocalFilesMissingOnZero(
      LocalCoreMetadata localMetadata, ZeroStoreShardMetadata distantZeroFiles) {
    Set<String> localFileNamesMissingOnZero = new HashSet<>(localMetadata.getLatestCommitFiles());
    localFileNamesMissingOnZero.removeAll(distantZeroFiles.getZeroFileNames());
    return localFileNamesMissingOnZero;
  }

  private void checkSegmentFilesIntegrity(
      LocalCoreMetadata localMetadata, ZeroStoreShardMetadata distantMetadata)
      throws SolrException {
    List<ZeroFile.WithLocal> segmentFiles =
        distantMetadata.getZeroFiles().stream()
            .filter(this::isSegmentsNFilename)
            .limit(2)
            .collect(Collectors.toList());
    if (segmentFiles.size() == 2) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          String.format(
              Locale.ROOT,
              "Zero store for collection %s and shard %s has conflicting files %s and %s",
              localMetadata.getCollectionName(),
              localMetadata.getShardName(),
              segmentFiles.get(0),
              segmentFiles.get(1)));
    } else if (segmentFiles.isEmpty() && !distantMetadata.getZeroFiles().isEmpty()) {
      // TODO - for now just log and propagate the error up, this class shouldn't do corruption
      // checking now
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Zero store has missing segments file");
    }
  }

  protected ZeroFile.WithLocal getNewZeroLocalFile(
      Directory coreDir, String fileName, String collectionName, String shardName, String suffix) {
    long fileSize = 0L;
    long fileChecksum = 0L;
    try (final IndexInput indexInput = coreDir.openInput(fileName, IOContext.READONCE)) {
      fileSize = indexInput.length();
      fileChecksum = CodecUtil.retrieveChecksum(indexInput);
    } catch (IOException e) {
      log.warn("Failed to get file metadata {}", fileName, e);
      // IO error when opening the file (probably not found, for example it could be wiped
      // in CorePushPull if a corruption was detected).
      // Consider the file empty so that it is pulled again from the Zero store.
    }
    return new ZeroFile.WithLocal(
        collectionName,
        shardName,
        fileName,
        buildFileName(fileName, suffix),
        fileSize,
        fileChecksum);
  }

  protected boolean areDistantAndLocalFileDifferent(
      Directory coreDir, ZeroFile.WithLocal distantFile, String localFileName, String coreName) {
    long localFileSize = 0L;
    long localFileChecksum = 0L;
    try (final IndexInput indexInput = coreDir.openInput(localFileName, IOContext.READONCE)) {
      localFileSize = indexInput.length();
      localFileChecksum = CodecUtil.retrieveChecksum(indexInput);
    } catch (IOException e) {
      log.warn("Error getting file metadata {}", localFileName, e);
      // IO error when opening the file (probably not found, for example it could be wiped
      // in CorePushPull if a corruption was detected).
      // Consider the file empty so that it is pulled again from the Zero store.
    }

    boolean different =
        (localFileSize != distantFile.getFileSize()
            || localFileChecksum != distantFile.getChecksum());

    if (different) {
      String message =
          String.format(
              Locale.ROOT,
              "Size/Checksum conflicts. collectionName=%s shardName=%s coreName=%s fileName=%s zeroStoreFileName=%s"
                  + " localSize=%s zeroStoreSize=%s localChecksum=%s zeroStoreChecksum=%s",
              distantFile.getCollectionName(),
              distantFile.getShardName(),
              coreName,
              localFileName,
              distantFile.getZeroFileName(),
              localFileSize,
              distantFile.getFileSize(),
              localFileChecksum,
              distantFile.getChecksum());
      log.info(message);
    }
    return different;
  }

  /** Identify the segments_N file in local Zero files. */
  public boolean isSegmentsNFilename(ZeroFile.WithLocal file) {
    return file.getSolrFileName().startsWith(SEGMENTS_N_PREFIX);
  }

  public ZeroFile.WithLocal newShardMetadataZeroFile(
      String collectionName, String shardName, String suffix) {
    return new ZeroFile.WithLocal(
        collectionName,
        shardName,
        SHARD_METADATA_ZERO_FILENAME,
        buildFileName(ZeroMetadataController.SHARD_METADATA_ZERO_FILENAME, suffix),
        ZeroStoreShardMetadata.FAKE_CORE_METADATA_LENGTH,
        ZeroStoreShardMetadata.FAKE_CORE_METADATA_CHECKSUM);
  }

  /**
   * This method implements the naming convention for files on the Zero store.
   *
   * <p>There really are two naming conventions, one for the {@link #SHARD_METADATA_ZERO_FILENAME},
   * and one for all other files. For simplicity the two conventions are identical.
   *
   * <ol>
   *   <li>{@link #SHARD_METADATA_ZERO_FILENAME} (<code>shard.metadata</code>) is named on the Zero
   *       store <code>shard.metadata.randomSuffix</code>, the random suffix is stored in Zookeeper
   *       and that's how the current <code>shard.metadata</code> file is identified (there might be
   *       older such files, they will have a different suffix from the one stored in Zookeeper)
   *   <li>All other files (composing a Solr core) can have any name, as long as it's unique so no
   *       chance of overwriting an existing file on the Zero store. The mapping from Zero store
   *       file name to local file name is maintained in the <code>shard.metadata</code> file so
   *       there's no need for a specific naming convention (same convention used nonetheless).
   * </ol>
   */
  public static String buildFileName(String prefix, String suffix) {
    return prefix + "." + suffix;
  }

  public ZeroFile.ToDelete newShardMetadataZeroFileToDelete(
      String collectionName, String shardName, String metadataSuffix) {
    return new ZeroFile.ToDelete(
        collectionName, shardName, buildFileName(SHARD_METADATA_ZERO_FILENAME, metadataSuffix));
  }
}
