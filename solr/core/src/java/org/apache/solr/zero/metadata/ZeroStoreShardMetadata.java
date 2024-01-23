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

import com.google.common.annotations.VisibleForTesting;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.solr.zero.client.ZeroFile;
import org.apache.solr.zero.process.CorePusher;
import org.apache.solr.zero.util.ToFromJson;

/**
 * Object defining metadata stored in Zero store for a Zero Collection shard and its builders
 * (storage in Zero store is a single copy per shard and not per core or replica as usual in
 * SolrCloud). This metadata includes all actual segment files as well as the segments_N file of the
 * commit point.
 *
 * <p>This object is serialized to/from Json and stored in the Zero store as a file named <code>
 * shard.metadata</code> (with a random suffix).
 *
 * <p>Although in the Zero store storage is per shard, each Solr node manages this file for its
 * specific core (replica) of the shard. The update strategy via unique filenames on the Zero store
 * and a tiebreaker using zookeeper guarantees consistency at the shard level (a replica doesn't
 * overwrite an update to the shard in the Zero store done by another replica on another node).
 */
public class ZeroStoreShardMetadata {

  /** shard.metadata file doesn't have its length or checksum compared so using fake values */
  public static final long FAKE_CORE_METADATA_LENGTH = -10;

  public static final long FAKE_CORE_METADATA_CHECKSUM = -10;
  public static final long UNDEFINED_GENERATION = -1L;

  /**
   * Generation number of index represented by this metadata. Generation numbers across replicas
   * don't carry any meaning since each replica can be doing its own indexing. We are only using
   * this generation for optimizations in the context of a single replica. It should not be used
   * beyond following stated purposes. If its mere existence is popping it into consideration for
   * solving unintended use cases, then we can remove it and implement the suggested alternatives
   * for following usages. 1. identify a scenario where local index generation number is higher than
   * what we have in Zero store. In that scenario we would switch index to a new directory when
   * pulling contents from Zero store. Because in the presence of higher generation number locally,
   * Zero store contents cannot establish their legitimacy. Storing this number is just an
   * optimization. We can always infer that number from segment_N file and get rid of this usage.
   * {@link ZeroMetadataController#diffMetadataforPull(LocalCoreMetadata, ZeroStoreShardMetadata)}
   * ()} 2. {@link CorePusher#endToEndPushCoreToZeroStore()} piggy backs on cached
   * ZeroStoreShardMetadata's generation number instead of carrying a separate lastGenerationPushed
   * property in the cache. This is also an optimization, we can always add lastGenerationPushed
   * property to cache and get rid of this usage.
   */
  private final long generation;

  /**
   * The array of files that constitute the current commit point of the core (as known by the Zero
   * store). This array is not ordered! There are no duplicate entries in it either
   */
  private final Set<ZeroFile.WithLocal> zeroFiles;

  /**
   * Files marked for delete but not yet removed from the Zero store. Files are only actually
   * deleted after a new version of shard.metadata has been successfully written to the Zero store
   * and "validated" by the ZooKeeper metadataSuffix update.
   */
  private final Set<ZeroFile.ToDelete> zeroFilesToDelete;

  /** For Jackson deserialization. See {@link ToFromJson}. */
  public ZeroStoreShardMetadata() {
    this(UNDEFINED_GENERATION);
  }

  /** Always builds non "isCorrupt" and non "isDeleted" metadata. */
  public ZeroStoreShardMetadata(
      Set<ZeroFile.WithLocal> zeroFiles,
      Set<ZeroFile.ToDelete> zeroFilesToDelete,
      long generation) {
    this.zeroFiles = zeroFiles;
    this.zeroFilesToDelete = zeroFilesToDelete;
    this.generation = generation;
  }

  @VisibleForTesting
  public ZeroStoreShardMetadata(long generation) {
    this.zeroFiles = new HashSet<>();
    this.zeroFilesToDelete = new HashSet<>();
    this.generation = generation;
  }

  /** Adds a file to the set of "active" files listed in the metadata */
  public void addFile(ZeroFile.WithLocal f) {
    this.zeroFiles.add(f);
  }

  /** Removes a file from the set of "active" files listed in the metadata */
  public void removeFile(ZeroFile.WithLocal f) {
    boolean removed = this.zeroFiles.remove(f);
    assert removed; // If we remove things that are not there, likely a bug in our code
  }

  /**
   * Adds a file to the set of files to delete listed in the metadata
   *
   * <p>This method should always be called with {@link #removeFile(ZeroFile.WithLocal)} above
   * (except when adding for delete the current shard.metadata file).
   */
  public void addFileToDelete(ZeroFile.ToDelete f) {
    this.zeroFilesToDelete.add(f);
  }

  public long getGeneration() {
    return this.generation;
  }

  public Set<ZeroFile.WithLocal> getZeroFiles() {
    return zeroFiles;
  }

  public Set<ZeroFile.ToDelete> getZeroFilesToDelete() {
    return zeroFilesToDelete;
  }

  public Map<String, ZeroFile.WithLocal> getZeroFilesAsMap() {
    return zeroFiles.stream()
        .collect(Collectors.toMap(ZeroFile.WithLocal::getSolrFileName, Function.identity()));
  }

  public Set<String> getZeroFileNames() {
    return zeroFiles.stream().map(ZeroFile.WithLocal::getSolrFileName).collect(Collectors.toSet());
  }

  /** Removes a file from the set of "deleted" files listed in the metadata */
  public void removeFilesFromDeleted(Set<ZeroFile.ToDelete> files) {
    int originalSize = this.zeroFilesToDelete.size();
    boolean removed = this.zeroFilesToDelete.removeAll(files);
    int totalRemoved = originalSize - this.zeroFilesToDelete.size();

    // If we remove things that are not there, likely a bug in our code
    assert removed && (totalRemoved == files.size());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ZeroStoreShardMetadata)) return false;

    ZeroStoreShardMetadata that = (ZeroStoreShardMetadata) o;

    if (this.generation != that.generation) return false;
    if (!this.zeroFiles.equals(that.zeroFiles)) return false;
    return this.zeroFilesToDelete.equals(that.zeroFilesToDelete);
  }

  @Override
  public int hashCode() {
    return Objects.hash(generation, this.zeroFiles, this.zeroFilesToDelete);
  }

  public static String generateMetadataSuffix() {
    return UUID.randomUUID().toString();
  }

  public String toJson() throws Exception {
    ToFromJson<ZeroStoreShardMetadata> converter = new ToFromJson<>();
    return converter.toJson(this);
  }

  public static ZeroStoreShardMetadata fromJson(String json) throws Exception {
    ToFromJson<ZeroStoreShardMetadata> converter = new ToFromJson<>();
    return converter.fromJson(json, ZeroStoreShardMetadata.class);
  }
}
