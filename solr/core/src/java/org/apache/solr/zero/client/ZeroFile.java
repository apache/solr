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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.google.common.annotations.VisibleForTesting;
import java.net.URI;
import java.time.Instant;
import java.util.Objects;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.zero.util.ToFromJson;

/**
 * The parent representation of a file on the Zero store. Contains the information required to
 * locate and name that file in the store, but nothing else. This class is instantiated directly
 * when listing files on the Zero store.
 *
 * <p>ZeroFile instances (including existing subclasses) are equal if they represent the same Zero
 * store file, even if other parameters of the instance are different. This makes it easier to
 * compare for example {@link ZeroFile} files coming from a directory listing of the Zero store and
 * {@link WithLocal} files of a core that are present both locally and on the Zero store.
 */
public class ZeroFile {
  protected final String collectionName;
  protected final String shardName;
  protected final String zeroFileName;

  /** For Jackson deserialization. See {@link ToFromJson}. */
  protected ZeroFile() {
    collectionName = shardName = zeroFileName = null;
  }

  public ZeroFile(String collectionName, String shardName, String zeroFileName) {
    this.collectionName = collectionName;
    this.shardName = shardName;
    this.zeroFileName = zeroFileName;
  }

  protected ZeroFile(ZeroFile f) {
    this.collectionName = f.collectionName;
    this.shardName = f.shardName;
    this.zeroFileName = f.zeroFileName;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public String getShardName() {
    return shardName;
  }

  public String getZeroFileName() {
    return zeroFileName;
  }

  public URI getFileURI(BackupRepository repo) {
    URI pathURI = getShardDirectoryURI(repo);
    return repo.resolve(pathURI, getZeroFileName());
  }

  public URI getShardDirectoryURI(BackupRepository repo) {
    URI collectionURI = getCollectionDirectoryURI(repo);
    return repo.resolveDirectory(collectionURI, shardName);
  }

  public URI getCollectionDirectoryURI(BackupRepository repo) {
    URI baseURI = repo.createURI(repo.getBackupLocation(null));
    return repo.resolveDirectory(baseURI, collectionName);
  }

  public boolean pathEmpty() {
    return (collectionName == null
        || collectionName.isEmpty()
        || shardName == null
        || shardName.isEmpty());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ZeroFile) {
      ZeroFile other = (ZeroFile) obj;
      return Objects.equals(collectionName, other.collectionName)
          && Objects.equals(shardName, other.shardName)
          && Objects.equals(zeroFileName, other.zeroFileName);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(collectionName, shardName, zeroFileName);
  }

  /**
   * A file stored in the Zero store with its matching local copy name. The names of the two are
   * totally independent of each other, even though in general the Zero store name is built using
   * the local Solr name plus a random suffix.
   *
   * <p>This class relies on {@link ZeroFile#equals(Object)}. Two local files representing the same
   * Zero store file but having different Solr names are considered equal. In practice this should
   * never happen, but relying on the superclass comparison allows for example comparing for
   * equality files that are part of an index and files coming from a listing of files on the Zero
   * store.
   */
  public static class WithLocal extends ZeroFile {
    protected final String solrFileName;

    private final boolean checksumPresent;

    /**
     * Name the file should have on a Solr server retrieving it, not including the core specific
     * part of the filename (i.e. the path)
     */
    private final long fileSize;

    /**
     * Lucene generated checksum of the file. It is used in addition to file size to compare local
     * and Zero store files.
     */
    private final long checksum;

    /** For Jackson deserialization. See {@link ToFromJson}. */
    protected WithLocal() {
      solrFileName = null;
      checksumPresent = false;
      fileSize = checksum = 0L;
    }

    @VisibleForTesting
    WithLocal(String collectionName, String shardName, String solrFileName, String zeroFileName) {
      this(collectionName, shardName, solrFileName, zeroFileName, false, -1L, -1L);
    }

    public WithLocal(
        String collectionName,
        String shardName,
        String solrFileName,
        String zeroFileName,
        long fileSize,
        long checksum) {
      this(collectionName, shardName, solrFileName, zeroFileName, true, fileSize, checksum);
    }

    private WithLocal(
        String collectionName,
        String shardName,
        String solrFileName,
        String zeroFileName,
        boolean checksumPresent,
        long fileSize,
        long checksum) {
      super(collectionName, shardName, zeroFileName);
      this.solrFileName = solrFileName;
      this.checksumPresent = checksumPresent;
      this.fileSize = fileSize;
      this.checksum = checksum;
    }

    public String getSolrFileName() {
      return solrFileName;
    }

    public boolean isChecksumPresent() {
      return checksumPresent;
    }

    public long getFileSize() {
      return this.fileSize;
    }

    public long getChecksum() {
      return this.checksum;
    }
  }

  /**
   * A file stored in the Zero store that is no longer used and should be deleted.
   *
   * <p>Deletes on the Zero store do not need to know the local Solr file name, therefore this class
   * does not extend {@link WithLocal}.
   *
   * <p>This class relies on {@link ZeroFile#equals(Object)}. Two files to delete representing the
   * same Zero store file but having different {@link #deletedAt} values are considered equal.
   */
  public static class ToDelete extends ZeroFile {

    // Even though deletes are only done when it is clear the file is no longer needed (a successful
    // push updated the Zero store commit point), it is possible that a slow pull on another node is
    // still pulling files using an older commit point. Even if this happens qnd the pull fails, a
    // subsequent pull would likely succeed. Maybe this timestamp is overdesign.
    private Instant deletedAt;

    /** For easy Jackson serialization of Instant type. See {@link ToFromJson}. */
    @JsonGetter("deletedAt")
    public long getMyInstantValEpoch() {
      return deletedAt.toEpochMilli();
    }

    /** For Jackson deserialization of Instant type. See {@link ToFromJson}. */
    @JsonGetter("deletedAt")
    public void setMyInstantValEpoch(long epochMilli) {
      this.deletedAt = Instant.ofEpochMilli(epochMilli);
    }

    /** For Jackson deserialization. See {@link ToFromJson}. */
    protected ToDelete() {
      deletedAt = null;
    }

    public ToDelete(ZeroFile zeroFile, Instant deletedAt) {
      super(zeroFile);
      this.deletedAt = deletedAt;
    }

    public ToDelete(String collectionName, String shardName, String zeroFileName) {
      this(collectionName, shardName, zeroFileName, Instant.now());
    }

    public ToDelete(
        String collectionName, String shardName, String zeroFileName, Instant deletedAt) {
      super(collectionName, shardName, zeroFileName);
      this.deletedAt = deletedAt;
    }

    /**
     * @return time in milliseconds (converted from nanotime) that file was marked as deleted
     */
    public Instant getDeletedAt() {
      return this.deletedAt;
    }
  }
}
