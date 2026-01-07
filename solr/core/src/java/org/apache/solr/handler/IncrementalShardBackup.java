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

package org.apache.solr.handler;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.math3.util.Precision;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.backup.BackupFilePaths;
import org.apache.solr.core.backup.Checksum;
import org.apache.solr.core.backup.ShardBackupId;
import org.apache.solr.core.backup.ShardBackupMetadata;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for orchestrating the actual incremental backup process.
 *
 * <p>If this is the first backup for a collection, all files are uploaded. But if previous backups
 * exist, uses the most recent {@link ShardBackupMetadata} file to determine which files already
 * exist in the repository and can be skipped.
 */
public class IncrementalShardBackup {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Maximum number of files to upload in parallel during backup. Can be configured via the system
   * property {@code solr.backup.maxparalleluploads} or environment variable {@code
   * SOLR_BACKUP_MAXPARALLELUPLOADS}.
   */
  private static final int DEFAULT_MAX_PARALLEL_UPLOADS =
      EnvUtils.getPropertyAsInteger("solr.backup.maxparalleluploads", 1);

  private SolrCore solrCore;

  private BackupFilePaths incBackupFiles;
  private BackupRepository backupRepo;

  private ShardBackupId prevShardBackupId;
  private ShardBackupId shardBackupId;
  private Optional<String> commitNameOption;

  /**
   * @param prevShardBackupId previous ShardBackupMetadata file which will be used for skipping
   *     uploading index files already present in this file.
   * @param shardBackupId file where all meta data of this backup will be stored to.
   */
  public IncrementalShardBackup(
      BackupRepository backupRepo,
      SolrCore solrCore,
      BackupFilePaths incBackupFiles,
      ShardBackupId prevShardBackupId,
      ShardBackupId shardBackupId,
      Optional<String> commitNameOption) {
    this.backupRepo = backupRepo;
    this.solrCore = solrCore;
    this.incBackupFiles = incBackupFiles;
    this.prevShardBackupId = prevShardBackupId;
    this.shardBackupId = shardBackupId;
    this.commitNameOption = commitNameOption;
  }

  public IncrementalShardSnapshotResponse backup() throws Exception {
    final IndexCommit indexCommit = getAndSaveIndexCommit();
    try {
      return backup(indexCommit);
    } finally {
      solrCore.getDeletionPolicy().releaseCommitPoint(indexCommit.getGeneration());
    }
  }

  /**
   * Returns {@link IndexDeletionPolicyWrapper#getAndSaveLatestCommit} unless a particular
   * commitName was requested.
   *
   * <p>Note:
   *
   * <ul>
   *   <li>This method does error handling when the commit can't be found and wraps them in {@link
   *       SolrException}
   *   <li>If this method returns, the result will be non null, and the caller <em>MUST</em> call
   *       {@link IndexDeletionPolicyWrapper#releaseCommitPoint} when finished
   * </ul>
   */
  private IndexCommit getAndSaveIndexCommit() throws IOException {
    if (commitNameOption.isPresent()) {
      return SnapShooter.getAndSaveNamedIndexCommit(solrCore, commitNameOption.get());
    }

    final IndexDeletionPolicyWrapper delPolicy = solrCore.getDeletionPolicy();
    final IndexCommit commit = delPolicy.getAndSaveLatestCommit();
    if (null == commit) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Index does not yet have any commits for core " + solrCore.getName());
    }
    if (log.isDebugEnabled()) {
      log.debug("Using latest commit: generation={}", commit.getGeneration());
    }
    return commit;
  }

  private IndexCommit getAndSaveLatestIndexCommit(IndexDeletionPolicyWrapper delPolicy) {
    final IndexCommit commit = delPolicy.getAndSaveLatestCommit();
    if (null == commit) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Index does not yet have any commits for core " + solrCore.getName());
    }
    if (log.isDebugEnabled()) {
      log.debug("Using latest commit: generation={}", commit.getGeneration());
    }
    return commit;
  }

  // note: remember to reserve the indexCommit first so it won't get deleted concurrently
  protected IncrementalShardSnapshotResponse backup(final IndexCommit indexCommit)
      throws Exception {
    assert indexCommit != null;
    URI backupLocation = incBackupFiles.getBackupLocation();
    log.info(
        "Creating backup snapshot at {} shardBackupMetadataFile:{}", backupLocation, shardBackupId);
    IncrementalShardSnapshotResponse details = new IncrementalShardSnapshotResponse();
    details.startTime = Instant.now().toString();

    Collection<String> files = indexCommit.getFileNames();
    Directory dir =
        solrCore
            .getDirectoryFactory()
            .get(
                solrCore.getIndexDir(),
                DirectoryFactory.DirContext.BACKUP,
                solrCore.getSolrConfig().indexConfig.lockType);
    try {
      BackupStats stats = incrementalCopy(files, dir);
      details.indexFileCount = stats.fileCount.get();
      details.uploadedIndexFileCount = stats.uploadedFileCount.get();
      details.indexSizeMB = stats.getIndexSizeMB();
      details.uploadedIndexFileMB = stats.getTotalUploadedMB();
    } finally {
      solrCore.getDirectoryFactory().release(dir);
    }

    CloudDescriptor cd = solrCore.getCoreDescriptor().getCloudDescriptor();
    if (cd != null) {
      details.shard = cd.getShardId();
    }

    details.endTime = Instant.now().toString();
    details.shardBackupId = shardBackupId.getIdAsString();
    log.info(
        "Done creating backup snapshot at {} shardBackupMetadataFile:{}",
        backupLocation,
        shardBackupId);
    return details;
  }

  private ShardBackupMetadata getPrevBackupPoint() throws IOException {
    if (prevShardBackupId == null) {
      return ShardBackupMetadata.empty();
    }
    return ShardBackupMetadata.from(
        backupRepo, incBackupFiles.getShardBackupMetadataDir(), prevShardBackupId);
  }

  private BackupStats incrementalCopy(Collection<String> indexFiles, Directory dir)
      throws IOException {
    ShardBackupMetadata oldBackupPoint = getPrevBackupPoint();
    ShardBackupMetadata currentBackupPoint = ShardBackupMetadata.empty();
    URI indexDir = incBackupFiles.getIndexDir();
    BackupStats backupStats = new BackupStats();

    // Only use an executor for parallel uploads when parallelism > 1
    // When set to 1, run synchronously to avoid thread-local state issues with CallerRunsPolicy
    int maxParallelUploads = DEFAULT_MAX_PARALLEL_UPLOADS;
    ExecutorService executor =
        maxParallelUploads > 1
            ? new ExecutorUtil.MDCAwareThreadPoolExecutor(
                0,
                maxParallelUploads,
                60L,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                new SolrNamedThreadFactory("IncrementalBackup"),
                new ThreadPoolExecutor.CallerRunsPolicy())
            : null;

    List<Future<?>> uploadFutures = new ArrayList<>();

    try {
      for (String fileName : indexFiles) {
        // Capture variable for lambda
        final String fileNameFinal = fileName;

        Runnable uploadTask =
            () -> {
              try {
                // Calculate checksum and check if file already exists in previous backup
                Optional<ShardBackupMetadata.BackedFile> opBackedFile =
                    oldBackupPoint.getFile(fileNameFinal);
                Checksum originalFileCS = backupRepo.checksum(dir, fileNameFinal);

                if (opBackedFile.isPresent()) {
                  ShardBackupMetadata.BackedFile backedFile = opBackedFile.get();
                  Checksum existedFileCS = backedFile.fileChecksum;
                  if (existedFileCS.equals(originalFileCS)) {
                    synchronized (currentBackupPoint) {
                      currentBackupPoint.addBackedFile(opBackedFile.get());
                    }
                    backupStats.skippedUploadingFile(existedFileCS);
                    return;
                  }
                }

                // File doesn't exist or has changed - upload it
                String backedFileName = UUID.randomUUID().toString();
                backupRepo.copyIndexFileFrom(dir, fileNameFinal, indexDir, backedFileName);

                synchronized (currentBackupPoint) {
                  currentBackupPoint.addBackedFile(backedFileName, fileNameFinal, originalFileCS);
                }
                backupStats.uploadedFile(originalFileCS);
              } catch (IOException e) {
                throw new RuntimeException("Failed to process file: " + fileNameFinal, e);
              }
            };

        if (executor != null) {
          uploadFutures.add(executor.submit(uploadTask));
        } else {
          // Run synchronously when parallelism is 1
          try {
            uploadTask.run();
          } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException) {
              throw (IOException) e.getCause();
            }
            throw e;
          }
        }
      }

      // Wait for all uploads to complete and collect any errors (only if using executor)
      if (executor != null) {
        // We need to wait for ALL futures before throwing, otherwise we might exit
        // before all successfully uploaded files are added to currentBackupPoint
        Throwable firstError = null;
        for (Future<?> future : uploadFutures) {
          try {
            future.get();
          } catch (ExecutionException e) {
            if (firstError == null) {
              Throwable cause = e.getCause();
              // Unwrap RuntimeExceptions that wrap the original IOException
              if (cause instanceof RuntimeException && cause.getCause() != null) {
                firstError = cause.getCause();
              } else {
                firstError = cause;
              }
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (firstError == null) {
              firstError = e;
            }
          }
        }

        // Now throw the first error we encountered, if any
        if (firstError != null) {
          if (firstError instanceof Error) {
            // Rethrow Errors (like OutOfMemoryError) - don't try to recover
            throw (Error) firstError;
          } else if (firstError instanceof IOException) {
            throw (IOException) firstError;
          } else if (firstError instanceof RuntimeException) {
            throw (RuntimeException) firstError;
          } else if (firstError instanceof InterruptedException) {
            throw new IOException("Backup interrupted", firstError);
          } else {
            throw new IOException("Error during parallel backup upload", firstError);
          }
        }
      }
    } finally {
      if (executor != null) {
        executor.shutdown();
        try {
          if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
            executor.shutdownNow();
          }
        } catch (InterruptedException e) {
          executor.shutdownNow();
          Thread.currentThread().interrupt();
        }
      }
    }

    currentBackupPoint.store(backupRepo, incBackupFiles.getShardBackupMetadataDir(), shardBackupId);
    return backupStats;
  }

  private static class BackupStats {
    private final AtomicInteger fileCount = new AtomicInteger();
    private final AtomicInteger uploadedFileCount = new AtomicInteger();
    private final AtomicLong indexSize = new AtomicLong();
    private final AtomicLong totalUploadedBytes = new AtomicLong();

    public void uploadedFile(Checksum file) {
      fileCount.incrementAndGet();
      uploadedFileCount.incrementAndGet();
      indexSize.addAndGet(file.size);
      totalUploadedBytes.addAndGet(file.size);
    }

    public void skippedUploadingFile(Checksum existedFile) {
      fileCount.incrementAndGet();
      indexSize.addAndGet(existedFile.size);
    }

    public double getIndexSizeMB() {
      return Precision.round(indexSize.get() / (1024.0 * 1024), 3);
    }

    public double getTotalUploadedMB() {
      return Precision.round(totalUploadedBytes.get() / (1024.0 * 1024), 3);
    }
  }

  public static class IncrementalShardSnapshotResponse extends SolrJerseyResponse {
    @Schema(description = "The time at which backup snapshot started at.")
    @JsonProperty("startTime")
    public String startTime;

    @Schema(description = "The count of index files in the snapshot.")
    @JsonProperty("indexFileCount")
    public int indexFileCount;

    @Schema(description = "The count of uploaded index files.")
    @JsonProperty("uploadedIndexFileCount")
    public int uploadedIndexFileCount;

    @Schema(description = "The size of index in MB.")
    @JsonProperty("indexSizeMB")
    public double indexSizeMB;

    @Schema(description = "The size of uploaded index in MB.")
    @JsonProperty("uploadedIndexFileMB")
    public double uploadedIndexFileMB;

    @Schema(description = "Shard Id.")
    @JsonProperty("shard")
    public String shard;

    @Schema(description = "The time at which backup snapshot completed at.")
    @JsonProperty("endTime")
    public String endTime;

    @Schema(description = "ShardId of shard to which core belongs to.")
    @JsonProperty("shardBackupId")
    public String shardBackupId;
  }
}
