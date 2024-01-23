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
package org.apache.solr.core;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.TimeUnit;
import org.apache.solr.zero.process.CorePuller;

/**
 * In memory representation of the {@code <zero> ... </zero>} section of solr.xml configuration file
 * For now contains only the description of a BackupRepository to be used as an abstraction over
 * desired Zero store. Multiple repositories can be defined, but only one will be used : - the
 * enabled one - the first of the enabled ones if multiple are enabled - the first one if none are
 * enabled the zero section must contain enabled = true for the repositories configuration to be
 * loaded
 */
public class ZeroConfig {

  private final PluginInfo[] repositories;
  private final boolean enabled;

  @VisibleForTesting
  public ZeroConfig() {
    this(false, new PluginInfo[0]);
  }

  private ZeroConfig(boolean enabled, PluginInfo[] repositories) {
    this.enabled = enabled;
    this.repositories = repositories;
  }

  public boolean isEnabled() {
    return enabled;
  }

  private static final PluginInfo[] NO_OP_REPOSITORIES = new PluginInfo[0];

  public PluginInfo[] getBackupRepositoryPlugins() {
    if (enabled) {
      return repositories;
    } else {
      return NO_OP_REPOSITORIES;
    }
  }

  /**
   * Limit to the number of Zero store files to delete accepted on the delete queue (and lost in
   * case of server crash). When the queue reaches that size, no more deletes are accepted (will be
   * retried later for a core, next time it is pushed).
   */
  private final int almostMaxDeleterQueueSize = 200;

  private final int deleterThreadPoolSize = 5;

  private final int maxDeleteAttempts = 50;
  private final long deleteSleepMsFailedAttempt = TimeUnit.SECONDS.toMillis(10);

  /**
   * After a successful core push has marked a file to be deleted, wait at least this long before
   * actually deleting it from the Zero store. This is in case another node is still fetching files
   * from the Zero store based on an older version of shard.properties that includes that file in
   * the commit point.
   *
   * <p>Note the time at which a file is marked deleted is when it got added to the new
   * shard.metadata file (or actually to the {@link
   * org.apache.solr.zero.metadata.ZeroStoreShardMetadata}), not when the push finished in success.
   * If the push took a very long time, by the time the new shard.metadata is visible, it's possible
   * that all newly added files to delete are already ripe for deleting (which is likely ok, and
   * even if that makes another node's pull to fail, upon retry that node will be able to pull the
   * core correctly).
   */
  private final long deleteDelayMs = TimeUnit.MINUTES.toMillis(3);

  /** Enables/disables Zero store for a Solr node. */
  private final boolean zeroStoreEnabled =
      Boolean.getBoolean(ZeroSystemProperty.ZeroStoreEnabled.getPropertyName());

  /**
   * Thread pool size for the executor that pushes files to Zero store. Defaults to 30 threads for
   * now, to support higher indexing throughput.
   */
  private final int numFilePusherThreads =
      Integer.getInteger(ZeroSystemProperty.numFilePusherThreads.getPropertyName(), 30);

  /**
   * Thread pool size for the executor that pulls files from Zero store. Defaults to 100 threads.
   */
  private final int numFilePullerThreads =
      Integer.getInteger(ZeroSystemProperty.numFilePullerThreads.getPropertyName(), 100);

  /** Max number of cores being concurrenty pulled async. Defaults to 10 cores. */
  private final int numCorePullerThreads =
      Integer.getInteger(ZeroSystemProperty.numCorePullerThreads.getPropertyName(), 10);

  /**
   * Number of automatic retries (after a transient IO error) to pull a core before giving up. This
   * is the number of retries in addition to the initial pull attempt. The automatic retries are
   * only triggered when the error is considered as transient. Defaults to 10. Attempts are spaced
   * by at least {@link #corePullRetryDelayMs}), which means we'll retry for at least 180 seconds
   * before giving up. This is something to adjust as the implementation of the delay between
   * retries is cleaned up, see {@link CorePuller}.
   */
  private final int numCorePullAutoRetries =
      Integer.getInteger(ZeroSystemProperty.numCorePullAutoRetries.getPropertyName(), 10);

  /**
   * Retry delay, in milliseconds, after a core pull failure when loading a core. Defaults to 20s.
   */
  private final long corePullRetryDelayMs =
      TimeUnit.SECONDS.toMillis(
          Integer.getInteger(ZeroSystemProperty.corePullRetryDelayS.getPropertyName(), 20));

  /**
   * Base delay, in milliseconds, between two attempts to pull core index files when starting an
   * index update. Then the effective delay is computed by multiplying this base delay with the
   * number of previous consecutive failed attempts. Defaults to 3s.
   */
  private final long corePullAttemptDelayMs =
      TimeUnit.SECONDS.toMillis(
          Integer.getInteger(ZeroSystemProperty.corePullAttemptDelayS.getPropertyName(), 3));

  /**
   * Maximum number of consecutive failed attempts to pull a core. The number of attempts takes into
   * account the automatic retries, if any, and the request-based attempts. For example, if a core
   * is corrupted on the Zero store side, then after this maximum number of attempts to pull it and
   * open the index, no more pulls will be done and any attempt will fail with an exception.
   *
   * <p>TODO : not used as intended / seems to act as a duplicate to numCorePullAutoRetries
   */
  private final int maxFailedCorePullAttempts =
      Integer.getInteger(ZeroSystemProperty.maxFailedCorePullAttempts.getPropertyName(), 20);

  /**
   * Delay, in milliseconds, before allowing another attempts when all pull attempts have been
   * exhausted. Defaults to 5 min.
   */
  private final long allAttemptsExhaustedRetryDelayMs =
      TimeUnit.MINUTES.toMillis(
          Integer.getInteger(
              ZeroSystemProperty.allAttemptsExhaustedRetryDelayMin.getPropertyName(), 5));

  public boolean isZeroStoreEnabled() {
    return zeroStoreEnabled;
  }

  /** Thread pool size for the executor that pushes files to Zero store. */
  public int getNumFilePusherThreads() {
    return numFilePusherThreads;
  }

  /** Thread pool size for the executor that pulls files from Zero store. */
  public int getNumFilePullerThreads() {
    return numFilePullerThreads;
  }

  /** Max number of cores being concurrenty pulled async */
  public int getNumCorePullerThreads() {
    return numCorePullerThreads;
  }

  /**
   * Number of automatic retries to pull a core before giving up. This is the number of retries in
   * addition to the initial pull attempt. The automatic retries are only triggered when the error
   * is considered as transient.
   *
   * <p>TODO requires a review as this configuration is only used from tests ;(
   */
  public int getNumCorePullAutoRetries() {
    return numCorePullAutoRetries;
  }

  /** Retry delay, in milliseconds, after a core pull failure when loading a core. */
  public long getCorePullRetryDelay() {
    return corePullRetryDelayMs;
  }

  /**
   * Base delay, in milliseconds, between two attempts to pull core index files when starting an
   * index update. Then the effective delay is computed by multiplying this base delay with the
   * number of previous consecutive failed attempts.
   */
  public long getCorePullAttemptDelay() {
    return corePullAttemptDelayMs;
  }

  /** Maximum number of consecutive failed attempts to pull a core. */
  public int getMaxFailedCorePullAttempts() {
    return maxFailedCorePullAttempts;
  }

  public long getDeleteDelayMs() {
    return deleteDelayMs;
  }

  public int getAlmostMaxDeleterQueueSize() {
    return almostMaxDeleterQueueSize;
  }

  public int getDeleterThreadPoolSize() {
    return deleterThreadPoolSize;
  }

  public int getMaxDeleteAttempts() {
    return maxDeleteAttempts;
  }

  public long getDeleteSleepMsFailedAttempt() {
    return deleteSleepMsFailedAttempt;
  }

  /**
   * Delay, in milliseconds, before allowing another attempts when all pull attempts have been
   * exhausted.
   */
  public long getAllAttemptsExhaustedRetryDelay() {
    return allAttemptsExhaustedRetryDelayMs;
  }

  /** Enum of constants representing the system properties used by the Zero store feature */
  public enum ZeroSystemProperty {
    ZeroStoreEnabled("zeroStoreEnabled"),
    numFilePusherThreads("zeroStoreNumFilePusherThreads"),
    numFilePullerThreads("zeroStoreNumFilePullerThreads"),
    numCorePullerThreads("zeroStoreNumCorePullerThreads"),
    numCorePullAutoRetries("zeroStoreNumCorePullAutoRetries"),
    corePullRetryDelayS("zeroStoreCorePullRetryDelayS"),
    corePullAttemptDelayS("zeroStoreCorePullAttemptDelayS"),
    maxFailedCorePullAttempts("zeroStoreMaxFailedCorePullAttempts"),
    allAttemptsExhaustedRetryDelayMin("zeroStoreAllAttemptsExhaustedRetryDelayMin"),
    ;

    private final String name;

    ZeroSystemProperty(String propName) {
      name = propName;
    }

    public String getPropertyName() {
      return name;
    }
  }

  public static class ZeroConfigBuilder {
    private PluginInfo[] repositoryPlugins = new PluginInfo[0];
    private boolean enabled = true;

    public ZeroConfigBuilder() {}

    public ZeroConfigBuilder setEnabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public ZeroConfigBuilder setBackupRepositoryPlugins(PluginInfo[] repositoryPlugins) {
      this.repositoryPlugins = repositoryPlugins != null ? repositoryPlugins : new PluginInfo[0];
      return this;
    }

    public ZeroConfig build() {
      return new ZeroConfig(enabled, repositoryPlugins);
    }
  }
}
