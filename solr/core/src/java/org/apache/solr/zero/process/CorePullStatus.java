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
package org.apache.solr.zero.process;

/** Enumerates all possible outcomes of core pull tasks */
public enum CorePullStatus {
  /** Core pulled successfully from Zero store */
  SUCCESS(true, false),
  /** There was no need to pull the core from the Zero store */
  NOT_NEEDED(true, false),
  /** The pull task was merged with another duplicate task in the queue (deduplicated) */
  DUPLICATE_REQUEST(true, false),

  /** Core failed to pull from the Zero store, unknown cause */
  FAILURE(false, true),
  /** Pulling process was interrupted */
  INTERRUPTED(false, true),
  /**
   * Core not pulled from Zero store because corresponding shard.metadata file was not found there
   */
  ZERO_METADATA_MISSING(false, false),
  /** The pull lock was not acquired immediately, task can be retried */
  FAILED_TO_ACQUIRE_LOCK(false, true);

  private final boolean isTransientError;
  private final boolean isSuccess;

  CorePullStatus(boolean isSuccess, boolean isTransientError) {
    assert !(isSuccess && isTransientError);
    this.isSuccess = isSuccess;
    this.isTransientError = isTransientError;
  }

  /**
   * Only defined for a status when ({@link #isSuccess()} returns {@code false}).
   *
   * @return {@code true} when it makes sense to retry (a few times) running the same task again,
   *     i.e. when the cause of the failure might disappear.
   */
  public boolean isTransientError() {
    return isTransientError;
  }

  /**
   * @return {@code true} if content was successfully synced from Zero store OR if no sync was
   *     required.
   */
  public boolean isSuccess() {
    return isSuccess;
  }
}
