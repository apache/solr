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
package org.apache.solr.client.solrj.request;

/** Encapsulates commit and optimize options for update requests. */
public record CommitOptions(
    boolean waitSearcher,
    boolean openSearcher,
    boolean softCommit,
    boolean expungeDeletes,
    int maxOptimizeSegments) {

  /** Compact constructor with validation. */
  public CommitOptions {
    if (maxOptimizeSegments < 1) {
      throw new IllegalArgumentException(
          "maxOptimizeSegments must be >= 1, got: " + maxOptimizeSegments);
    }
  }

  /**
   * Creates default commit options with: - waitSearcher: true - openSearcher: true - softCommit:
   * false - expungeDeletes: false - maxOptimizeSegments: Integer.MAX_VALUE
   */
  public CommitOptions() {
    this(true, true, false, false, Integer.MAX_VALUE);
  }

  /**
   * Sets whether to wait for the searcher to be registered/visible. Returns a new CommitOptions
   * instance with the updated value.
   *
   * @param waitSearcher true to wait for searcher
   * @return new CommitOptions instance for method chaining
   */
  public CommitOptions waitSearcher(boolean waitSearcher) {
    return new CommitOptions(
        waitSearcher, openSearcher, softCommit, expungeDeletes, maxOptimizeSegments);
  }

  /**
   * Sets whether to open a new searcher as part of the commit. Returns a new CommitOptions instance
   * with the updated value.
   *
   * @param openSearcher true to open a new searcher
   * @return new CommitOptions instance for method chaining
   */
  public CommitOptions openSearcher(boolean openSearcher) {
    return new CommitOptions(
        waitSearcher, openSearcher, softCommit, expungeDeletes, maxOptimizeSegments);
  }

  /**
   * Returns a new CommitOptions with the softCommit value changed.
   *
   * @param softCommit true for soft commit, false for hard commit
   * @return new CommitOptions instance with updated softCommit
   */
  public CommitOptions withSoftCommit(boolean softCommit) {
    return new CommitOptions(
        waitSearcher, openSearcher, softCommit, expungeDeletes, maxOptimizeSegments);
  }

  /**
   * Sets whether to expunge deleted documents.
   *
   * @param expungeDeletes true to expunge deletes
   * @return new CommitOptions instance for method chaining
   */
  public CommitOptions expungeDeletes(boolean expungeDeletes) {
    return new CommitOptions(
        waitSearcher, openSearcher, softCommit, expungeDeletes, maxOptimizeSegments);
  }

  /**
   * Sets the maximum number of segments to optimize down to. Only applies when using
   * ACTION.OPTIMIZE.
   *
   * @param maxOptimizeSegments maximum number of segments (must be >= 1)
   * @return new CommitOptions instance for method chaining
   * @throws IllegalArgumentException if maxOptimizeSegments &lt; 1
   */
  public CommitOptions maxOptimizeSegments(int maxOptimizeSegments) {
    return new CommitOptions(
        waitSearcher, openSearcher, softCommit, expungeDeletes, maxOptimizeSegments);
  }

  // Convenience factory methods

  /**
   * Creates CommitOptions with type based on if a soft commit was requested
   *
   * @return CommitOptions configured for soft commit
   */
  public static CommitOptions commit(boolean softCommit) {
    if (softCommit) {
      return forSoftCommit();
    } else {
      return forHardCommit();
    }
  }

  /**
   * Creates CommitOptions for a standard hard commit.
   *
   * @return CommitOptions with default settings for hard commit
   */
  public static CommitOptions forHardCommit() {
    return new CommitOptions();
  }

  /**
   * Creates CommitOptions for a soft commit.
   *
   * @return CommitOptions configured for soft commit
   */
  public static CommitOptions forSoftCommit() {
    return new CommitOptions().withSoftCommit(true);
  }

  /**
   * Creates CommitOptions for optimization with default settings. Optimizes down to 1 segment and
   * expunges deletes.
   *
   * @return CommitOptions suitable for optimize operations
   */
  public static CommitOptions forOptimize() {
    return new CommitOptions().expungeDeletes(true).maxOptimizeSegments(1);
  }

  /**
   * Creates CommitOptions for optimization with specified max segments.
   *
   * @param maxSegments maximum number of segments to optimize to
   * @return CommitOptions configured for optimize with specified max segments
   */
  public static CommitOptions forOptimize(int maxSegments) {
    return new CommitOptions().expungeDeletes(true).maxOptimizeSegments(maxSegments);
  }
}
