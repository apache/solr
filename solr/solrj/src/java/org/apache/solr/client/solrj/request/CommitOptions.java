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

/**
 * Encapsulates commit and optimize options for update requests. This class provides a type-safe way
 * to specify commit parameters instead of using multiple boolean parameters in method signatures.
 *
 * @since Solr 10.0
 */
public class CommitOptions {

  private boolean waitSearcher = true;
  private boolean openSearcher = true;
  private boolean softCommit = false;
  private boolean expungeDeletes = false;
  private int maxOptimizeSegments = Integer.MAX_VALUE;

  /**
   * Creates default commit options with: - waitSearcher: true - openSearcher: true - softCommit:
   * false - expungeDeletes: false - maxOptimizeSegments: Integer.MAX_VALUE
   */
  public CommitOptions() {}

  /**
   * Sets whether to wait for the searcher to be registered/visible.
   *
   * @param waitSearcher true to wait for searcher
   * @return this CommitOptions instance for method chaining
   */
  public CommitOptions waitSearcher(boolean waitSearcher) {
    this.waitSearcher = waitSearcher;
    return this;
  }

  /**
   * Sets whether to open a new searcher as part of the commit.
   *
   * @param openSearcher true to open a new searcher
   * @return this CommitOptions instance for method chaining
   */
  public CommitOptions openSearcher(boolean openSearcher) {
    this.openSearcher = openSearcher;
    return this;
  }

  /**
   * Sets whether this should be a soft commit.
   *
   * @param softCommit true for soft commit, false for hard commit
   * @return this CommitOptions instance for method chaining
   */
  public CommitOptions softCommit(boolean softCommit) {
    this.softCommit = softCommit;
    return this;
  }

  /**
   * Sets whether to expunge deleted documents during optimize.
   *
   * @param expungeDeletes true to expunge deletes
   * @return this CommitOptions instance for method chaining
   */
  public CommitOptions expungeDeletes(boolean expungeDeletes) {
    this.expungeDeletes = expungeDeletes;
    return this;
  }

  /**
   * Sets the maximum number of segments to optimize down to. Only applies when using
   * ACTION.OPTIMIZE.
   *
   * @param maxOptimizeSegments maximum number of segments (must be >= 1)
   * @return this CommitOptions instance for method chaining
   * @throws IllegalArgumentException if maxOptimizeSegments &lt; 1
   */
  public CommitOptions maxOptimizeSegments(int maxOptimizeSegments) {
    if (maxOptimizeSegments < 1) {
      throw new IllegalArgumentException(
          "maxOptimizeSegments must be >= 1, got: " + maxOptimizeSegments);
    }
    this.maxOptimizeSegments = maxOptimizeSegments;
    return this;
  }

  // Getters

  public boolean getWaitSearcher() {
    return waitSearcher;
  }

  public boolean getOpenSearcher() {
    return openSearcher;
  }

  public boolean getSoftCommit() {
    return softCommit;
  }

  public boolean getExpungeDeletes() {
    return expungeDeletes;
  }

  public int getMaxOptimizeSegments() {
    return maxOptimizeSegments;
  }

  // Convenience factory methods

  /**
   * Creates CommitOptions for a standard hard commit.
   *
   * @return CommitOptions with default settings for hard commit
   */
  public static CommitOptions hardCommit() {
    return new CommitOptions().softCommit(false);
  }

  /**
   * Creates CommitOptions for a soft commit.
   *
   * @return CommitOptions configured for soft commit
   */
  public static CommitOptions softCommit() {
    return new CommitOptions().softCommit(true);
  }

  /**
   * Creates CommitOptions for optimization with default settings. Optimizes down to 1 segment and
   * expunges deletes.
   *
   * @return CommitOptions suitable for optimize operations
   */
  public static CommitOptions optimize() {
    return new CommitOptions().expungeDeletes(true).maxOptimizeSegments(1);
  }

  /**
   * Creates CommitOptions for optimization with specified max segments.
   *
   * @param maxSegments maximum number of segments to optimize to
   * @return CommitOptions configured for optimize with specified max segments
   */
  public static CommitOptions optimize(int maxSegments) {
    return optimize().maxOptimizeSegments(maxSegments);
  }

  @Override
  public String toString() {
    return "CommitOptions{"
        + "waitSearcher="
        + waitSearcher
        + ", openSearcher="
        + openSearcher
        + ", softCommit="
        + softCommit
        + ", expungeDeletes="
        + expungeDeletes
        + ", maxOptimizeSegments="
        + maxOptimizeSegments
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    CommitOptions that = (CommitOptions) o;

    return waitSearcher == that.waitSearcher
        && openSearcher == that.openSearcher
        && softCommit == that.softCommit
        && expungeDeletes == that.expungeDeletes
        && maxOptimizeSegments == that.maxOptimizeSegments;
  }

  @Override
  public int hashCode() {
    int result = (waitSearcher ? 1 : 0);
    result = 31 * result + (openSearcher ? 1 : 0);
    result = 31 * result + (softCommit ? 1 : 0);
    result = 31 * result + (expungeDeletes ? 1 : 0);
    result = 31 * result + maxOptimizeSegments;
    return result;
  }
}
