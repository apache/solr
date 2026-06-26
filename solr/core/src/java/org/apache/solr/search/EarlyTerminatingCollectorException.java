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
package org.apache.solr.search;

import java.util.Locale;

/**
 * Thrown by {@link EarlyTerminatingCollector} when the maximum to abort the scoring / collection
 * process early, when the specified maximum number of documents were collected.
 *
 * <p>This is a control-flow exception only — it is caught at the search entry point and its
 * formatted {@link #getMessage() message} plus {@link #getNumberCollected()}, {@link
 * #getNumberScanned()}, and {@link #getApproximateTotalHits(int)} are inspected, but the stack
 * trace itself is never used. We skip {@link #fillInStackTrace()} to keep early-termination cheap,
 * matching the Lucene {@code CollectionTerminatedException} and {@code
 * TimeLimitingBulkScorer.TimeExceededException} convention.
 */
@SuppressWarnings("serial")
public class EarlyTerminatingCollectorException extends RuntimeException {
  private static final long serialVersionUID = 5939241340763428118L;
  private final int numberScanned;
  private final int numberCollected;

  public EarlyTerminatingCollectorException(int numberCollected, int numberScanned) {
    super(
        String.format(
            Locale.ROOT,
            "maxHitsAllowed reached: %d documents collected out of %d scanned",
            numberCollected,
            numberScanned));
    assert numberCollected <= numberScanned : numberCollected + "<=" + numberScanned;
    assert 0 < numberCollected;

    this.numberCollected = numberCollected;
    this.numberScanned = numberScanned;
  }

  @Override
  public Throwable fillInStackTrace() {
    // never re-thrown so we can save the expensive stacktrace
    return this;
  }

  /**
   * The total number of documents in the index that were "scanned" by the index when collecting the
   * {@link #getNumberCollected()} documents that triggered this exception.
   *
   * <p>This number represents the sum of:
   *
   * <ul>
   *   <li>The total number of documents in all LeafReaders that were fully exhausted during
   *       collection
   *   <li>The id of the last doc collected in the last LeafReader consulted during collection.
   * </ul>
   */
  public int getNumberScanned() {
    return numberScanned;
  }

  /** The number of documents collected that resulted in early termination */
  public int getNumberCollected() {
    return numberCollected;
  }

  public long getApproximateTotalHits(int maxDocId) {
    if (numberScanned == maxDocId) {
      return numberCollected;
    } else {
      return (long) (maxDocId * ((double) numberCollected) / ((double) numberScanned));
    }
  }
}
