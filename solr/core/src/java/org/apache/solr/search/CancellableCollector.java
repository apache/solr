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

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;

/** Allows a query to be cancelled */
public class CancellableCollector implements Collector {

  /**
   * Thrown when a query gets cancelled. This is a control-flow exception only — it is caught by the
   * searcher and never inspected, so we skip filling in the stack trace to keep cancellation cheap
   * (matches Lucene {@code CollectionTerminatedException} and {@code
   * TimeLimitingBulkScorer.TimeExceededException}).
   */
  @SuppressWarnings("serial")
  public static class QueryCancelledException extends RuntimeException {
    @Override
    public Throwable fillInStackTrace() {
      // never re-thrown so we can save the expensive stacktrace
      return this;
    }
  }

  private final Collector collector;
  private final AtomicBoolean isQueryCancelled;

  public CancellableCollector(Collector collector) {
    this(collector, new AtomicBoolean());
  }

  /**
   * Creates a {@link CancellableCollector} that polls a caller-supplied cancellation flag. Multiple
   * per-slice collectors can share the same {@link AtomicBoolean} so that a single {@link
   * #cancel()} on any instance is observed by all of them — required for cancelling a query that is
   * fanned out across parallel segment slices.
   */
  public CancellableCollector(Collector collector, AtomicBoolean cancellationFlag) {
    Objects.requireNonNull(
        collector, "Internal collector not provided but wrapper collector accessed");
    Objects.requireNonNull(cancellationFlag, "cancellationFlag must not be null");
    this.collector = collector;
    this.isQueryCancelled = cancellationFlag;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {

    if (isQueryCancelled.get()) {
      throw new QueryCancelledException();
    }

    return new FilterLeafCollector(collector.getLeafCollector(context)) {
      int collectCount = 0;

      @Override
      public void collect(int doc) throws IOException {
        // To avoid polling the AtomicBoolean (volatile read) on every collected document,
        // which acts as a memory barrier and prevents JIT optimizations,
        // we check it only every 1024 docs. This keeps the hot loop fast while maintaining
        // responsive cancellation.
        if ((collectCount++ & 0x3FF) == 0) {
          if (isQueryCancelled.get()) {
            throw new QueryCancelledException();
          }
        }
        in.collect(doc);
      }
    };
  }

  @Override
  public ScoreMode scoreMode() {
    return collector.scoreMode();
  }

  public void cancel() {
    isQueryCancelled.compareAndSet(false, true);
  }

  public Collector getInternalCollector() {
    return collector;
  }
}
