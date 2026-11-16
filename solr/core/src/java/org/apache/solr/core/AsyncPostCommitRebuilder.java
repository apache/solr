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

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a rebuild task (e.g. a suggester or spellchecker's {@code buildOnCommit} rebuild) off a
 * dedicated background thread instead of inline within a {@link SolrEventListener#newSearcher}
 * callback, so a slow rebuild doesn't block the commit that triggered it.
 *
 * <p>A commit's {@code newSearcher()} listeners run on {@code SolrCore}'s single-threaded {@code
 * searcherExecutor}, which {@code DirectUpdateHandler2.commit()} blocks on with no timeout - so any
 * listener doing expensive, synchronous work there (rebuilding a suggester or spellchecker
 * dictionary, for example) blocks every commit until it finishes. This class lets that work happen
 * on its own thread instead, while preserving two safety properties a naive {@code
 * executor.execute(rebuildTask)} would not:
 *
 * <ul>
 *   <li><strong>Coalescing.</strong> If a rebuild is already running when a newer commit fires,
 *       that commit's rebuild is skipped entirely rather than queued, so a burst of commits can't
 *       pile up an ever-growing backlog of increasingly stale rebuilds - results simply lag behind
 *       by at most the duration of one rebuild.
 *   <li><strong>Safe searcher lifetime.</strong> The specific searcher instance a rebuild reads
 *       from is pinned open for the rebuild's duration by taking a real reference through {@link
 *       SolrCore#getNewestSearcher}, the same mechanism request handlers use via {@code
 *       core.getSearcher()} - not just the searcher's raw {@code IndexReader}. {@code
 *       SolrCore.registerSearcher} decrefs (and, once that hits zero, closes - including the
 *       searcher's caches and its directory reference) the previously-current searcher as soon as a
 *       newer one is registered, regardless of anything else independently holding the old
 *       searcher's {@code IndexReader} open; an extra {@code IndexReader.incRef()} does not prevent
 *       that, only a genuine extra reference through the {@code RefCounted} wrapper does. A slow
 *       rebuild easily outlives a single commit interval, so a second commit landing mid-rebuild is
 *       not an edge case, it's the expected case under load.
 * </ul>
 */
public class AsyncPostCommitRebuilder {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrCore core;
  private final String name;
  private final ExecutorService executor;
  private final AtomicBoolean inProgress = new AtomicBoolean(false);
  private volatile long lastDurationMs = -1L;

  /**
   * @param core the core this rebuilder belongs to, used to safely pin a specific searcher open via
   *     {@link SolrCore#getNewestSearcher}
   * @param name a short, unique, human-readable identifier (e.g. {@code
   *     "suggesterBuildOnCommit-mySuggester"}) used both as the background thread's name and in log
   *     messages, so logs/thread dumps can be tied back to the specific configured component this
   *     rebuilder belongs to
   */
  public AsyncPostCommitRebuilder(SolrCore core, String name) {
    this.core = core;
    this.name = name;
    this.executor = ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory(name));
  }

  /**
   * If no rebuild is currently in progress, and {@code newSearcher} hasn't already been superseded
   * by a newer searcher, runs {@code rebuildTask} on this rebuilder's dedicated executor while
   * {@code newSearcher} is pinned open. Otherwise this is a no-op (logged, not treated as an error)
   * - both skip conditions mean the rebuild this call would have started is unnecessary: either a
   * fresher one is already running, or the searcher this one would have targeted is already out of
   * date.
   *
   * @param newSearcher the searcher {@code rebuildTask} should run against - normally the one just
   *     passed to the caller's own {@code newSearcher()} listener callback. {@code rebuildTask} is
   *     expected to close over this same instance.
   * @param rebuildTask the rebuild logic to run; any exception it throws is caught and logged here,
   *     never rethrown, since this always runs off the caller's thread. Callers that do their own
   *     error handling/logging (recommended, so log messages can be specific to what's being
   *     rebuilt) won't normally trigger this path.
   */
  public void maybeRunAsync(SolrIndexSearcher newSearcher, Runnable rebuildTask) {
    if (!inProgress.compareAndSet(false, true)) {
      log.info("Skipping {}: a rebuild is already in progress", name);
      return;
    }
    RefCounted<SolrIndexSearcher> searcherRef = core.getNewestSearcher(false);
    if (searcherRef == null || searcherRef.get() != newSearcher) {
      // Either the core is closing, or a later commit's newSearcher() event already ran (on
      // SolrCore's single-threaded searcherExecutor) and registered an even newer searcher
      // before we got here - in which case newSearcher is already stale and rebuilding from it
      // now would be wasted work anyway; the next commit (or this core closing) takes it from
      // here.
      if (searcherRef != null) {
        searcherRef.decref();
      }
      log.info(
          "Skipping {}: newSearcher was already superseded before the async rebuild could start",
          name);
      inProgress.set(false);
      return;
    }
    final long startNanos = System.nanoTime();
    log.info("Starting {}", name);
    try {
      executor.execute(
          () -> {
            try {
              rebuildTask.run();
            } catch (Exception e) {
              log.error("Exception during {}: ", name, e);
            } finally {
              lastDurationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
              inProgress.set(false);
              searcherRef.decref();
              log.info("Finished {} in {}ms", name, lastDurationMs);
            }
          });
    } catch (RuntimeException e) {
      // Most commonly RejectedExecutionException: core is closing concurrently with this
      // commit. Whatever the cause, the task above never started, so its finally block never
      // ran - undo what was set up for it here instead of leaving inProgress stuck true (which
      // would silently stop all future rebuilds) and leaking the pinned searcher reference.
      log.warn("Failed to submit {}", name, e);
      inProgress.set(false);
      searcherRef.decref();
    }
  }

  public boolean isInProgress() {
    return inProgress.get();
  }

  /**
   * Duration of the last rebuild run through this rebuilder, in ms; -1 if none has finished yet.
   */
  public long getLastDurationMs() {
    return lastDurationMs;
  }

  public void shutdown() {
    ExecutorUtil.shutdownAndAwaitTermination(executor);
  }
}
