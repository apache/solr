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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FilterScorable;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

/**
 * A {@link CollectorManager} implements which wrap a set of {@link CollectorManager} as {@link
 * MultiCollector} acts for {@link Collector}.
 *
 * <p>Cross-cutting search-chain features (max-hits early termination, segment-terminate-early,
 * post-filter, cancellation) are handled by {@link SolrCollectorManagers.ChainCM} which wraps this
 * manager. This class only composes its child managers.
 */
public class SolrMultiCollectorManager
    implements CollectorManager<SolrMultiCollectorManager.Collectors, Object[]> {

  private final CollectorManager<Collector, ?>[] collectorManagers;

  @SafeVarargs
  @SuppressWarnings({"varargs", "unchecked"})
  public SolrMultiCollectorManager(
      final CollectorManager<? extends Collector, ?>... collectorManagers) {
    if (collectorManagers.length < 1) {
      throw new IllegalArgumentException("There must be at least one collector");
    }
    this.collectorManagers = (CollectorManager[]) collectorManagers;
  }

  // TODO: could Lucene's MultiCollector permit reuse of its logic?
  public static ScoreMode scoreMode(Collector[] collectors) {
    ScoreMode scoreMode = null;
    for (Collector collector : collectors) {
      if (scoreMode == null) {
        scoreMode = collector.scoreMode();
      } else if (scoreMode != collector.scoreMode()) {
        return ScoreMode.COMPLETE;
      }
    }
    return scoreMode;
  }

  @Override
  public Collectors newCollector() throws IOException {
    return new Collectors();
  }

  @Override
  public Object[] reduce(Collection<Collectors> reducableCollectors) throws IOException {
    final int size = reducableCollectors.size();
    final Object[] results = new Object[collectorManagers.length];
    for (int i = 0; i < collectorManagers.length; i++) {
      final List<Collector> reducableCollector = new ArrayList<>(size);
      for (Collectors collectors : reducableCollectors) {
        reducableCollector.add(collectors.collectors[i]);
      }
      results[i] = collectorManagers[i].reduce(reducableCollector);
    }
    return results;
  }

  /** Wraps multiple collectors for processing */
  class Collectors implements Collector {

    private final Collector[] collectors;

    private Collectors() throws IOException {
      collectors = new Collector[collectorManagers.length];
      for (int i = 0; i < collectors.length; i++) {
        collectors[i] = collectorManagers[i].newCollector();
      }
    }

    @Override
    public final LeafCollector getLeafCollector(final LeafReaderContext context)
        throws IOException {
      return new LeafCollectors(context, scoreMode() == ScoreMode.TOP_SCORES);
    }

    @Override
    public final ScoreMode scoreMode() {
      return SolrMultiCollectorManager.scoreMode(collectors);
    }

    /**
     * Wraps multiple leaf collectors and delegates collection across each one
     *
     * @lucene.internal
     */
    private class LeafCollectors implements LeafCollector {

      private final LeafCollector[] leafCollectors;
      private final boolean skipNonCompetitiveScores;

      private LeafCollectors(final LeafReaderContext context, boolean skipNonCompetitiveScores)
          throws IOException {
        this.skipNonCompetitiveScores = skipNonCompetitiveScores;
        leafCollectors = new LeafCollector[collectors.length];
        int terminated = 0;
        for (int i = 0; i < collectors.length; i++) {
          try {
            leafCollectors[i] = collectors[i].getLeafCollector(context);
          } catch (CollectionTerminatedException e) {
            // Per-child handling matches Lucene's MultiCollector: drop this child for the
            // rest of the leaf and continue collecting for the others. Only when ALL children
            // have terminated do we propagate the exception.
            leafCollectors[i] = null;
            terminated++;
          }
        }
        if (terminated == leafCollectors.length && leafCollectors.length > 0) {
          throw new CollectionTerminatedException();
        }
      }

      @Override
      public final void setScorer(final Scorable scorer) throws IOException {
        if (skipNonCompetitiveScores) {
          // TOP_SCORES: aggregate per-child setMinCompetitiveScore so the underlying scorer
          // only skips a doc when ALL active children agree it's non-competitive — mirroring
          // Lucene's MultiCollector.MinCompetitiveScoreAwareScorable. Without this, the last
          // child to call setMinCompetitiveScore overwrites previous values and can cause
          // over-aggressive skipping (e.g. ReRankCollector + MaxScoreCollector with
          // minExactCount tightly bounded).
          if (leafCollectors.length == 1) {
            LeafCollector lc = leafCollectors[0];
            if (lc != null) {
              lc.setScorer(scorer);
            }
          } else {
            float[] minScores = new float[leafCollectors.length];
            for (int i = 0; i < leafCollectors.length; i++) {
              LeafCollector lc = leafCollectors[i];
              if (lc != null) {
                lc.setScorer(new MinCompetitiveScoreAwareScorable(scorer, i, minScores));
              }
            }
          }
        } else {
          FilterScorable fScorer =
              new FilterScorable(scorer) {
                @Override
                public void setMinCompetitiveScore(float minScore) throws IOException {
                  // Ignore calls to setMinCompetitiveScore so that if we wrap two
                  // collectors and one of them wants to skip low-scoring hits, then
                  // the other collector still sees all hits.
                }
              };
          for (LeafCollector leafCollector : leafCollectors) {
            if (leafCollector != null) {
              leafCollector.setScorer(fScorer);
            }
          }
        }
      }

      @Override
      public final void collect(final int doc) throws IOException {
        // Matches Lucene MultiCollector.MultiLeafCollector#collect exactly: keep the per-iteration
        // body small (load slot, null-check, call) so the JIT can inline the child collect call,
        // and only walk the array to detect "all terminated" inside the rare catch branch.
        for (int i = 0; i < leafCollectors.length; i++) {
          LeafCollector leafCollector = leafCollectors[i];
          if (leafCollector != null) {
            try {
              leafCollector.collect(doc);
            } catch (CollectionTerminatedException e) {
              leafCollectors[i].finish();
              leafCollectors[i] = null;
              if (allLeavesTerminated()) {
                throw new CollectionTerminatedException();
              }
            }
          }
        }
      }

      private boolean allLeavesTerminated() {
        for (LeafCollector lc : leafCollectors) {
          if (lc != null) {
            return false;
          }
        }
        return true;
      }

      @Override
      public final void finish() throws IOException {
        // Matches Lucene MultiCollector.MultiLeafCollector#finish: forward end-of-leaf to
        // every child that hasn't already been finished via per-doc termination above.
        for (LeafCollector leafCollector : leafCollectors) {
          if (leafCollector != null) {
            leafCollector.finish();
          }
        }
      }
    }
  }

  /**
   * Mirrors {@code Lucene MultiCollector.MinCompetitiveScoreAwareScorable}: tracks per-child
   * minimum competitive scores and propagates the smallest of them to the underlying scorer, so the
   * scorer only skips a doc when every wrapped collector agrees it's non-competitive.
   */
  private static final class MinCompetitiveScoreAwareScorable extends FilterScorable {
    private final int idx;
    private final float[] minScores;

    MinCompetitiveScoreAwareScorable(Scorable in, int idx, float[] minScores) {
      super(in);
      this.idx = idx;
      this.minScores = minScores;
    }

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
      if (minScore > minScores[idx]) {
        minScores[idx] = minScore;
        in.setMinCompetitiveScore(minScore());
      }
    }

    private float minScore() {
      float min = Float.MAX_VALUE;
      for (int i = 0; i < minScores.length; i++) {
        if (minScores[i] < min) {
          min = minScores[i];
        }
      }
      return min;
    }
  }
}
