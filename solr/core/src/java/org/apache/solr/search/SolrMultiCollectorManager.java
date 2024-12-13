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
      for (Collectors collectors : reducableCollectors)
        reducableCollector.add(collectors.collectors[i]);
      results[i] = collectorManagers[i].reduce(reducableCollector);
    }
    return results;
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

  /** Wraps multiple collectors for processing */
  class Collectors implements Collector {

    private final Collector[] collectors;

    private Collectors() throws IOException {
      collectors = new Collector[collectorManagers.length];
      for (int i = 0; i < collectors.length; i++)
        collectors[i] = collectorManagers[i].newCollector();
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
        for (int i = 0; i < collectors.length; i++)
          leafCollectors[i] = collectors[i].getLeafCollector(context);
      }

      @Override
      public final void setScorer(final Scorable scorer) throws IOException {
        if (skipNonCompetitiveScores) {
          for (LeafCollector leafCollector : leafCollectors)
            if (leafCollector != null) leafCollector.setScorer(scorer);
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
        for (LeafCollector leafCollector : leafCollectors) {
          if (leafCollector != null) {
            leafCollector.collect(doc);
          }
        }
      }
    }
  }
}
