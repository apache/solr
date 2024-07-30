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
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.solr.search.join.GraphQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiThreadedSearcher {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final SolrIndexSearcher searcher;

  public MultiThreadedSearcher(SolrIndexSearcher searcher) {
    this.searcher = searcher;
  }

  SearchResult searchCollectorManagers(
      int len,
      QueryCommand cmd,
      Query query,
      boolean needTopDocs,
      boolean needMaxScore,
      boolean needDocSet)
      throws IOException {
    Collection<CollectorManager<Collector, Object>> collectors = new ArrayList<>();

    int firstCollectorsSize = 0;

    final int firstTopDocsCollectorIndex;
    if (needTopDocs) {
      firstTopDocsCollectorIndex = firstCollectorsSize;
      firstCollectorsSize++;
    } else {
      firstTopDocsCollectorIndex = -1;
    }

    final int firstMaxScoreCollectorIndex;
    if (needMaxScore) {
      firstMaxScoreCollectorIndex = firstCollectorsSize;
      firstCollectorsSize++;
    } else {
      firstMaxScoreCollectorIndex = -1;
    }

    Collector[] firstCollectors = new Collector[firstCollectorsSize];

    if (needTopDocs) {

      collectors.add(new TopDocsCM(len, cmd, firstCollectors, firstTopDocsCollectorIndex));
    }
    if (needMaxScore) {
      collectors.add(new MaxScoreCM(firstCollectors, firstMaxScoreCollectorIndex));
    }
    if (needDocSet) {
      int maxDoc = searcher.getRawReader().maxDoc();
      log.error("raw read max={}", searcher.getRawReader().maxDoc());

      collectors.add(new DocSetCM(maxDoc));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    CollectorManager<Collector, Object>[] colls = collectors.toArray(new CollectorManager[0]);
    SolrMultiCollectorManager manager = new SolrMultiCollectorManager(colls);
    Object[] ret;
    try {
      ret = searcher.search(query, manager);
    } catch (Exception ex) {
      if (ex instanceof RuntimeException
          && ex.getCause() != null
          && ex.getCause() instanceof ExecutionException
          && ex.getCause().getCause() != null
          && ex.getCause().getCause() instanceof RuntimeException) {
        throw (RuntimeException) ex.getCause().getCause();
      } else {
        throw ex;
      }
    }

    ScoreMode scoreMode = SolrMultiCollectorManager.scoreMode(firstCollectors);

    return new SearchResult(scoreMode, ret);
  }

  static boolean allowMT(DelegatingCollector postFilter, QueryCommand cmd, Query query) {
    if (postFilter != null
        || cmd.getSegmentTerminateEarly()
        || cmd.getTimeAllowed() > 0
        || !cmd.getMultiThreaded()) {
      return false;
    } else {
      MTCollectorQueryCheck allowMT = new MTCollectorQueryCheck();
      query.visit(allowMT);
      return allowMT.allowed();
    }
  }

  /**
   * A {@link QueryVisitor} that recurses through the query tree, determining if all queries support
   * multi-threaded collecting.
   */
  private static class MTCollectorQueryCheck extends QueryVisitor {

    private QueryVisitor subVisitor = this;

    private boolean allowMt(Query query) {
      if (query instanceof RankQuery || query instanceof GraphQuery || query instanceof JoinQuery) {
        return false;
      }
      return true;
    }

    @Override
    public void consumeTerms(Query query, Term... terms) {
      if (!allowMt(query)) {
        subVisitor = EMPTY_VISITOR;
      }
    }

    @Override
    public void consumeTermsMatching(
        Query query, String field, Supplier<ByteRunAutomaton> automaton) {
      if (!allowMt(query)) {
        subVisitor = EMPTY_VISITOR;
      } else {
        super.consumeTermsMatching(query, field, automaton);
      }
    }

    @Override
    public void visitLeaf(Query query) {
      if (!allowMt(query)) {
        subVisitor = EMPTY_VISITOR;
      }
    }

    @Override
    public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
      return subVisitor;
    }

    public boolean allowed() {
      return subVisitor != EMPTY_VISITOR;
    }
  }

  static class MaxScoreResult {
    final float maxScore;

    public MaxScoreResult(float maxScore) {
      this.maxScore = maxScore;
    }
  }

  static class FixedBitSetCollector extends SimpleCollector {
    @SuppressWarnings("JdkObsolete")
    private final LinkedList<FixedBitSet> bitSets = new LinkedList<>();

    @SuppressWarnings("JdkObsolete")
    private final LinkedList<Integer> skipWords = new LinkedList<>();

    @SuppressWarnings("JdkObsolete")
    private final LinkedList<Integer> skipBits = new LinkedList<>();

    FixedBitSetCollector() {}

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      this.bitSets.add(null); // lazy allocate when collecting document(s)
      this.skipWords.add(context.docBase / 64);
      this.skipBits.add(context.docBase % 64);
    }

    @Override
    public void collect(int doc) throws IOException {
      FixedBitSet bitSet = this.bitSets.getLast();
      final int idx = this.skipBits.getLast() + doc;

      final int numWords = FixedBitSet.bits2words(idx + 1); // +1 to ensure minimum 1 word

      if (bitSet == null) {
        this.bitSets.removeLast();
        bitSet = new FixedBitSet(numWords * 64);
        this.bitSets.addLast(bitSet);

      } else if (bitSet.getBits().length < numWords) {
        FixedBitSet smallerBitSet = this.bitSets.removeLast();
        bitSet = new FixedBitSet(numWords * 64);
        bitSet.xor(smallerBitSet);
        this.bitSets.addLast(bitSet);
      }

      bitSet.set(idx);
    }

    void update(FixedBitSet allBitSet) {
      final long[] allBits = allBitSet.getBits();
      for (int bs_idx = 0; bs_idx < this.bitSets.size(); ++bs_idx) {
        final FixedBitSet itBitSet = this.bitSets.get(bs_idx);
        if (itBitSet != null) {
          final int skipWords = this.skipWords.get(bs_idx);
          final long[] itBits = itBitSet.getBits();
          for (int idx = 0; idx < itBits.length && skipWords + idx < allBits.length; ++idx) {
            allBits[skipWords + idx] ^= itBits[idx];
          }
        }
      }
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }
  }

  static class SearchResult {
    final ScoreMode scoreMode;
    private final Object[] result;

    public SearchResult(ScoreMode scoreMode, Object[] result) {
      this.scoreMode = scoreMode;
      this.result = result;
    }

    public TopDocsResult getTopDocsResult() {
      for (Object res : result) {
        if (res instanceof TopDocsResult) {
          return (TopDocsResult) res;
        }
      }
      return null;
    }

    public float getMaxScore(int totalHits) {
      if (totalHits > 0) {
        for (Object res : result) {
          if (res instanceof MaxScoreResult) {
            return ((MaxScoreResult) res).maxScore;
          }
        }
        return Float.NaN;
      } else {
        return 0.0f;
      }
    }

    public FixedBitSet getFixedBitSet() {
      for (Object res : result) {
        if (res instanceof FixedBitSet) {
          return (FixedBitSet) res;
        }
      }
      return null;
    }
  }

  private static class MaxScoreCM implements CollectorManager<Collector, Object> {
    private final Collector[] firstCollectors;
    private final int firstMaxScoreCollectorIndex;

    public MaxScoreCM(Collector[] firstCollectors, int firstMaxScoreCollectorIndex) {
      this.firstCollectors = firstCollectors;
      this.firstMaxScoreCollectorIndex = firstMaxScoreCollectorIndex;
    }

    @Override
    public Collector newCollector() throws IOException {
      MaxScoreCollector collector = new MaxScoreCollector();
      if (firstCollectors[firstMaxScoreCollectorIndex] == null) {
        firstCollectors[firstMaxScoreCollectorIndex] = collector;
      }
      return collector;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Object reduce(Collection collectors) throws IOException {

      MaxScoreCollector collector;
      float maxScore = 0.0f;
      for (Iterator var4 = collectors.iterator();
          var4.hasNext();
          maxScore = Math.max(maxScore, collector.getMaxScore())) {
        collector = (MaxScoreCollector) var4.next();
      }

      return new MaxScoreResult(maxScore);
    }
  }

  private static class DocSetCM implements CollectorManager<Collector, Object> {
    private final int maxDoc;

    public DocSetCM(int maxDoc) {
      this.maxDoc = maxDoc;
    }

    @Override
    public Collector newCollector() throws IOException {
      // TODO: add to firstCollectors here? or if not have comment w.r.t. why not adding
      return new FixedBitSetCollector();
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    public Object reduce(Collection collectors) throws IOException {
      final FixedBitSet reduced = new FixedBitSet(maxDoc);
      for (Object collector : collectors) {
        if (collector instanceof FixedBitSetCollector) {
          FixedBitSetCollector fixedBitSetCollector = (FixedBitSetCollector) collector;
          fixedBitSetCollector.update(reduced);
        }
      }
      return reduced;
    }
  }

  private class TopDocsCM implements CollectorManager<Collector, Object> {
    private final int len;
    private final QueryCommand cmd;
    private final Collector[] firstCollectors;
    private final int firstTopDocsCollectorIndex;

    public TopDocsCM(
        int len, QueryCommand cmd, Collector[] firstCollectors, int firstTopDocsCollectorIndex) {
      this.len = len;
      this.cmd = cmd;
      this.firstCollectors = firstCollectors;
      this.firstTopDocsCollectorIndex = firstTopDocsCollectorIndex;
    }

    @Override
    public Collector newCollector() throws IOException {
      @SuppressWarnings("rawtypes")
      TopDocsCollector collector = searcher.buildTopDocsCollector(len, cmd);
      if (firstCollectors[firstTopDocsCollectorIndex] == null) {
        firstCollectors[firstTopDocsCollectorIndex] = collector;
      }
      return collector;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Object reduce(Collection collectors) throws IOException {

      TopDocs[] topDocs = new TopDocs[collectors.size()];

      int totalHits = -1;
      int i = 0;

      Collector collector;
      for (Object o : collectors) {
        collector = (Collector) o;
        if (collector instanceof TopDocsCollector) {
          TopDocs td = ((TopDocsCollector) collector).topDocs(0, len);
          assert td != null : Arrays.asList(topDocs);
          topDocs[i++] = td;
        }
      }

      TopDocs mergedTopDocs = null;

      if (topDocs.length > 0 && topDocs[0] != null) {
        if (topDocs[0] instanceof TopFieldDocs) {
          TopFieldDocs[] topFieldDocs =
              Arrays.copyOf(topDocs, topDocs.length, TopFieldDocs[].class);
          mergedTopDocs = TopFieldDocs.merge(searcher.weightSort(cmd.getSort()), len, topFieldDocs);
        } else {
          mergedTopDocs = TopDocs.merge(0, len, topDocs);
        }
        totalHits = (int) mergedTopDocs.totalHits.value;
      }
      return new TopDocsResult(mergedTopDocs, totalHits);
    }
  }

  static class TopDocsResult {
    final TopDocs topDocs;
    final int totalHits;

    public TopDocsResult(TopDocs topDocs, int totalHits) {
      this.topDocs = topDocs;
      this.totalHits = totalHits;
    }
  }
}
