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

import com.carrotsearch.hppc.IntFloatHashMap;
import com.carrotsearch.hppc.IntFloatMap;
import com.carrotsearch.hppc.IntIntHashMap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.component.QueryElevationComponent;
import org.apache.solr.request.SolrRequestInfo;

/* A TopDocsCollector used by reranking queries. */
public class ReRankCollector extends TopDocsCollector<ScoreDoc> {

  private final TopDocsCollector<? extends ScoreDoc> mainCollector;
  private final IndexSearcher searcher;
  private final int reRankDocs;
  private final int length;
  private final Set<BytesRef> boostedPriority; // order is the "priority"
  private final Rescorer reRankQueryRescorer;
  private final Sort sort;
  private final Query query;
  private ReRankScaler reRankScaler;
  private ReRankOperator reRankOperator;

  public ReRankCollector(
      int reRankDocs,
      int length,
      Rescorer reRankQueryRescorer,
      QueryCommand cmd,
      IndexSearcher searcher,
      Set<BytesRef> boostedPriority,
      ReRankScaler reRankScaler,
      ReRankOperator reRankOperator)
      throws IOException {
    this(reRankDocs, length, reRankQueryRescorer, cmd, searcher, boostedPriority);
    this.reRankScaler = reRankScaler;
    this.reRankOperator = reRankOperator;
  }

  public ReRankCollector(
      int reRankDocs,
      int length,
      Rescorer reRankQueryRescorer,
      QueryCommand cmd,
      IndexSearcher searcher,
      Set<BytesRef> boostedPriority)
      throws IOException {
    super(null);
    this.reRankDocs = reRankDocs;
    this.length = length;
    this.boostedPriority = boostedPriority;
    this.query = cmd.getQuery();
    Sort sort = cmd.getSort();
    int maxDoc = searcher.getIndexReader().maxDoc();
    int numHits = Math.min(Math.max(this.reRankDocs, length), maxDoc);
    if (sort == null) {
      this.sort = null;
      this.mainCollector = TopScoreDocCollector.create(numHits, cmd.getMinExactCount());
    } else {
      this.sort = sort = sort.rewrite(searcher);
      // scores are needed for Rescorer (regardless of whether sort needs it)
      this.mainCollector = TopFieldCollector.create(sort, numHits, cmd.getMinExactCount());
    }
    this.searcher = searcher;
    this.reRankQueryRescorer = reRankQueryRescorer;
  }

  @Override
  public int getTotalHits() {
    return mainCollector.getTotalHits();
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    return mainCollector.getLeafCollector(context);
  }

  @Override
  public ScoreMode scoreMode() {
    return this.mainCollector.scoreMode();
  }

  @Override
  public TopDocs topDocs(int start, int howMany) {

    try {

      TopDocs mainDocs = mainCollector.topDocs(0, Math.max(reRankDocs, length));

      if (mainDocs.totalHits.value == 0 || mainDocs.scoreDocs.length == 0) {
        return mainDocs;
      }

      if (sort != null) {
        TopFieldCollector.populateScores(mainDocs.scoreDocs, searcher, query);
      }

      ScoreDoc[] mainScoreDocs = mainDocs.scoreDocs;
      boolean zeroOutScores = reRankScaler != null && reRankScaler.scaleScores();
      IntFloatMap docToOriginalScore = new IntFloatHashMap();
      ScoreDoc[] mainScoreDocsClone = deepClone(mainScoreDocs, docToOriginalScore, zeroOutScores);
      ScoreDoc[] reRankScoreDocs = new ScoreDoc[Math.min(mainScoreDocs.length, reRankDocs)];
      System.arraycopy(mainScoreDocs, 0, reRankScoreDocs, 0, reRankScoreDocs.length);

      mainDocs.scoreDocs = reRankScoreDocs;

      // If we're scaling scores use the replace rescorer because we just want the re-rank score.
      TopDocs rescoredDocs;
      try {
        rescoredDocs =
            zeroOutScores // previously zero-ed out scores are to be replaced
                ? reRankScaler
                    .getReplaceRescorer()
                    .rescore(searcher, mainDocs, mainDocs.scoreDocs.length)
                : reRankQueryRescorer.rescore(searcher, mainDocs, mainDocs.scoreDocs.length);
      } catch (IncompleteRerankingException ex) {
        mainDocs.scoreDocs = mainScoreDocsClone;
        rescoredDocs = mainDocs;
      }

      // Lower howMany to return if we've collected fewer documents.
      howMany = Math.min(howMany, mainScoreDocs.length);

      if (boostedPriority != null) {
        SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
        Map<Object, Object> requestContext = null;
        if (info != null) {
          requestContext = info.getReq().getContext();
        }

        IntIntHashMap boostedDocs =
            QueryElevationComponent.getBoostDocs(
                (SolrIndexSearcher) searcher, boostedPriority, requestContext);

        float maxScore =
            rescoredDocs.scoreDocs.length == 0 ? Float.NaN : rescoredDocs.scoreDocs[0].score;
        Arrays.sort(
            rescoredDocs.scoreDocs, new BoostedComp(boostedDocs, mainDocs.scoreDocs, maxScore));
      }

      if (howMany == rescoredDocs.scoreDocs.length) {
        if (reRankScaler != null && reRankScaler.scaleScores()) {
          rescoredDocs.scoreDocs =
              reRankScaler.scaleScores(
                  mainScoreDocsClone, rescoredDocs.scoreDocs, reRankScoreDocs.length);
        }
      } else if (howMany > rescoredDocs.scoreDocs.length) {
        // We need to return more then we've reRanked, so create the combined page.
        ScoreDoc[] scoreDocs = new ScoreDoc[howMany];
        System.arraycopy(
            mainScoreDocs, 0, scoreDocs, 0, scoreDocs.length); // lay down the initial docs
        System.arraycopy(
            rescoredDocs.scoreDocs,
            0,
            scoreDocs,
            0,
            rescoredDocs.scoreDocs.length); // overlay the re-ranked docs.
        rescoredDocs.scoreDocs = scoreDocs;
        if (reRankScaler != null && reRankScaler.scaleScores()) {
          rescoredDocs.scoreDocs =
              reRankScaler.scaleScores(
                  mainScoreDocsClone, rescoredDocs.scoreDocs, reRankScoreDocs.length);
        }
      } else {
        // We've rescored more then we need to return.

        if (reRankScaler != null && reRankScaler.scaleScores()) {
          rescoredDocs.scoreDocs =
              reRankScaler.scaleScores(
                  mainScoreDocsClone, rescoredDocs.scoreDocs, rescoredDocs.scoreDocs.length);
        }
        ScoreDoc[] scoreDocs = new ScoreDoc[howMany];
        System.arraycopy(rescoredDocs.scoreDocs, 0, scoreDocs, 0, howMany);
        rescoredDocs.scoreDocs = scoreDocs;
      }
      return toRescoredDocs(rescoredDocs, docToOriginalScore);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }

  private TopDocs toRescoredDocs(TopDocs topDocs, IntFloatMap originalScores) {
    ScoreDoc[] scoreDocs = topDocs.scoreDocs;
    final RescoreDoc[] rescoredDocs = new RescoreDoc[scoreDocs.length];
    for (int i = 0; i < scoreDocs.length; i++) {
      rescoredDocs[i] = new RescoreDoc(scoreDocs[i], originalScores.get(scoreDocs[i].doc));
    }
    return new TopDocs(topDocs.totalHits, rescoredDocs);
  }

  private ScoreDoc[] deepClone(
      ScoreDoc[] scoreDocs, IntFloatMap originalScoreMap, boolean zeroOut) {
    ScoreDoc[] scoreDocs1 = new ScoreDoc[scoreDocs.length];
    for (int i = 0; i < scoreDocs.length; i++) {
      ScoreDoc scoreDoc = scoreDocs[i];
      if (scoreDoc != null) {
        originalScoreMap.put(scoreDoc.doc, scoreDoc.score);
        scoreDocs1[i] = new ScoreDoc(scoreDoc.doc, scoreDoc.score);
        if (zeroOut) {
          scoreDoc.score = 0f;
        }
      }
    }
    return scoreDocs1;
  }

  public static class BoostedComp implements Comparator<ScoreDoc> {
    IntFloatHashMap boostedMap;

    public BoostedComp(IntIntHashMap boostedDocs, ScoreDoc[] scoreDocs, float maxScore) {
      this.boostedMap = new IntFloatHashMap(boostedDocs.size() * 2);

      for (int i = 0; i < scoreDocs.length; i++) {
        final int idx;
        if ((idx = boostedDocs.indexOf(scoreDocs[i].doc)) >= 0) {
          boostedMap.put(scoreDocs[i].doc, maxScore + boostedDocs.indexGet(idx));
        } else {
          break;
        }
      }
    }

    @Override
    public int compare(ScoreDoc doc1, ScoreDoc doc2) {
      float score1 = doc1.score;
      float score2 = doc2.score;
      int idx;
      if ((idx = boostedMap.indexOf(doc1.doc)) >= 0) {
        score1 = boostedMap.indexGet(idx);
      }

      if ((idx = boostedMap.indexOf(doc2.doc)) >= 0) {
        score2 = boostedMap.indexGet(idx);
      }

      return -Float.compare(score1, score2);
    }
  }

  static class RescoreDoc extends ScoreDoc {
    public float originalScore;

    public RescoreDoc(ScoreDoc scoreDoc, float originalScore) {
      super(scoreDoc.doc, scoreDoc.score, scoreDoc.shardIndex);
      this.originalScore = originalScore;
    }
  }
}
