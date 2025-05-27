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

import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/**
 * <code>TopDocsSlice</code> implements DocList based off provided <code>TopDocs</code>.
 *
 * @since solr 9.9
 */
public class TopDocsSlice extends DocSlice {

  private final TopDocs topDocs;

  private final boolean hasScores;

  /**
   * Construct a slice off topDocs
   *
   * @param offset starting offset for this range of docs
   * @param len length of results
   * @param matches total number of matches for the query
   */
  public TopDocsSlice(
      int offset,
      int len,
      TopDocs topDocs,
      long matches,
      boolean hasScores,
      float maxScore,
      TotalHits.Relation matchesRelation) {
    super(offset, len, null, null, matches, maxScore, matchesRelation);
    this.topDocs = topDocs;
    this.hasScores = hasScores;
    super.docLength = topDocs.scoreDocs.length;
  }

  @Override
  public TopDocsSlice subset(int offset, int len) {
    if (this.offset == offset && this.len == len) {
      return this;
    }

    // if we didn't store enough (and there was more to store)
    // then we can't take a subset.
    int requestedEnd = offset + len;
    if (requestedEnd > docLength && this.matches > docLength) {
      return null;
    }
    int realEndDoc = Math.min(requestedEnd, docLength);
    int realLen = Math.max(realEndDoc - offset, 0);
    if (this.offset == offset && this.len == realLen) {
      return this;
    }
    return new TopDocsSlice(
        offset, realLen, topDocs, matches, hasScores, maxScore, matchesRelation);
  }

  @Override
  public boolean hasScores() {
    return topDocs != null && hasScores;
  }

  @Override
  public DocIterator iterator() {
    boolean hasMatchScore =
        topDocs.scoreDocs.length > 0 && topDocs.scoreDocs[0] instanceof ReRankCollector.RescoreDoc;
    if (hasMatchScore) {
      return new ReRankedTopDocsIterator();
    } else {
      return new TopDocsIterator();
    }
  }

  class TopDocsIterator implements DocIterator {
    int pos = offset;
    final int end = offset + len;

    @Override
    public boolean hasNext() {
      return pos < end;
    }

    @Override
    public Integer next() {
      return nextDoc();
    }

    /** The remove operation is not supported by this Iterator. */
    @Override
    public void remove() {
      throw new UnsupportedOperationException(
          "The remove  operation is not supported by this Iterator.");
    }

    @Override
    public int nextDoc() {
      return topDocs.scoreDocs[pos++].doc;
    }

    @Override
    public float score() {
      return topDocs.scoreDocs[pos - 1].score;
    }
  }

  class ReRankedTopDocsIterator extends TopDocsIterator {

    @Override
    public Float matchScore() {
      try {
        return ((ReRankCollector.RescoreDoc) topDocs.scoreDocs[pos - 1]).matchScore;
      } catch (ClassCastException e) {
        return null;
      }
    }
  }
}
