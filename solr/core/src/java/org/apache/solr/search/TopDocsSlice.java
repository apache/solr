package org.apache.solr.search;

import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

public class TopDocsSlice extends DocSlice {

  private final TopDocs topDocs;

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
      float maxScore,
      TotalHits.Relation matchesRelation) {
    super(offset, len, null, null, matches, maxScore, matchesRelation);
    this.topDocs = topDocs;
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
    return new TopDocsSlice(offset, realLen, topDocs, matches, maxScore, matchesRelation);
  }

  @Override
  public boolean hasScores() {
    return topDocs != null;
  }

  @Override
  public DocIterator iterator() {
    return new DocIterator() {
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

      @Override
      public Float matchScore() {
        if (topDocs.scoreDocs[pos - 1] instanceof ReRankCollector.RescoreDoc) {
          return ((ReRankCollector.RescoreDoc) topDocs.scoreDocs[pos - 1]).matchScore;
        } else {
          return null;
        }
      }
    };
  }
}
