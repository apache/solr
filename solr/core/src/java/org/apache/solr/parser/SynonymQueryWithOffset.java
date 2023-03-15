package org.apache.solr.parser;

import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;

/**
 * Wraps a SynonymQuery and stores an Integer startOffset taken from the
 * Token that gave rise to the contained Terms.
 */
public final class SynonymQueryWithOffset extends Query {

  private SynonymQuery query;

  private Integer startOffset = null;

  public SynonymQueryWithOffset(SynonymQuery query, int offset) {
    this.query = query;
    this.startOffset = offset;
  }

  public Integer getStartOffset() {
    return startOffset;
  }

  public boolean hasStartOffset() {
    return startOffset != null;
  }

  @Override
  public String toString(String field) {
    return query.toString();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    query.visit(visitor);
  }

  /**
   * Equality is based on the contained SynonymQuery and ignores the startOffset.
   * A SynonymQueryWithOffset will consider itself equal to the SynonymQuery that it contains.
   *
   * Note that this relationship is not currently symmetric. A SynonymQuery that
   * will not consider itself equal to any SynonymQueryWithOffset
   * because SynonymQuery.equals() requires class equality. This could be fixed by updating
   * SynonymQuery.equals() inside the lucene codebase.
   */
  @Override
  public boolean equals(Object obj) {
    return query.equals(obj);
  }

  @Override
  public int hashCode() {
    return query.hashCode();
  }

  public Query rewrite(IndexReader reader) throws IOException {
    return query.rewrite(reader);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    return query.createWeight(searcher, scoreMode, boost);
  }

}

