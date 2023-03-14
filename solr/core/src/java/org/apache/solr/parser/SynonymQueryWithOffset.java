package org.apache.solr.parser;

import java.io.IOException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;

/**
 * A query that treats multiple terms as synonyms.
 *
 * <p>For scoring purposes, this query tries to score the terms as if you had indexed them as one
 * term: it will match any of the terms but only invoke the similarity a single time, scoring the
 * sum of all term frequencies for the document.
 */
public final class SynonymQueryWithOffset extends Query {

  private SynonymQuery query;

  public SynonymQueryWithOffset(SynonymQuery query) {
    this.query = query;
  }

  @Override
  public String toString(String field) {
    return query.toString();
  }

  @Override
  public void visit(QueryVisitor visitor) {
    query.visit(visitor);
  }

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

