package org.apache.solr.parser;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.TermQuery;

public class TermQueryWithOffset extends TermQuery {

  private int offset = -1;

  public TermQueryWithOffset(Term t, int offset) {
    super(t);
    this.offset = offset;
  }

  public int getOffset() {
    return offset;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TermQuery)) {
      return false;
    }
    return getTerm().equals(((TermQuery) other).getTerm());
  }
}
