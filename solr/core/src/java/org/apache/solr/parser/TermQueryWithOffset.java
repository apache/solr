package org.apache.solr.parser;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.TermQuery;

public class TermQueryWithOffset extends TermQuery {
  public TermQueryWithOffset(Term t) {
    super(t);
  }

  public TermQueryWithOffset(Term t, TermStates states) {
    super(t, states);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TermQuery)) {
      return false;
    }
    return getTerm().equals(((TermQuery) other).getTerm());
  }
}
