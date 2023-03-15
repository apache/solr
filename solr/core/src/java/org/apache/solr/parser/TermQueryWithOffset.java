package org.apache.solr.parser;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;

/**
 * A TermQuery that also has an Integer startOffset taken from the Token that
 * gave rise to the contained Term.
 */
public class TermQueryWithOffset extends TermQuery {

  private Integer startOffset = null;

  public TermQueryWithOffset(Term t, int startOffset) {
    super(t);
    this.startOffset = startOffset;
  }

  public Integer getStartOffset() {
    return startOffset;
  }

  public boolean hasStartOffset() {
    return startOffset != null;
  }

  /**
   * Equality is based on the contained Term and ignores the startOffset.
   * A TermQueryWithOffset will consider itself equal to a TermQuery as long as
   * the contained Terms are themselves equal.
   *
   * Note that this relationship is not currently symmetric. A TermQuery that
   * isn't a TermQueryWithOffset will not consider itself equal to any TermQueryWithOffset
   * because TermQuery.equals requires class equality. This could be fixed by updating
   * TermQuery.equals inside the lucene codebase.
   */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TermQuery)) {
      return false;
    }
    return getTerm().equals(((TermQuery) other).getTerm());
  }
}
