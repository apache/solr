package org.apache.solr.hamcrest;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;

public class QueryMatchers {
  private QueryMatchers() {}

  public static Matcher<Query> termQuery(String field, String text) {
    // TODO Use a better matcher for more descriptive results?
    return is(new TermQuery(new Term(field, text)));
  }

  public static Matcher<Query> boostQuery(String field, String text, float boost) {
    return boostQuery(termQuery(field, text), boost);
  }

  public static Matcher<Query> boostQuery(Matcher<? extends Query> query, float boost) {
    return allOf(hasProperty("query", query), hasProperty("boost", is(boost)));
  }

  public static Matcher<Query> phraseQuery(String field, String... terms) {
    // TODO Use a better matcher for more descriptive results?
    return is(new PhraseQuery(field, terms));
  }

  public static Matcher<BooleanClause> booleanClause(Matcher<? extends Query> query) {
    return booleanClause(query, BooleanClause.Occur.SHOULD);
  }

  public static Matcher<BooleanClause> booleanClause(Matcher<? extends Query> query, BooleanClause.Occur occur) {
    return new TypeSafeDiagnosingMatcher<BooleanClause>() {
      @Override
      protected boolean matchesSafely(BooleanClause item, Description mismatchDescription) {
        boolean match = true;
        mismatchDescription.appendText("was a BooleanClause ");
        if (is(occur).matches(item.getOccur())) {
          match = false;
          mismatchDescription.appendText("that " + item.getOccur().name() + " occur ");
        }
        if (!query.matches(item.getQuery())) {
          match = false;
          mismatchDescription.appendText("with query " + item.getQuery());
        }
        return match;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a BooleanClause that " + occur.name() + " occur with query ")
            .appendDescriptionOf(query);
      }
    };
  }

  // TODO Figure out Varargs
  public static Matcher<Query> booleanQuery(Matcher<Query> query) {
    return _booleanQuery(containsInAnyOrder(booleanClause(query)));
  }

  public static Matcher<Query> booleanQuery(Matcher<Query> c1, Matcher<Query> c2) {
    return _booleanQuery(containsInAnyOrder(booleanClause(c1), booleanClause(c2)));
  }

  public static Matcher<Query> booleanQuery(Matcher<Query> c1, Matcher<Query> c2, Matcher<Query> c3) {
    return _booleanQuery(containsInAnyOrder(booleanClause(c1), booleanClause(c2), booleanClause(c3)));
  }

  public static Matcher<Query> booleanQuery(Matcher<Query> c1, Matcher<Query> c2, Matcher<Query> c3, Matcher<Query> c4) {
    return _booleanQuery(containsInAnyOrder(booleanClause(c1), booleanClause(c2), booleanClause(c3), booleanClause(c4)));
  }

  private static Matcher<Query> _booleanQuery(Matcher<Iterable<? extends BooleanClause>> matcher) {
    return new TypeSafeDiagnosingMatcher<Query>() {
      @Override
      protected boolean matchesSafely(Query item, Description mismatchDescription) {
        if (item instanceof BooleanQuery) {
          BooleanQuery bq = (BooleanQuery) item;
          if (matcher.matches(bq.clauses())) return true;
          mismatchDescription.appendText(" was a BooleanQuery with ");
          matcher.describeMismatch(bq.clauses(), mismatchDescription);
        } else {
          classMismatch(mismatchDescription, item);
        }
        return false;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(" a BooleanQuery with ")
            .appendDescriptionOf(matcher);
      }
    };
  }

  public static Matcher<Query> disjunctionQuery(Matcher<Query> q1, Matcher<Query> q2) {
    return _disjunctionQuery(containsInAnyOrder(q1, q2));
  }

  public static Matcher<Query> disjunctionQuery(Matcher<Query> q1, Matcher<Query> q2, Matcher<Query> q3, Matcher<Query> q4) {
    return _disjunctionQuery(containsInAnyOrder(q1, q2, q3, q4));
  }

  private static Matcher<Query> _disjunctionQuery(Matcher<Iterable<? extends Query>> disjuncts) {
    return new TypeSafeDiagnosingMatcher<Query>() {
      @Override
      protected boolean matchesSafely(Query item, Description mismatchDescription) {
        if (item instanceof DisjunctionMaxQuery) {
          DisjunctionMaxQuery dmq = (DisjunctionMaxQuery) item;
          if (disjuncts.matches(dmq.getDisjuncts())) return true;
          mismatchDescription.appendText(" was a DisjunctionMaxQuery with ");
          disjuncts.describeMismatch(dmq.getDisjuncts(), mismatchDescription);
        } else {
          classMismatch(mismatchDescription, item);
        }
        return false;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a DisjunctionMaxQuery with ")
            .appendDescriptionOf(disjuncts);
      }
    };
  }

  private static void classMismatch(Description mismatchDescription, Object item) {
    mismatchDescription.appendText("was a ")
        .appendText(item.getClass().getSimpleName())
        .appendText(" ")
        .appendValue(item);
  }
}
