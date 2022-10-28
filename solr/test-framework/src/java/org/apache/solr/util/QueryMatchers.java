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
package org.apache.solr.util;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.internal.ReflectiveTypeFinder;

// TODO: This can be contributed to Lucene
public class QueryMatchers {
  private abstract static class SubTypeDiagnosingMatcher<Q extends Query>
      extends TypeSafeDiagnosingMatcher<Query> {
    private static final ReflectiveTypeFinder TYPE_FINDER =
        new ReflectiveTypeFinder("matchesExactType", 2, 0);

    public SubTypeDiagnosingMatcher() {
      super(TYPE_FINDER);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected final boolean matchesSafely(Query item, Description mismatchDescription) {
      // We know that we're getting a Q instead of a Query here because we use our own TypeFinder
      return matchesExactType((Q) item, mismatchDescription);
    }

    protected abstract boolean matchesExactType(Q item, Description mismatchDescription);
  }

  private QueryMatchers() {}

  /**
   * Matches is a Query object's toString result is equal to this query string. A useful shortcut
   * when constructing complex queries with deterministic string repr
   *
   * <p>Note: Do not use this in place of DisjunctionMaxQuery, but safe to use for the disjunct
   * clauses
   *
   * @param query the query string to match against
   */
  public static Matcher<Query> stringQuery(String query) {
    return new TypeSafeDiagnosingMatcher<>() {
      @Override
      protected boolean matchesSafely(Query item, Description mismatchDescription) {
        return is(query).matches(item.toString());
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(query);
      }
    };
  }

  public static Matcher<Query> termQuery(String field, String text) {
    // TODO Use a better matcher for more descriptive results?
    return is(new TermQuery(new Term(field, text)));
  }

  public static Matcher<Query> boosted(String field, String text, float boost) {
    return boosted(termQuery(field, text), boost);
  }

  public static Matcher<Query> boosted(Matcher<? extends Query> query, float boost) {
    return new SubTypeDiagnosingMatcher<BoostQuery>() {
      @Override
      protected boolean matchesExactType(BoostQuery item, Description mismatchDescription) {
        boolean match = true;
        mismatchDescription.appendText("was a BoostQuery ");
        if (!query.matches(item.getQuery())) {
          match = false;
          mismatchDescription.appendText("with" + item.getQuery());
        }
        if (boost != item.getBoost()) {
          match = false;
          mismatchDescription.appendText("with boost " + item.getBoost());
        }
        return match;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a BoostQuery ").appendDescriptionOf(query).appendText("^" + boost);
      }
    };
  }

  /**
   * Create a phrase query matcher with a whitespace delimited sequence of terms
   *
   * @param field the field the phrase query should match against
   * @param terms a whitespace delimited set of terms that should
   */
  public static Matcher<Query> phraseQuery(String field, String terms) {
    // TODO Use a better matcher for more descriptive results?
    return is(new PhraseQuery(field, terms.split(" ")));
  }

  private static Matcher<BooleanClause> shouldClause(Matcher<? extends Query> query) {
    return booleanClause(query, BooleanClause.Occur.SHOULD);
  }

  private static Matcher<BooleanClause> mustClause(Matcher<? extends Query> query) {
    return booleanClause(query, BooleanClause.Occur.MUST);
  }

  public static Matcher<BooleanClause> booleanClause(
      Matcher<? extends Query> query, BooleanClause.Occur occur) {
    return new TypeSafeDiagnosingMatcher<>() {
      @Override
      protected boolean matchesSafely(BooleanClause item, Description mismatchDescription) {
        boolean match = true;
        mismatchDescription.appendText("was a BooleanClause ");
        if (occur != item.getOccur()) {
          match = false;
          mismatchDescription.appendText("that " + item.getOccur().name() + " occur ");
        }
        if (!query.matches(item.getQuery())) {
          match = false;
          mismatchDescription.appendText("with " + item.getQuery());
          query.describeMismatch(item.getQuery(), mismatchDescription);
        }
        return match;
      }

      @Override
      public void describeTo(Description description) {
        description
            .appendText(occur.toString())
            .appendText("(")
            .appendDescriptionOf(query)
            .appendText(")");
      }
    };
  }

  // TODO Figure out Varargs
  public static Matcher<Query> booleanQuery(Matcher<Query> q1, BooleanClause.Occur occur) {
    return _booleanQuery(contains(booleanClause(q1, occur)));
  }

  public static Matcher<Query> booleanQuery(
      Matcher<Query> q1, Matcher<Query> q2, BooleanClause.Occur occur) {
    return _booleanQuery(containsInAnyOrder(booleanClause(q1, occur), booleanClause(q2, occur)));
  }

  public static Matcher<Query> booleanQuery(Matcher<Query> query) {
    return _booleanQuery(contains(shouldClause(query)));
  }

  public static Matcher<Query> booleanQuery(Matcher<Query> c1, Matcher<Query> c2) {
    return _booleanQuery(containsInAnyOrder(shouldClause(c1), shouldClause(c2)));
  }

  public static Matcher<Query> booleanQuery(
      Matcher<Query> c1, Matcher<Query> c2, Matcher<Query> c3) {
    return _booleanQuery(containsInAnyOrder(shouldClause(c1), shouldClause(c2), shouldClause(c3)));
  }

  public static Matcher<Query> booleanQuery(
      Matcher<Query> c1, Matcher<Query> c2, Matcher<Query> c3, Matcher<Query> c4) {
    return _booleanQuery(
        containsInAnyOrder(shouldClause(c1), shouldClause(c2), shouldClause(c3), shouldClause(c4)));
  }

  private static Matcher<Query> _booleanQuery(Matcher<Iterable<? extends BooleanClause>> matcher) {
    return new SubTypeDiagnosingMatcher<BooleanQuery>() {
      @Override
      protected boolean matchesExactType(BooleanQuery item, Description mismatchDescription) {
        if (matcher.matches(item.clauses())) return true;

        mismatchDescription.appendText("was a BooleanQuery with ");
        matcher.describeMismatch(item.clauses(), mismatchDescription);
        return false;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a BooleanQuery with ").appendDescriptionOf(matcher);
      }
    };
  }

  public static Matcher<Query> disjunctionOf(Matcher<Query> q1) {
    return disjunctionQuery(contains(q1));
  }

  public static Matcher<Query> disjunctionOf(Matcher<Query> q1, Matcher<Query> q2) {
    return disjunctionQuery(containsInAnyOrder(q1, q2));
  }

  public static Matcher<Query> disjunctionOf(
      Matcher<Query> q1, Matcher<Query> q2, Matcher<Query> q3) {
    return disjunctionQuery(containsInAnyOrder(q1, q2, q3));
  }

  public static Matcher<Query> disjunctionOf(
      Matcher<Query> q1, Matcher<Query> q2, Matcher<Query> q3, Matcher<Query> q4) {
    return disjunctionQuery(containsInAnyOrder(q1, q2, q3, q4));
  }

  private static Matcher<Query> disjunctionQuery(Matcher<Iterable<? extends Query>> disjuncts) {
    return new SubTypeDiagnosingMatcher<DisjunctionMaxQuery>() {
      @Override
      protected boolean matchesExactType(
          DisjunctionMaxQuery item, Description mismatchDescription) {
        if (disjuncts.matches(item.getDisjuncts())) return true;

        mismatchDescription.appendText("was a DisjunctionMaxQuery with ");
        disjuncts.describeMismatch(item.getDisjuncts(), mismatchDescription);
        return false;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("a DisjunctionMaxQuery with ").appendDescriptionOf(disjuncts);
      }
    };
  }
}
