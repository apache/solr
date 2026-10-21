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

import java.util.List;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

/** Unit tests for {@link DisMaxQParser} internals that do not need a running core. */
public class TestDisMaxQParser extends SolrTestCase {

  private static TermQuery term(String field, String text) {
    return new TermQuery(new Term(field, text));
  }

  private static BooleanQuery should(Query... queries) {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (Query query : queries) {
      builder.add(query, BooleanClause.Occur.SHOULD);
    }
    return builder.build();
  }

  // SOLR-18314: isEffectivelySingleTerm decides whether a parsed pf phrase query collapses to a
  // single analyzed position (so the phrase boost imposes no adjacency and should be dropped). It
  // must return true only when every field's clause is a single position; any clause spanning two
  // or more positions means the phrase boost adds a real adjacency constraint and must be kept.
  @Test
  public void testIsEffectivelySingleTermIsTrueForSingleTermShapes() {
    // A bare single term.
    assertTrue(DisMaxQParser.isEffectivelySingleTerm(term("subject", "wireless")));

    // The parser wraps each field's clause in a BoostQuery; it must be unwrapped.
    assertTrue(
        DisMaxQParser.isEffectivelySingleTerm(new BoostQuery(term("subject", "wireless"), 10f)));

    // Synonyms at a single position: synonymQueryStyle=as_same_term (the default) builds a
    // SynonymQuery and pick_best a DisjunctionMaxQuery.
    assertTrue(
        DisMaxQParser.isEffectivelySingleTerm(
            new SynonymQuery.Builder("subject")
                .addTerm(new Term("subject", "tv"))
                .addTerm(new Term("subject", "television"))
                .build()));
    assertTrue(
        DisMaxQParser.isEffectivelySingleTerm(
            new BoostQuery(
                new DisjunctionMaxQuery(
                    List.of(term("subject", "tv"), term("subject", "television")), 0f),
                10f)));

    // Degenerate one-position phrases (QueryBuilder emits a TermQuery or SynonymQuery instead, but
    // these are equally single-term).
    assertTrue(DisMaxQParser.isEffectivelySingleTerm(new PhraseQuery("subject", "wireless")));
    MultiPhraseQuery singlePosition =
        new MultiPhraseQuery.Builder().add(new Term("subject", "wireless")).build();
    assertTrue(DisMaxQParser.isEffectivelySingleTerm(singlePosition));

    // A DisjunctionMaxQuery whose every disjunct is single-term is single-term across fields.
    DisjunctionMaxQuery allSingle =
        new DisjunctionMaxQuery(
            List.of(
                new BoostQuery(term("subject", "wireless"), 10f),
                new BoostQuery(term("features", "wireless"), 5f)),
            0.1f);
    assertTrue(DisMaxQParser.isEffectivelySingleTerm(allSingle));
  }

  @Test
  public void testIsEffectivelySingleTermIsFalseForMultiTermShapes() {
    // A genuine two-term phrase (e.g. WordDelimiter splitting "wi-fi" into "wi fi").
    assertFalse(DisMaxQParser.isEffectivelySingleTerm(new PhraseQuery("subject", "wi", "fi")));

    // A two-position MultiPhraseQuery (e.g. synonyms within a multi-token phrase).
    MultiPhraseQuery twoPositions =
        new MultiPhraseQuery.Builder()
            .add(new Term("subject", "wi"))
            .add(new Term("subject", "fi"))
            .build();
    assertFalse(DisMaxQParser.isEffectivelySingleTerm(twoPositions));

    // A sloppy graph phrase (e.g. a multi-word synonym) is a disjunction of per-path phrases; any
    // multi-term path keeps the boost.
    assertFalse(
        DisMaxQParser.isEffectivelySingleTerm(
            should(term("subject", "wifi"), new PhraseQuery(100, "subject", "wi", "fi"))));

    // Any field with a multi-term clause keeps the boost, even if other fields are single-term.
    DisjunctionMaxQuery mixed =
        new DisjunctionMaxQuery(
            List.of(
                new BoostQuery(term("subject", "wireless"), 10f),
                new BoostQuery(new PhraseQuery("features", "wi", "fi"), 5f)),
            0.1f);
    assertFalse(DisMaxQParser.isEffectivelySingleTerm(mixed));
  }

  @Test
  public void testIsEffectivelySingleTermIsFalseForUnrecognizedShapes() {
    // Unrecognized shapes conservatively keep the boost rather than silently dropping it.
    assertFalse(DisMaxQParser.isEffectivelySingleTerm(new MatchAllDocsQuery()));

    // synonymQueryStyle=as_distinct_terms builds a BooleanQuery for single-position synonyms;
    // eDisMax's minClauseSize gate keeps it too, so the parsers agree.
    assertFalse(
        DisMaxQParser.isEffectivelySingleTerm(
            should(term("subject", "tv"), term("subject", "television"))));
    BooleanQuery required =
        new BooleanQuery.Builder()
            .add(term("subject", "wireless"), BooleanClause.Occur.MUST)
            .build();
    assertFalse(DisMaxQParser.isEffectivelySingleTerm(required));

    // An empty disjunction is not vacuously single-term.
    assertFalse(DisMaxQParser.isEffectivelySingleTerm(new DisjunctionMaxQuery(List.of(), 0f)));

    // null is a valid input (parse() can return it) and must not throw.
    assertFalse(DisMaxQParser.isEffectivelySingleTerm(null));
  }
}
