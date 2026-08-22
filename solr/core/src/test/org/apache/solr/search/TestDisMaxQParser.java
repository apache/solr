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
import org.apache.lucene.search.TermQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

/** Unit tests for {@link DisMaxQParser} internals that do not need a running core. */
public class TestDisMaxQParser extends SolrTestCaseJ4 {

  private static TermQuery term(String field, String text) {
    return new TermQuery(new Term(field, text));
  }

  // SOLR-18314: isEffectivelySingleTerm decides whether a parsed pf phrase query collapses to a
  // single analyzed term (so the phrase boost is redundant and should be dropped). It must return
  // true only when every field's clause is a single term; any clause with two or more terms means
  // the phrase boost adds a real adjacency constraint and must be kept.
  @Test
  public void testIsEffectivelySingleTermIsTrueForSingleTermShapes() {
    // A bare single term.
    assertTrue(DisMaxQParser.isEffectivelySingleTerm(term("subject", "wireless")));

    // The parser wraps each field's clause in a BoostQuery; it must be unwrapped.
    assertTrue(
        DisMaxQParser.isEffectivelySingleTerm(new BoostQuery(term("subject", "wireless"), 10f)));

    // A one-term phrase is just a term boost in disguise.
    assertTrue(DisMaxQParser.isEffectivelySingleTerm(new PhraseQuery("subject", "wireless")));

    // A one-position MultiPhraseQuery (e.g. a single term with synonyms) is likewise single-term.
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

    // A two-position MultiPhraseQuery (e.g. a multi-word synonym expansion).
    MultiPhraseQuery twoPositions =
        new MultiPhraseQuery.Builder()
            .add(new Term("subject", "wi"))
            .add(new Term("subject", "fi"))
            .build();
    assertFalse(DisMaxQParser.isEffectivelySingleTerm(twoPositions));

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

    BooleanQuery booleanQuery =
        new BooleanQuery.Builder()
            .add(term("subject", "wireless"), BooleanClause.Occur.SHOULD)
            .build();
    assertFalse(DisMaxQParser.isEffectivelySingleTerm(booleanQuery));

    // null is a valid input (parse() can return it) and must not throw.
    assertFalse(DisMaxQParser.isEffectivelySingleTerm(null));
  }
}
