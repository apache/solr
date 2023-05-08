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
package org.apache.solr.parser;

import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostAttribute;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.tests.analysis.CannedBinaryTokenStream;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockSynonymFilter;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.solr.SolrTestCase;

/** Adapted from org.apache.lucene.util.TestQueryBuilder */
public class SolrQueryBuilderTest extends SolrTestCase {

  public void testTerm() {
    TermQueryWithOffset expected = new TermQueryWithOffset(new Term("field", "test"), 0);
    SolrQueryBuilder builder = new SolrQueryBuilder(new MockAnalyzer(random()));
    assertEquals(expected, builder.createBooleanQuery("field", "test"));
  }

  public void testBoolean() {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(new TermQueryWithOffset(new Term("field", "foo"), 0), BooleanClause.Occur.SHOULD);
    expected.add(new TermQueryWithOffset(new Term("field", "bar"), 4), BooleanClause.Occur.SHOULD);
    SolrQueryBuilder builder = new SolrQueryBuilder(new MockAnalyzer(random()));
    assertEquals(expected.build(), builder.createBooleanQuery("field", "foo bar"));
  }

  public void testBooleanMust() {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(new TermQueryWithOffset(new Term("field", "foo"), 0), BooleanClause.Occur.MUST);
    expected.add(new TermQueryWithOffset(new Term("field", "bar"), 4), BooleanClause.Occur.MUST);
    SolrQueryBuilder builder = new SolrQueryBuilder(new MockAnalyzer(random()));
    assertEquals(
        expected.build(), builder.createBooleanQuery("field", "foo bar", BooleanClause.Occur.MUST));
  }

  public void testMinShouldMatchNone() {
    SolrQueryBuilder builder = new SolrQueryBuilder(new MockAnalyzer(random()));
    assertEquals(
        builder.createBooleanQuery("field", "one two three four"),
        builder.createMinShouldMatchQuery("field", "one two three four", 0f));
  }

  public void testMinShouldMatchAll() {
    SolrQueryBuilder builder = new SolrQueryBuilder(new MockAnalyzer(random()));
    assertEquals(
        builder.createBooleanQuery("field", "one two three four", BooleanClause.Occur.MUST),
        builder.createMinShouldMatchQuery("field", "one two three four", 1f));
  }

  public void testMinShouldMatch() {
    BooleanQuery.Builder expectedB = new BooleanQuery.Builder();
    expectedB.add(new TermQueryWithOffset(new Term("field", "one"), 0), BooleanClause.Occur.SHOULD);
    expectedB.add(new TermQueryWithOffset(new Term("field", "two"), 4), BooleanClause.Occur.SHOULD);
    expectedB.add(
        new TermQueryWithOffset(new Term("field", "three"), 8), BooleanClause.Occur.SHOULD);
    expectedB.add(
        new TermQueryWithOffset(new Term("field", "four"), 14), BooleanClause.Occur.SHOULD);
    expectedB.setMinimumNumberShouldMatch(0);
    Query expected = expectedB.build();

    SolrQueryBuilder builder = new SolrQueryBuilder(new MockAnalyzer(random()));
    // assertEquals(expected, builder.createMinShouldMatchQuery("field", "one two three four",
    // 0.1f));
    // assertEquals(expected, builder.createMinShouldMatchQuery("field", "one two three four",
    // 0.24f));

    expectedB.setMinimumNumberShouldMatch(1);
    expected = expectedB.build();
    assertEquals(expected, builder.createMinShouldMatchQuery("field", "one two three four", 0.25f));
    assertEquals(expected, builder.createMinShouldMatchQuery("field", "one two three four", 0.49f));

    expectedB.setMinimumNumberShouldMatch(2);
    expected = expectedB.build();
    assertEquals(expected, builder.createMinShouldMatchQuery("field", "one two three four", 0.5f));
    assertEquals(expected, builder.createMinShouldMatchQuery("field", "one two three four", 0.74f));

    expectedB.setMinimumNumberShouldMatch(3);
    expected = expectedB.build();
    assertEquals(expected, builder.createMinShouldMatchQuery("field", "one two three four", 0.75f));
    assertEquals(expected, builder.createMinShouldMatchQuery("field", "one two three four", 0.99f));
  }

  public void testPhraseQueryPositionIncrements() throws Exception {
    PhraseQuery.Builder pqBuilder = new PhraseQuery.Builder();
    pqBuilder.add(new Term("field", "1"), 0);
    pqBuilder.add(new Term("field", "2"), 2);
    PhraseQueryWithOffset expected = new PhraseQueryWithOffset(pqBuilder.build(), 0);
    CharacterRunAutomaton stopList =
        new CharacterRunAutomaton(new RegExp("[sS][tT][oO][pP]").toAutomaton());

    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false, stopList);

    SolrQueryBuilder builder = new SolrQueryBuilder(analyzer);
    assertEquals(expected, builder.createPhraseQuery("field", "1 stop 2"));
  }

  public void testEmpty() {
    SolrQueryBuilder builder = new SolrQueryBuilder(new MockAnalyzer(random()));
    assertNull(builder.createBooleanQuery("field", ""));
  }

  /** adds synonym of "dog" for "dogs", and synonym of "cavy" for "guinea pig". */
  static class MockSynonymAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      MockTokenizer tokenizer = new MockTokenizer();
      return new TokenStreamComponents(tokenizer, new MockSynonymFilter(tokenizer));
    }
  }

  /** simple synonyms test */
  public void testSynonyms() throws Exception {
    SynonymQueryWithOffset expected =
        new SynonymQueryWithOffset(
            new SynonymQuery.Builder("field")
                .addTerm(new Term("field", "dogs"))
                .addTerm(new Term("field", "dog"))
                .build(),
            0);
    SolrQueryBuilder builder = new SolrQueryBuilder(new MockSynonymAnalyzer());
    assertEquals(expected, builder.createBooleanQuery("field", "dogs"));
    assertEquals(expected, builder.createPhraseQuery("field", "dogs"));
    assertEquals(expected, builder.createBooleanQuery("field", "dogs", BooleanClause.Occur.MUST));
    assertEquals(expected, builder.createPhraseQuery("field", "dogs"));
  }

  /** forms multiphrase query */
  public void testSynonymsPhrase() throws Exception {
    MultiPhraseQuery.Builder expectedBuilder = new MultiPhraseQuery.Builder();
    expectedBuilder.add(new Term("field", "old"));
    expectedBuilder.add(new Term[] {new Term("field", "dogs"), new Term("field", "dog")});
    SolrQueryBuilder builder = new SolrQueryBuilder(new MockSynonymAnalyzer());
    assertEquals(expectedBuilder.build(), builder.createPhraseQuery("field", "old dogs"));
  }

  /** forms graph query */
  public void testMultiWordSynonymsPhrase() {
    Query expected =
        new BooleanQuery.Builder()
            .add(new PhraseQuery("field", "guinea", "pig"), BooleanClause.Occur.SHOULD)
            .add(
                new TermQueryWithOffset(new Term("field", "cavy"), null),
                BooleanClause.Occur.SHOULD)
            .build();

    SolrQueryBuilder queryBuilder = new SolrQueryBuilder(new MockSynonymAnalyzer());
    assertEquals(expected, queryBuilder.createPhraseQuery("field", "guinea pig"));
  }

  public void testMultiWordSynonymsPhraseWithSlop() throws Exception {
    BooleanQuery expected =
        new BooleanQuery.Builder()
            .add(
                new PhraseQuery.Builder()
                    .setSlop(4)
                    .add(new Term("field", "guinea"))
                    .add(new Term("field", "pig"))
                    .build(),
                BooleanClause.Occur.SHOULD)
            .add(new TermQueryWithOffset(new Term("field", "cavy"), 0), BooleanClause.Occur.SHOULD)
            .build();
    SolrQueryBuilder queryBuilder = new SolrQueryBuilder(new MockSynonymAnalyzer());
    assertEquals(expected, queryBuilder.createPhraseQuery("field", "guinea pig", 4));
  }

  /** forms graph query */
  public void testMultiWordSynonymsBoolean() throws Exception {
    for (BooleanClause.Occur occur :
        new BooleanClause.Occur[] {BooleanClause.Occur.SHOULD, BooleanClause.Occur.MUST}) {
      Query syn1 =
          new BooleanQuery.Builder()
              .add(
                  new TermQueryWithOffset(new Term("field", "guinea"), null),
                  BooleanClause.Occur.MUST)
              .add(
                  new TermQueryWithOffset(new Term("field", "pig"), null), BooleanClause.Occur.MUST)
              .build();
      Query syn2 = new TermQueryWithOffset(new Term("field", "cavy"), null);

      BooleanQuery synQuery =
          new BooleanQuery.Builder()
              .add(syn1, BooleanClause.Occur.SHOULD)
              .add(syn2, BooleanClause.Occur.SHOULD)
              .build();

      BooleanQuery expectedGraphQuery = new BooleanQuery.Builder().add(synQuery, occur).build();

      SolrQueryBuilder queryBuilder = new SolrQueryBuilder(new MockSynonymAnalyzer());
      assertEquals(
          expectedGraphQuery, queryBuilder.createBooleanQuery("field", "guinea pig", occur));

      BooleanQuery expectedBooleanQuery =
          new BooleanQuery.Builder()
              .add(synQuery, occur)
              .add(new TermQueryWithOffset(new Term("field", "story"), null), occur)
              .build();
      assertEquals(
          expectedBooleanQuery,
          queryBuilder.createBooleanQuery("field", "guinea pig story", occur));

      expectedBooleanQuery =
          new BooleanQuery.Builder()
              .add(new TermQueryWithOffset(new Term("field", "the"), null), occur)
              .add(synQuery, occur)
              .add(new TermQueryWithOffset(new Term("field", "story"), null), occur)
              .build();
      assertEquals(
          expectedBooleanQuery,
          queryBuilder.createBooleanQuery("field", "the guinea pig story", occur));

      expectedBooleanQuery =
          new BooleanQuery.Builder()
              .add(new TermQueryWithOffset(new Term("field", "the"), null), occur)
              .add(synQuery, occur)
              .add(new TermQueryWithOffset(new Term("field", "story"), null), occur)
              .add(synQuery, occur)
              .build();
      assertEquals(
          expectedBooleanQuery,
          queryBuilder.createBooleanQuery("field", "the guinea pig story guinea pig", occur));
    }
  }

  /** forms graph query */
  public void testMultiWordPhraseSynonymsBoolean() throws Exception {
    for (BooleanClause.Occur occur :
        new BooleanClause.Occur[] {BooleanClause.Occur.SHOULD, BooleanClause.Occur.MUST}) {
      Query syn1 =
          new PhraseQueryWithOffset(
              new PhraseQuery.Builder()
                  .add(new Term("field", "guinea"))
                  .add(new Term("field", "pig"))
                  .build(),
              null);
      Query syn2 = new TermQueryWithOffset(new Term("field", "cavy"), null);

      BooleanQuery synQuery =
          new BooleanQuery.Builder()
              .add(syn1, BooleanClause.Occur.SHOULD)
              .add(syn2, BooleanClause.Occur.SHOULD)
              .build();
      BooleanQuery expectedGraphQuery = new BooleanQuery.Builder().add(synQuery, occur).build();
      SolrQueryBuilder queryBuilder = new SolrQueryBuilder(new MockSynonymAnalyzer());
      queryBuilder.setAutoGenerateMultiTermSynonymsPhraseQuery(true);
      assertEquals(
          expectedGraphQuery, queryBuilder.createBooleanQuery("field", "guinea pig", occur));

      BooleanQuery expectedBooleanQuery =
          new BooleanQuery.Builder()
              .add(synQuery, occur)
              .add(new TermQueryWithOffset(new Term("field", "story"), null), occur)
              .build();
      assertEquals(
          expectedBooleanQuery,
          queryBuilder.createBooleanQuery("field", "guinea pig story", occur));

      expectedBooleanQuery =
          new BooleanQuery.Builder()
              .add(new TermQueryWithOffset(new Term("field", "the"), null), occur)
              .add(synQuery, occur)
              .add(new TermQueryWithOffset(new Term("field", "story"), null), occur)
              .build();
      assertEquals(
          expectedBooleanQuery,
          queryBuilder.createBooleanQuery("field", "the guinea pig story", occur));

      expectedBooleanQuery =
          new BooleanQuery.Builder()
              .add(new TermQueryWithOffset(new Term("field", "the"), null), occur)
              .add(synQuery, occur)
              .add(new TermQueryWithOffset(new Term("field", "story"), null), occur)
              .add(synQuery, occur)
              .build();
      assertEquals(
          expectedBooleanQuery,
          queryBuilder.createBooleanQuery("field", "the guinea pig story guinea pig", occur));
    }
  }

  protected static class SimpleCJKTokenizer extends Tokenizer {
    private CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    public SimpleCJKTokenizer() {
      super();
    }

    @Override
    public final boolean incrementToken() throws IOException {
      int ch = input.read();
      if (ch < 0) return false;
      clearAttributes();
      termAtt.setEmpty().append((char) ch);
      return true;
    }
  }

  private static class SimpleCJKAnalyzer extends Analyzer {
    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      return new TokenStreamComponents(new SimpleCJKTokenizer());
    }
  }

  public void testCJKTerm() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer();

    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(new TermQueryWithOffset(new Term("field", "中"), 0), BooleanClause.Occur.SHOULD);
    expected.add(new TermQueryWithOffset(new Term("field", "国"), 1), BooleanClause.Occur.SHOULD);

    SolrQueryBuilder builder = new SolrQueryBuilder(analyzer);
    assertEquals(expected.build(), builder.createBooleanQuery("field", "中国"));
  }

  public void testCJKPhrase() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer();

    PhraseQueryWithOffset expected =
        new PhraseQueryWithOffset(new PhraseQuery("field", "中", "国"), 0);

    SolrQueryBuilder builder = new SolrQueryBuilder(analyzer);
    assertEquals(expected, builder.createPhraseQuery("field", "中国"));
  }

  public void testCJKSloppyPhrase() throws Exception {
    // individual CJK chars as terms
    SimpleCJKAnalyzer analyzer = new SimpleCJKAnalyzer();

    PhraseQueryWithOffset expected =
        new PhraseQueryWithOffset(new PhraseQuery(3, "field", "中", "国"), 0);

    SolrQueryBuilder builder = new SolrQueryBuilder(analyzer);
    assertEquals(expected, builder.createPhraseQuery("field", "中国", 3));
  }

  /** adds synonym of "國" for "国". */
  protected static class MockCJKSynonymFilter extends TokenFilter {
    CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
    boolean addSynonym = false;

    public MockCJKSynonymFilter(TokenStream input) {
      super(input);
    }

    @Override
    public final boolean incrementToken() throws IOException {
      if (addSynonym) { // inject our synonym
        clearAttributes();
        termAtt.setEmpty().append("國");
        posIncAtt.setPositionIncrement(0);
        addSynonym = false;
        return true;
      }

      if (input.incrementToken()) {
        addSynonym = termAtt.toString().equals("国");
        return true;
      } else {
        return false;
      }
    }
  }

  static class MockCJKSynonymAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer = new SimpleCJKTokenizer();
      return new TokenStreamComponents(tokenizer, new MockCJKSynonymFilter(tokenizer));
    }
  }

  /** simple CJK synonym test */
  public void testCJKSynonym() throws Exception {
    SynonymQueryWithOffset expected =
        new SynonymQueryWithOffset(
            new SynonymQuery.Builder("field")
                .addTerm(new Term("field", "国"))
                .addTerm(new Term("field", "國"))
                .build(),
            0);
    SolrQueryBuilder builder = new SolrQueryBuilder(new MockCJKSynonymAnalyzer());
    assertEquals(expected, builder.createBooleanQuery("field", "国"));
    assertEquals(expected, builder.createPhraseQuery("field", "国"));
    assertEquals(expected, builder.createBooleanQuery("field", "国", BooleanClause.Occur.MUST));
  }

  /** synonyms with default OR operator */
  public void testCJKSynonymsOR() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(new TermQueryWithOffset(new Term("field", "中"), 0), BooleanClause.Occur.SHOULD);
    SynonymQueryWithOffset inner =
        new SynonymQueryWithOffset(
            new SynonymQuery.Builder("field")
                .addTerm(new Term("field", "国"))
                .addTerm(new Term("field", "國"))
                .build(),
            1);
    expected.add(inner, BooleanClause.Occur.SHOULD);
    SolrQueryBuilder builder = new SolrQueryBuilder(new MockCJKSynonymAnalyzer());
    assertEquals(expected.build(), builder.createBooleanQuery("field", "中国"));
  }

  /** more complex synonyms with default OR operator */
  public void testCJKSynonymsOR2() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(new TermQueryWithOffset(new Term("field", "中"), 0), BooleanClause.Occur.SHOULD);
    SynonymQueryWithOffset inner =
        new SynonymQueryWithOffset(
            new SynonymQuery.Builder("field")
                .addTerm(new Term("field", "国"))
                .addTerm(new Term("field", "國"))
                .build(),
            1);
    expected.add(inner, BooleanClause.Occur.SHOULD);
    SynonymQueryWithOffset inner2 =
        new SynonymQueryWithOffset(
            new SynonymQuery.Builder("field")
                .addTerm(new Term("field", "国"))
                .addTerm(new Term("field", "國"))
                .build(),
            2);
    expected.add(inner2, BooleanClause.Occur.SHOULD);
    SolrQueryBuilder builder = new SolrQueryBuilder(new MockCJKSynonymAnalyzer());
    assertEquals(expected.build(), builder.createBooleanQuery("field", "中国国"));
  }

  /** synonyms with default AND operator */
  public void testCJKSynonymsAND() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(new TermQueryWithOffset(new Term("field", "中"), 0), BooleanClause.Occur.MUST);
    SynonymQueryWithOffset inner =
        new SynonymQueryWithOffset(
            new SynonymQuery.Builder("field")
                .addTerm(new Term("field", "国"))
                .addTerm(new Term("field", "國"))
                .build(),
            1);
    expected.add(inner, BooleanClause.Occur.MUST);
    SolrQueryBuilder builder = new SolrQueryBuilder(new MockCJKSynonymAnalyzer());
    assertEquals(
        expected.build(), builder.createBooleanQuery("field", "中国", BooleanClause.Occur.MUST));
  }

  /** more complex synonyms with default AND operator */
  public void testCJKSynonymsAND2() throws Exception {
    BooleanQuery.Builder expected = new BooleanQuery.Builder();
    expected.add(new TermQueryWithOffset(new Term("field", "中"), 0), BooleanClause.Occur.MUST);
    SynonymQueryWithOffset inner =
        new SynonymQueryWithOffset(
            new SynonymQuery.Builder("field")
                .addTerm(new Term("field", "国"))
                .addTerm(new Term("field", "國"))
                .build(),
            1);
    expected.add(inner, BooleanClause.Occur.MUST);
    SynonymQueryWithOffset inner2 =
        new SynonymQueryWithOffset(
            new SynonymQuery.Builder("field")
                .addTerm(new Term("field", "国"))
                .addTerm(new Term("field", "國"))
                .build(),
            2);
    expected.add(inner2, BooleanClause.Occur.MUST);
    SolrQueryBuilder builder = new SolrQueryBuilder(new MockCJKSynonymAnalyzer());
    assertEquals(
        expected.build(), builder.createBooleanQuery("field", "中国国", BooleanClause.Occur.MUST));
  }

  /** forms multiphrase query */
  public void testCJKSynonymsPhrase() throws Exception {
    MultiPhraseQuery.Builder expectedBuilder = new MultiPhraseQuery.Builder();
    expectedBuilder.add(new Term("field", "中"));
    expectedBuilder.add(new Term[] {new Term("field", "国"), new Term("field", "國")});
    SolrQueryBuilder builder = new SolrQueryBuilder(new MockCJKSynonymAnalyzer());
    assertEquals(expectedBuilder.build(), builder.createPhraseQuery("field", "中国"));
    expectedBuilder.setSlop(3);
    assertEquals(expectedBuilder.build(), builder.createPhraseQuery("field", "中国", 3));
  }

  public void testNoTermAttribute() {
    // Can't use MockTokenizer because it adds TermAttribute and we don't want that
    Analyzer analyzer =
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            return new TokenStreamComponents(
                new Tokenizer() {
                  boolean wasReset = false;

                  @Override
                  public void reset() throws IOException {
                    super.reset();
                    assertFalse(wasReset);
                    wasReset = true;
                  }

                  @Override
                  public boolean incrementToken() throws IOException {
                    assertTrue(wasReset);
                    return false;
                  }
                });
          }
        };
    SolrQueryBuilder builder = new SolrQueryBuilder(analyzer);
    assertNull(builder.createBooleanQuery("field", "whatever"));
  }

  public void testMaxBooleanClause() throws Exception {
    int size = 34;
    CannedBinaryTokenStream.BinaryToken[] tokens = new CannedBinaryTokenStream.BinaryToken[size];
    BytesRef term1 = new BytesRef("ff");
    BytesRef term2 = new BytesRef("f");
    for (int i = 0; i < size; ) {
      if (i % 2 == 0) {
        tokens[i] = new CannedBinaryTokenStream.BinaryToken(term2, 1, 1);
        tokens[i + 1] = new CannedBinaryTokenStream.BinaryToken(term1, 0, 2);
        i += 2;
      } else {
        tokens[i] = new CannedBinaryTokenStream.BinaryToken(term2, 1, 1);
        i++;
      }
    }
    SolrQueryBuilder qb = new SolrQueryBuilder(null);
    try (TokenStream ts = new CannedBinaryTokenStream(tokens)) {
      expectThrows(
          IndexSearcher.TooManyClauses.class,
          () -> qb.analyzeGraphBoolean("", ts, BooleanClause.Occur.MUST));
    }
    try (TokenStream ts = new CannedBinaryTokenStream(tokens)) {
      expectThrows(
          IndexSearcher.TooManyClauses.class,
          () -> qb.analyzeGraphBoolean("", ts, BooleanClause.Occur.SHOULD));
    }
    try (TokenStream ts = new CannedBinaryTokenStream(tokens)) {
      expectThrows(IndexSearcher.TooManyClauses.class, () -> qb.analyzeGraphPhrase(ts, "", 0));
    }
  }

  private static final class MockBoostTokenFilter extends TokenFilter {

    final BoostAttribute boostAtt = addAttribute(BoostAttribute.class);
    final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

    protected MockBoostTokenFilter(TokenStream input) {
      super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken() == false) {
        return false;
      }
      if (termAtt.length() == 3) {
        boostAtt.setBoost(0.5f);
      }
      return true;
    }
  }

  public void testTokenStreamBoosts() {
    Analyzer msa = new MockSynonymAnalyzer();
    Analyzer a =
        new AnalyzerWrapper(msa.getReuseStrategy()) {
          @Override
          protected Analyzer getWrappedAnalyzer(String fieldName) {
            return msa;
          }

          @Override
          protected TokenStreamComponents wrapComponents(
              String fieldName, TokenStreamComponents components) {
            return new TokenStreamComponents(
                components.getSource(), new MockBoostTokenFilter(components.getTokenStream()));
          }
        };

    SolrQueryBuilder builder = new SolrQueryBuilder(a);
    Query q = builder.createBooleanQuery("field", "hot dogs");
    Query expected =
        new BooleanQuery.Builder()
            .add(
                new BoostQuery(new TermQueryWithOffset(new Term("field", "hot"), 0), 0.5f),
                BooleanClause.Occur.SHOULD)
            .add(
                new SynonymQueryWithOffset(
                    new SynonymQuery.Builder("field")
                        .addTerm(new Term("field", "dogs"))
                        .addTerm(new Term("field", "dog"), 0.5f)
                        .build(),
                    4),
                BooleanClause.Occur.SHOULD)
            .build();

    // TODO
    assertEquals(expected, q);
  }
}
