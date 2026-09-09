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
package org.apache.solr.spelling;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

/**
 * Test for SpellingQueryConverter
 *
 * @since solr 1.3
 */
public class SpellingQueryConverterTest extends SolrTestCase {

  @Test
  public void test() throws IOException {
    SpellingQueryConverter converter = new SpellingQueryConverter();
    converter.init(new NamedList<>());
    converter.setAnalyzer(new WhitespaceAnalyzer());
    List<SpellCheckToken> tokens = SpellCheckToken.drain(converter.convert("field:foo"));
    assertNotNull("tokens is null and it shouldn't be", tokens);
    assertEquals("tokens Size: " + tokens.size() + " is not: " + 1, 1, tokens.size());
  }

  @Test
  public void testNumeric() throws IOException {
    SpellingQueryConverter converter = new SpellingQueryConverter();
    converter.init(new NamedList<>());
    converter.setAnalyzer(new WhitespaceAnalyzer());
    String[] queries = {
      "12345",
      "foo:12345",
      "12345 67890",
      "foo:(12345 67890)",
      "foo:(life 67890)",
      "12345 life",
      "+12345 +life",
      "-12345 life"
    };
    int[] tokensToExpect = {1, 1, 2, 2, 2, 2, 2, 2};
    for (int i = 0; i < queries.length; i++) {
      List<SpellCheckToken> tokens = SpellCheckToken.drain(converter.convert(queries[i]));
      assertEquals(
          "tokens Size: " + tokens.size() + " is not: " + tokensToExpect[i],
          tokens.size(),
          tokensToExpect[i]);
    }
  }

  @Test
  public void testSpecialChars() throws IOException {
    SpellingQueryConverter converter = new SpellingQueryConverter();
    converter.init(new NamedList<>());
    converter.setAnalyzer(new WhitespaceAnalyzer());
    String original = "field_with_underscore:value_with_underscore";
    List<SpellCheckToken> tokens = SpellCheckToken.drain(converter.convert(original));
    assertNotNull("tokens is null and it shouldn't be", tokens);
    assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());
    assertTrue("Token offsets do not match", isOffsetCorrect(original, tokens));

    original = "field_with_digits123:value_with_digits123";
    tokens = SpellCheckToken.drain(converter.convert(original));
    assertNotNull("tokens is null and it shouldn't be", tokens);
    assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());
    assertTrue("Token offsets do not match", isOffsetCorrect(original, tokens));

    original = "field-with-hyphens:value-with-hyphens";
    tokens = SpellCheckToken.drain(converter.convert(original));
    assertNotNull("tokens is null and it shouldn't be", tokens);
    assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());
    assertTrue("Token offsets do not match", isOffsetCorrect(original, tokens));

    // mix 'em up and add some to the value
    //    original = "field_with-123s:value_,.|with-hyphens";
    //    tokens = converter.convert(original);
    //    assertTrue("tokens is null, and it shouldn't be", tokens != null);
    //    assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());
    //    assertTrue("Token offsets do not match", isOffsetCorrect(original, tokens));

    original = "foo:bar^5.0";
    tokens = SpellCheckToken.drain(converter.convert(original));
    assertNotNull("tokens is null and it shouldn't be", tokens);
    assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());
    assertTrue("Token offsets do not match", isOffsetCorrect(original, tokens));

    String firstKeyword = "value1";
    String secondKeyword = "value2";
    original = "field-with-parenthesis:(" + firstKeyword + " " + secondKeyword + ")";
    tokens = SpellCheckToken.drain(converter.convert(original));
    assertNotNull("tokens is null and it shouldn't be", tokens);
    assertEquals("tokens Size: " + tokens.size() + " is not 2", 2, tokens.size());
    assertTrue("Token offsets do not match", isOffsetCorrect(original, tokens));
    assertEquals("first Token is not " + firstKeyword, tokens.get(0).toString(), firstKeyword);
    assertEquals("second Token is not " + secondKeyword, tokens.get(1).toString(), secondKeyword);
  }

  private boolean isOffsetCorrect(String s, List<SpellCheckToken> tokens) {
    for (SpellCheckToken token : tokens) {
      int start = token.startOffset();
      int end = token.endOffset();
      if (!s.substring(start, end).equals(token.toString())) return false;
    }
    return true;
  }

  @Test
  public void testUnicode() throws IOException {
    SpellingQueryConverter converter = new SpellingQueryConverter();
    converter.init(new NamedList<>());
    converter.setAnalyzer(new WhitespaceAnalyzer());

    // chinese text value
    List<SpellCheckToken> tokens =
        SpellCheckToken.drain(converter.convert("text_field:我购买了道具和服装。"));
    assertNotNull("tokens is null and it shouldn't be", tokens);
    assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());

    tokens = SpellCheckToken.drain(converter.convert("text_购field:我购买了道具和服装。"));
    assertNotNull("tokens is null and it shouldn't be", tokens);
    assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());

    tokens = SpellCheckToken.drain(converter.convert("text_field:我购xyz买了道具和服装。"));
    assertNotNull("tokens is null and it shouldn't be", tokens);
    assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());
  }

  @Test
  public void testMultipleClauses() throws IOException {
    SpellingQueryConverter converter = new SpellingQueryConverter();
    converter.init(new NamedList<>());
    converter.setAnalyzer(new WhitespaceAnalyzer());

    // two field:value pairs should give two tokens
    List<SpellCheckToken> tokens =
        SpellCheckToken.drain(converter.convert("买text_field:我购买了道具和服装。 field2:bar"));
    assertNotNull("tokens is null and it shouldn't be", tokens);
    assertEquals("tokens Size: " + tokens.size() + " is not 2", 2, tokens.size());

    // a field:value pair and a search term should give two tokens
    tokens = SpellCheckToken.drain(converter.convert("text_field:我购买了道具和服装。 bar"));
    assertNotNull("tokens is null and it shouldn't be", tokens);
    assertEquals("tokens Size: " + tokens.size() + " is not 2", 2, tokens.size());
  }

  @Test
  public void testRequiredOrProhibitedFlags() throws IOException {
    SpellingQueryConverter converter = new SpellingQueryConverter();
    converter.init(new NamedList<>());
    converter.setAnalyzer(new WhitespaceAnalyzer());

    {
      List<SpellCheckToken> tokens = SpellCheckToken.drain(converter.convert("aaa bbb ccc"));
      assertTrue("Should have 3 tokens", tokens != null && tokens.size() == 3);
      assertTrue(
          "token 1 should be optional",
          !hasRequiredFlag(tokens.get(0)) && !hasProhibitedFlag(tokens.get(0)));
      assertTrue(
          "token 2 should be optional",
          !hasRequiredFlag(tokens.get(1)) && !hasProhibitedFlag(tokens.get(1)));
      assertTrue(
          "token 3 should be optional",
          !hasRequiredFlag(tokens.get(2)) && !hasProhibitedFlag(tokens.get(2)));
    }
    {
      List<SpellCheckToken> tokens = SpellCheckToken.drain(converter.convert("+aaa bbb -ccc"));
      assertTrue("Should have 3 tokens", tokens != null && tokens.size() == 3);
      assertTrue(
          "token 1 should be required",
          hasRequiredFlag(tokens.get(0)) && !hasProhibitedFlag(tokens.get(0)));
      assertTrue(
          "token 2 should be optional",
          !hasRequiredFlag(tokens.get(1)) && !hasProhibitedFlag(tokens.get(1)));
      assertTrue(
          "token 3 should be prohibited",
          !hasRequiredFlag(tokens.get(2)) && hasProhibitedFlag(tokens.get(2)));
    }
    {
      List<SpellCheckToken> tokens = SpellCheckToken.drain(converter.convert("aaa AND bbb ccc"));
      assertTrue("Should have 3 tokens", tokens != null && tokens.size() == 3);
      assertTrue(
          "token 1 doesn't precede n.b.o.",
          !hasNBOFlag(tokens.get(0)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue(
          "token 2 doesn't precede n.b.o.",
          !hasNBOFlag(tokens.get(1)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue(
          "token 3 doesn't precede n.b.o.",
          !hasNBOFlag(tokens.get(2)) && hasInBooleanFlag(tokens.get(0)));
    }
    {
      List<SpellCheckToken> tokens = SpellCheckToken.drain(converter.convert("aaa OR bbb OR ccc"));
      assertTrue("Should have 3 tokens", tokens != null && tokens.size() == 3);
      assertTrue(
          "token 1 doesn't precede n.b.o.",
          !hasNBOFlag(tokens.get(0)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue(
          "token 2 doesn't precede n.b.o.",
          !hasNBOFlag(tokens.get(1)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue(
          "token 3 doesn't precede n.b.o.",
          !hasNBOFlag(tokens.get(2)) && hasInBooleanFlag(tokens.get(0)));
    }
    {
      List<SpellCheckToken> tokens =
          SpellCheckToken.drain(converter.convert("aaa AND bbb NOT ccc"));
      assertTrue("Should have 3 tokens", tokens != null && tokens.size() == 3);
      assertTrue(
          "token 1 doesn't precede n.b.o.",
          !hasNBOFlag(tokens.get(0)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue(
          "token 2 precedes n.b.o.", hasNBOFlag(tokens.get(1)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue(
          "token 3 doesn't precede n.b.o.",
          !hasNBOFlag(tokens.get(2)) && hasInBooleanFlag(tokens.get(0)));
    }
    {
      List<SpellCheckToken> tokens =
          SpellCheckToken.drain(converter.convert("aaa NOT bbb AND ccc"));
      assertTrue("Should have 3 tokens", tokens != null && tokens.size() == 3);
      assertTrue(
          "token 1 precedes n.b.o.", hasNBOFlag(tokens.get(0)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue(
          "token 2 precedes n.b.o.", hasNBOFlag(tokens.get(1)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue(
          "token 3 doesn't precedes n.b.o.",
          !hasNBOFlag(tokens.get(2)) && hasInBooleanFlag(tokens.get(0)));
    }
    {
      List<SpellCheckToken> tokens =
          SpellCheckToken.drain(converter.convert("aaa AND NOT bbb AND ccc"));
      assertTrue("Should have 3 tokens", tokens != null && tokens.size() == 3);
      assertTrue(
          "token 1 precedes n.b.o.", hasNBOFlag(tokens.get(0)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue(
          "token 2 precedes n.b.o.", hasNBOFlag(tokens.get(1)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue(
          "token 3 doesn't precedes n.b.o.",
          !hasNBOFlag(tokens.get(2)) && hasInBooleanFlag(tokens.get(0)));
    }
  }

  /**
   * A query word whose analysis throws is skipped, and the query's other words are still converted.
   */
  @Test
  public void testWordFailingAnalysisIsSkipped() throws IOException {
    SpellingQueryConverter converter = new SpellingQueryConverter();
    converter.init(new NamedList<>());
    converter.setAnalyzer(
        new Analyzer() {
          @Override
          protected TokenStreamComponents createComponents(String fieldName) {
            Tokenizer source = new WhitespaceTokenizer();
            return new TokenStreamComponents(source, new FailOnTermFilter(source, "bbb"));
          }
        });

    List<SpellCheckToken> tokens = SpellCheckToken.drain(converter.convert("aaa bbb ccc"));

    assertEquals(List.of("aaa", "ccc"), tokens.stream().map(SpellCheckToken::text).toList());
  }

  /** Fails on one term, the way a filter reading an external dictionary would on a bad read. */
  private static class FailOnTermFilter extends TokenFilter {
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final String failOn;

    FailOnTermFilter(TokenStream input, String failOn) {
      super(input);
      this.failOn = failOn;
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (!input.incrementToken()) {
        return false;
      }
      if (failOn.contentEquals(termAtt)) {
        throw new IOException("analysis of '" + failOn + "' failed");
      }
      return true;
    }
  }

  private boolean hasRequiredFlag(SpellCheckToken t) {
    return (t.flags() & QueryConverter.REQUIRED_TERM_FLAG) == QueryConverter.REQUIRED_TERM_FLAG;
  }

  private boolean hasProhibitedFlag(SpellCheckToken t) {
    return (t.flags() & QueryConverter.PROHIBITED_TERM_FLAG) == QueryConverter.PROHIBITED_TERM_FLAG;
  }

  private boolean hasNBOFlag(SpellCheckToken t) {
    return (t.flags() & QueryConverter.TERM_PRECEDES_NEW_BOOLEAN_OPERATOR_FLAG)
        == QueryConverter.TERM_PRECEDES_NEW_BOOLEAN_OPERATOR_FLAG;
  }

  private boolean hasInBooleanFlag(SpellCheckToken t) {
    return (t.flags() & QueryConverter.TERM_IN_BOOLEAN_QUERY_FLAG)
        == QueryConverter.TERM_IN_BOOLEAN_QUERY_FLAG;
  }
}
