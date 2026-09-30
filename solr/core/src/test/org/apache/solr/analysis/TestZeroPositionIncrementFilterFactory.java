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
package org.apache.solr.analysis;

import static org.apache.lucene.tests.analysis.BaseTokenStreamTestCase.assertTokenStreamContents;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.analysis.Token;
import org.apache.solr.SolrTestCase;

public class TestZeroPositionIncrementFilterFactory extends SolrTestCase {

  public void testSubsequentTokensShareFirstPosition() throws Exception {
    ZeroPositionIncrementFilterFactory factory =
        new ZeroPositionIncrementFilterFactory(new HashMap<>());
    TokenStream input = factory.create(whitespaceMockTokenizer("one two three"));
    assertTokenStreamContents(input, new String[] {"one", "two", "three"}, new int[] {1, 0, 0});
  }

  public void testAlreadyOverlappingTokensStayOverlapping() throws Exception {
    Token first = token("Books", 1);
    Token second = token("Books/NonFic", 0);
    Token third = token("Books/NonFic/Law", 0);
    ZeroPositionIncrementFilterFactory factory =
        new ZeroPositionIncrementFilterFactory(new HashMap<>());
    TokenStream input = factory.create(new CannedTokenStream(first, second, third));
    assertTokenStreamContents(
        input, new String[] {"Books", "Books/NonFic", "Books/NonFic/Law"}, new int[] {1, 0, 0});
  }

  public void testResetRestoresFirstTokenIncrement() throws Exception {
    ZeroPositionIncrementFilterFactory factory =
        new ZeroPositionIncrementFilterFactory(new HashMap<>());
    TokenStream input = factory.create(new CannedTokenStream(token("one", 1), token("two", 1)));
    PositionIncrementAttribute pos = input.addAttribute(PositionIncrementAttribute.class);

    input.reset();
    assertTrue(input.incrementToken());
    assertEquals(1, pos.getPositionIncrement());
    assertTrue(input.incrementToken());
    assertEquals(0, pos.getPositionIncrement());
    assertFalse(input.incrementToken());
    input.end();

    input.reset();
    assertTrue(input.incrementToken());
    assertEquals(1, pos.getPositionIncrement());
    assertTrue(input.incrementToken());
    assertEquals(0, pos.getPositionIncrement());
    input.end();
    input.close();
  }

  public void testRejectsUnknownArgs() {
    Map<String, String> args = new HashMap<>();
    args.put("bogus", "true");
    expectThrows(
        IllegalArgumentException.class, () -> new ZeroPositionIncrementFilterFactory(args));
  }

  private static Token token(String text, int posInc) {
    Token token = new Token(text, 0, text.length());
    token.setPositionIncrement(posInc);
    return token;
  }

  private static MockTokenizer whitespaceMockTokenizer(String input) {
    MockTokenizer mockTokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    mockTokenizer.setReader(new StringReader(input));
    return mockTokenizer;
  }
}
