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

import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

public class SpellCheckCollatorCollationOnlyTest extends SolrTestCaseJ4 {

  // Tests SOLR-13360 using some manual correction scenario based on various observations recorded
  // in the ticket.
  //
  // Notes
  // - tokens with (tok.getPositionIncrement() == 0) will be filtered out.
  // - replacing longer tokens with shorter ones can make the offset go negative, which seems ok as
  // long as the startOffset is always in (strict) increasing order

  @Test
  public void testCollationGeneration1() {
    // sample from synonyms.txt:
    // # Synonyms used in semantic expansion
    // tabby => tabby, cat, feline, animal
    // persian => persian, cat, feline, animal

    String origQuery = "cats persians tabby";
    List<SpellCheckCorrection> corrections =
        List.of(
            // ref 1st token
            correction("cats", 0, 4, 1, "cat"), // original token. offset goes to -1

            // ref 2nd token
            correction("persian", 5, 13, 1, "persian"), // original token. offset goes to -2

            // ref 3rd token
            correction("tabbi", 14, 19, 1, "tabbi"), // original token
            correction("cat", 14, 19, 0, "cat"), // synonym
            correction("felin", 14, 19, 0, "felin"), // synonym
            correction("anim", 14, 19, 0, "anim") // synonym
            );

    String collation = SpellCheckCollator.getCollation(origQuery, corrections);
    String expected = "cat persian tabbi";
    assertEquals("Incorrect collation: " + collation, expected, collation);
  }

  @Test
  public void testCollationGeneration1Shuffle() {
    // same as testCollationGeneration1 but I am manually shuffling the tokens

    String origQuery = "cats persians tabby";
    List<SpellCheckCorrection> corrections =
        List.of(
            // ref 2nd token
            correction("persian", 5, 13, 1, "persian"), // original token. offset goes to -2

            // ref 1st token
            correction("cats", 0, 4, 1, "cat"), // original token. offset goes to -1

            // ref 3rd token
            correction("tabbi", 14, 19, 1, "tabbi"), // original token
            correction("cat", 14, 19, 0, "cat"), // synonym
            correction("felin", 14, 19, 0, "felin"), // synonym
            correction("anim", 14, 19, 0, "anim") // synonym
            );

    String collation = SpellCheckCollator.getCollation(origQuery, corrections);
    String expected = "cat persian tabbi";
    assertEquals("Incorrect collation: " + collation, expected, collation);
  }

  @Test
  public void testCollationGeneration1Repeat() {
    // same as testCollationGeneration1 but I am manually repeating one of the tokens

    String origQuery = "cats persians tabby";
    List<SpellCheckCorrection> corrections =
        List.of(
            // ref 1st token
            correction("cats", 0, 4, 1, "cat"), // original token. offset goes to -1

            // ref 1st token - duplicated
            correction("cats", 0, 4, 1, "cat"), // original token. offset goes to -1

            // ref 2nd token
            correction("persian", 5, 13, 1, "persian"), // original token. offset goes to -2

            // ref 3rd token
            correction("tabbi", 14, 19, 1, "tabbi"), // original token
            correction("cat", 14, 19, 0, "cat"), // synonym
            correction("felin", 14, 19, 0, "felin"), // synonym
            correction("anim", 14, 19, 0, "anim") // synonym
            );

    String collation = SpellCheckCollator.getCollation(origQuery, corrections);
    String expected = "cat persian tabbi";
    assertEquals("Incorrect collation: " + collation, expected, collation);
  }

  @Test
  public void testCollationGeneration2() {
    // sample from synonyms.txt:
    // panthera pardus, leopard|0.6
    //
    // Note. depending on the field type, this can end up as the list of tokens: [leopard, 0, 6,
    // panthera, pardu, cats]

    String origQuery = "panthera pardus cats";

    List<SpellCheckCorrection> corrections =
        List.of(
            correction("leopard", 0, 15, 1, "leopard"),
            correction("0", 0, 15, 1, "0"),
            correction("6", 0, 15, 1, "6"),
            correction("panthera", 0, 8, 0, "panthera"),
            correction("pardu", 9, 15, 1, "pardu"),
            correction("cats", 16, 20, 1, "cat"));

    String collation = SpellCheckCollator.getCollation(origQuery, corrections);
    String expected = "leopard cat";
    assertEquals("Incorrect collation: " + collation, expected, collation);
  }

  private static SpellCheckCorrection correction(
      String text, int start, int end, int positionIncrement, String correction) {
    SpellCheckCorrection spellCheckCorrection = new SpellCheckCorrection();
    spellCheckCorrection.setOriginal(token(text, start, end, positionIncrement));
    spellCheckCorrection.setCorrection(correction);
    spellCheckCorrection.setNumberOfOccurences(1);
    return spellCheckCorrection;
  }

  private static Token token(String text, int start, int end, int positionIncrement) {
    Token token = new Token(text, start, end);
    token.setPositionIncrement(positionIncrement);
    return token;
  }
}
