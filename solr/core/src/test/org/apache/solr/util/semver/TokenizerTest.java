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

// Forked and adapted from https://github.com/vdurmont/semver4j - MIT license
// Copyright (c) 2015-present Vincent DURMONT vdurmont@gmail.com

package org.apache.solr.util.semver;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class TokenizerTest {
  @Test public void tokenize_NPM_tilde() {
    String requirement = "~ 1.2.7";
    List<Tokenizer.Token> tokens = Tokenizer.tokenize(requirement, Semver.SemverType.NPM);
    assertEquals(2, tokens.size());

    assertEquals(Tokenizer.TokenType.TILDE, tokens.get(0).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(1).type);
    assertEquals("1.2.7", tokens.get(1).value);
  }

  @Test public void tokenize_NPM_caret() {
    String requirement = "^ 1.2.7   ";
    List<Tokenizer.Token> tokens = Tokenizer.tokenize(requirement, Semver.SemverType.NPM);
    assertEquals(2, tokens.size());

    assertEquals(Tokenizer.TokenType.CARET, tokens.get(0).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(1).type);
    assertEquals("1.2.7", tokens.get(1).value);
  }

  @Test public void tokenize_NPM_lte() {
    String requirement = "<=1.2.7";
    List<Tokenizer.Token> tokens = Tokenizer.tokenize(requirement, Semver.SemverType.NPM);
    assertEquals(2, tokens.size());

    assertEquals(Tokenizer.TokenType.LTE, tokens.get(0).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(1).type);
    assertEquals("1.2.7", tokens.get(1).value);
  }

  @Test public void tokenize_NPM_lt() {
    String requirement = "<1.2.7";
    List<Tokenizer.Token> tokens = Tokenizer.tokenize(requirement, Semver.SemverType.NPM);
    assertEquals(2, tokens.size());

    assertEquals(Tokenizer.TokenType.LT, tokens.get(0).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(1).type);
    assertEquals("1.2.7", tokens.get(1).value);
  }

  @Test public void tokenize_NPM_gte() {
    String requirement = ">=1.2.7";
    List<Tokenizer.Token> tokens = Tokenizer.tokenize(requirement, Semver.SemverType.NPM);
    assertEquals(2, tokens.size());

    assertEquals(Tokenizer.TokenType.GTE, tokens.get(0).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(1).type);
    assertEquals("1.2.7", tokens.get(1).value);
  }

  @Test public void tokenize_NPM_gt() {
    String requirement = ">1.2.7";
    List<Tokenizer.Token> tokens = Tokenizer.tokenize(requirement, Semver.SemverType.NPM);
    assertEquals(2, tokens.size());

    assertEquals(Tokenizer.TokenType.GT, tokens.get(0).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(1).type);
    assertEquals("1.2.7", tokens.get(1).value);
  }

  @Test public void tokenize_NPM_eq() {
    String requirement = "=1.2.7";
    List<Tokenizer.Token> tokens = Tokenizer.tokenize(requirement, Semver.SemverType.NPM);
    assertEquals(2, tokens.size());

    assertEquals(Tokenizer.TokenType.EQ, tokens.get(0).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(1).type);
    assertEquals("1.2.7", tokens.get(1).value);
  }

  @Test public void tokenize_NPM_gte_major() {
    String requirement = ">=1";
    List<Tokenizer.Token> tokens = Tokenizer.tokenize(requirement, Semver.SemverType.NPM);
    assertEquals(2, tokens.size());

    assertEquals(Tokenizer.TokenType.GTE, tokens.get(0).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(1).type);
    assertEquals("1", tokens.get(1).value);
  }

  @Test public void tokenize_NPM_suffix() {
    String requirement = "1.2.7-rc.1";
    List<Tokenizer.Token> tokens = Tokenizer.tokenize(requirement, Semver.SemverType.NPM);
    assertEquals(3, tokens.size());

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(0).type);
    assertEquals("1.2.7", tokens.get(0).value);

    // @TODO: Differentiate between hyphen for range vs. suffix
    assertEquals(Tokenizer.TokenType.HYPHEN, tokens.get(1).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(2).type);
    assertEquals("rc.1", tokens.get(2).value);
  }

  @Test public void tokenize_NPM_or_suffix() {
    String requirement = "1.2.7-rc.1 || 1.2.7-rc.2";
    List<Tokenizer.Token> tokens = Tokenizer.tokenize(requirement, Semver.SemverType.NPM);
    assertEquals(7, tokens.size());

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(0).type);
    assertEquals("1.2.7", tokens.get(0).value);

    assertEquals(Tokenizer.TokenType.HYPHEN, tokens.get(1).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(2).type);
    assertEquals("rc.1", tokens.get(2).value);

    assertEquals(Tokenizer.TokenType.OR, tokens.get(3).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(4).type);
    assertEquals("1.2.7", tokens.get(4).value);

    assertEquals(Tokenizer.TokenType.HYPHEN, tokens.get(5).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(6).type);
    assertEquals("rc.2", tokens.get(6).value);
  }

  @Test public void tokenize_NPM_or_hyphen() {
    String requirement = "1.2.7 || 1.2.9 - 2.0.0";
    List<Tokenizer.Token> tokens = Tokenizer.tokenize(requirement, Semver.SemverType.NPM);
    assertEquals(5, tokens.size());

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(0).type);
    assertEquals("1.2.7", tokens.get(0).value);

    assertEquals(Tokenizer.TokenType.OR, tokens.get(1).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(2).type);
    assertEquals("1.2.9", tokens.get(2).value);

    assertEquals(Tokenizer.TokenType.HYPHEN, tokens.get(3).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(4).type);
    assertEquals("2.0.0", tokens.get(4).value);
  }

  @Test public void tokenize_NPM_or_lte_parenthesis() {
    String requirement = "1.2.7 || (<=1.2.9 || 2.0.0)";
    List<Tokenizer.Token> tokens = Tokenizer.tokenize(requirement, Semver.SemverType.NPM);
    assertEquals(8, tokens.size());

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(0).type);
    assertEquals("1.2.7", tokens.get(0).value);

    assertEquals(Tokenizer.TokenType.OR, tokens.get(1).type);

    assertEquals(Tokenizer.TokenType.OPENING, tokens.get(2).type);

    assertEquals(Tokenizer.TokenType.LTE, tokens.get(3).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(4).type);
    assertEquals("1.2.9", tokens.get(4).value);

    assertEquals(Tokenizer.TokenType.OR, tokens.get(5).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(6).type);
    assertEquals("2.0.0", tokens.get(6).value);

    assertEquals(Tokenizer.TokenType.CLOSING, tokens.get(7).type);
  }

  @Test public void tokenize_NPM_or_and() {
    String requirement = ">1.2.1 <1.2.8 || >2.0.0 <3.0.0";
    List<Tokenizer.Token> tokens = Tokenizer.tokenize(requirement, Semver.SemverType.NPM);
    assertEquals(11, tokens.size());

    assertEquals(Tokenizer.TokenType.GT, tokens.get(0).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(1).type);
    assertEquals("1.2.1", tokens.get(1).value);

    assertEquals(Tokenizer.TokenType.AND, tokens.get(2).type);

    assertEquals(Tokenizer.TokenType.LT, tokens.get(3).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(4).type);
    assertEquals("1.2.8", tokens.get(4).value);

    assertEquals(Tokenizer.TokenType.OR, tokens.get(5).type);

    assertEquals(Tokenizer.TokenType.GT, tokens.get(6).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(7).type);
    assertEquals("2.0.0", tokens.get(7).value);

    assertEquals(Tokenizer.TokenType.AND, tokens.get(8).type);

    assertEquals(Tokenizer.TokenType.LT, tokens.get(9).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(10).type);
    assertEquals("3.0.0", tokens.get(10).value);
  }

  @Test public void tokenize_Cocoapods_tilde() {
    String requirement = "~> 1.2.7";
    List<Tokenizer.Token> tokens = Tokenizer.tokenize(requirement, Semver.SemverType.COCOAPODS);
    assertEquals(2, tokens.size());

    assertEquals(Tokenizer.TokenType.TILDE, tokens.get(0).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(1).type);
    assertEquals("1.2.7", tokens.get(1).value);
  }

  @Test public void tokenize_Cocoapods_lte() {
    String requirement = "<=1.2.7";
    List<Tokenizer.Token> tokens = Tokenizer.tokenize(requirement, Semver.SemverType.COCOAPODS);
    assertEquals(2, tokens.size());

    assertEquals(Tokenizer.TokenType.LTE, tokens.get(0).type);

    assertEquals(Tokenizer.TokenType.VERSION, tokens.get(1).type);
    assertEquals("1.2.7", tokens.get(1).value);
  }
}
