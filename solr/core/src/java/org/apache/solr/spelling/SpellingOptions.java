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

import java.util.function.Supplier;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.spell.SuggestMode;
import org.apache.solr.common.params.SolrParams;

/** */
public class SpellingOptions {

  /**
   * Produces a fresh {@link TokenStream} over the terms to spell check. A {@link TokenStream} is
   * single-use (one reset, then incrementToken any number of times, then end, then close); this
   * supplier lets several {@link SolrSpellChecker}s consume the same underlying terms (e.g. via
   * {@link ConjunctionSolrSpellChecker}) by each getting their own instance. Each caller of {@code
   * get()} owns that instance completely, including closing it.
   */
  public Supplier<TokenStream> tokenStreamSupplier;

  /** An optional {@link org.apache.lucene.index.IndexReader} */
  public IndexReader reader;

  /** The number of suggestions to return, if there are any. Defaults to 1. */
  public int count = 1;

  public int alternativeTermCount = 0;

  public SuggestMode suggestMode = SuggestMode.SUGGEST_WHEN_NOT_IN_INDEX;

  /** Provide additional, per implementation, information about the results */
  public boolean extendedResults;

  /**
   * Optionally restrict the results to have a minimum accuracy level. Per Implementation. By
   * default set to Float.MIN_VALUE.
   */
  public float accuracy = Float.MIN_VALUE;

  /** Any other custom params can be passed through. May be null and is null by default. */
  public SolrParams customParams;

  public SpellingOptions() {}

  // A couple of convenience ones
  public SpellingOptions(Supplier<TokenStream> tokenStreamSupplier, int count) {
    this.tokenStreamSupplier = tokenStreamSupplier;
    this.count = count;
  }

  public SpellingOptions(Supplier<TokenStream> tokenStreamSupplier, IndexReader reader) {
    this.tokenStreamSupplier = tokenStreamSupplier;
    this.reader = reader;
  }

  public SpellingOptions(Supplier<TokenStream> tokenStreamSupplier, IndexReader reader, int count) {
    this.tokenStreamSupplier = tokenStreamSupplier;
    this.reader = reader;
    this.count = count;
  }

  public SpellingOptions(
      Supplier<TokenStream> tokenStreamSupplier,
      IndexReader reader,
      int count,
      SuggestMode suggestMode,
      boolean extendedResults,
      float accuracy,
      SolrParams customParams) {
    this.tokenStreamSupplier = tokenStreamSupplier;
    this.reader = reader;
    this.count = count;
    this.suggestMode = suggestMode;
    this.extendedResults = extendedResults;
    this.accuracy = accuracy;
    this.customParams = customParams;
  }

  public SpellingOptions(
      Supplier<TokenStream> tokenStreamSupplier,
      IndexReader reader,
      int count,
      int alternativeTermCount,
      SuggestMode suggestMode,
      boolean extendedResults,
      float accuracy,
      SolrParams customParams) {
    this.tokenStreamSupplier = tokenStreamSupplier;
    this.reader = reader;
    this.count = count;
    this.alternativeTermCount = alternativeTermCount;
    this.suggestMode = suggestMode;
    this.extendedResults = extendedResults;
    this.accuracy = accuracy;
    this.customParams = customParams;
  }
}
