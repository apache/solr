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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

/**
 * Analyzes each parsed query word in turn and concatenates their analysis into a single stream,
 * shifting offsets to the original query string and setting {@link FlagsAttribute} from the
 * query-syntax parse ({@link QueryConverter#REQUIRED_TERM_FLAG} and friends). Each word is only
 * analyzed once this stream reaches it, not up front.
 */
final class QueryWordsTokenStream extends TokenStream {

  /** One query word as parsed from query syntax, prior to analysis. */
  record ParsedWord(String text, int startIndex, int flags) {}

  private final List<ParsedWord> words;
  private final Analyzer analyzer;
  private int nextWordIndex;
  private int currentWordIndex;
  private TokenStream current;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
  private final PositionIncrementAttribute posIncAtt =
      addAttribute(PositionIncrementAttribute.class);
  private final PayloadAttribute payloadAtt = addAttribute(PayloadAttribute.class);
  private final FlagsAttribute flagsAtt = addAttribute(FlagsAttribute.class);

  QueryWordsTokenStream(List<ParsedWord> words, Analyzer analyzer) {
    this.words = words;
    this.analyzer = analyzer;
  }

  @Override
  public boolean incrementToken() throws IOException {
    while (true) {
      if (current != null && current.incrementToken()) {
        ParsedWord word = words.get(currentWordIndex);
        clearAttributes();
        termAtt.append(current.addAttribute(CharTermAttribute.class));
        OffsetAttribute innerOffset = current.addAttribute(OffsetAttribute.class);
        offsetAtt.setOffset(
            word.startIndex() + innerOffset.startOffset(),
            word.startIndex() + innerOffset.endOffset());
        typeAtt.setType(current.addAttribute(TypeAttribute.class).type());
        posIncAtt.setPositionIncrement(
            current.addAttribute(PositionIncrementAttribute.class).getPositionIncrement());
        payloadAtt.setPayload(current.addAttribute(PayloadAttribute.class).getPayload());
        flagsAtt.setFlags(word.flags());
        return true;
      }
      if (current != null) {
        current.end();
        current.close();
        current = null;
      }
      if (nextWordIndex >= words.size()) {
        return false;
      }
      currentWordIndex = nextWordIndex;
      current = analyzer.tokenStream("", words.get(nextWordIndex).text());
      nextWordIndex++;
      current.reset();
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    nextWordIndex = 0;
    currentWordIndex = 0;
    if (current != null) {
      current.close();
      current = null;
    }
  }

  @Override
  public void end() throws IOException {
    super.end();
    if (current != null) {
      current.end();
    }
  }

  @Override
  public void close() throws IOException {
    if (current != null) {
      current.close();
      current = null;
    }
    super.close();
  }
}
