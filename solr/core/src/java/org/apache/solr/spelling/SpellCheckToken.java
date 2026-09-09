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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.FlagsAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.BytesRef;

/**
 * One term occurrence carried through the spellchecker API, and the key type of {@link
 * SpellingResult}. Unlike the old {@code Token} it replaces, this is a plain, immutable record --
 * not a Lucene {@code AttributeImpl} subclass.
 *
 * <p>A value type is needed here, rather than reading terms off a stream, because {@link
 * SolrSpellChecker#mergeSuggestions} keys suggestions by (text, offset) pairs deserialized from a
 * remote shard's response, where there is no {@link org.apache.lucene.analysis.TokenStream} to read
 * from at all.
 */
public record SpellCheckToken(
    String text,
    int startOffset,
    int endOffset,
    String type,
    int positionIncrement,
    int flags,
    BytesRef payload) {

  public SpellCheckToken(String text, int startOffset, int endOffset) {
    this(text, startOffset, endOffset, "word", 1, 0, null);
  }

  @Override
  public String toString() {
    return text;
  }

  /**
   * Registers the attributes this class needs on {@code stream} once, then lets callers build a
   * {@link SpellCheckToken} for each position the stream reaches, without every call site
   * re-registering the same six attributes.
   */
  public static final class AttributeReader {
    private final CharTermAttribute termAtt;
    private final OffsetAttribute offsetAtt;
    private final TypeAttribute typeAtt;
    private final PositionIncrementAttribute posIncAtt;
    private final FlagsAttribute flagsAtt;
    private final PayloadAttribute payloadAtt;

    public AttributeReader(TokenStream stream) {
      termAtt = stream.addAttribute(CharTermAttribute.class);
      offsetAtt = stream.addAttribute(OffsetAttribute.class);
      typeAtt = stream.addAttribute(TypeAttribute.class);
      posIncAtt = stream.addAttribute(PositionIncrementAttribute.class);
      flagsAtt = stream.addAttribute(FlagsAttribute.class);
      payloadAtt = stream.addAttribute(PayloadAttribute.class);
    }

    /** Builds a {@link SpellCheckToken} from the stream's current position. */
    public SpellCheckToken current() {
      return new SpellCheckToken(
          termAtt.toString(),
          offsetAtt.startOffset(),
          offsetAtt.endOffset(),
          typeAtt.type(),
          posIncAtt.getPositionIncrement(),
          flagsAtt.getFlags(),
          payloadAtt.getPayload());
    }
  }
}
