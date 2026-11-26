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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.FixedBitSet;

/**
 * Iterates the intersection of two {@link FixedBitSet}s of the same length, word by word, without
 * allocating. Unlike a conjunction that iterates one set and checks the other per doc, the cost is
 * proportional to the number of words plus the size of the intersection.
 *
 * <p>The bit sets must not be modified during iteration.
 *
 * @lucene.internal
 */
public final class FixedBitSetIntersectionIterator extends DocIdSetIterator {

  private final long[] aBits;
  private final long[] bBits;
  private final int numWords;
  private final int length;
  private final long cost;

  private int doc = -1;
  private int wordIndex = -1;
  private long word; // the remaining intersected bits of the current word, after doc

  public FixedBitSetIntersectionIterator(FixedBitSet a, FixedBitSet b) {
    if (a.length() != b.length()) {
      throw new IllegalArgumentException("Lengths differ: " + a.length() + " and " + b.length());
    }
    this.aBits = a.getBits();
    this.bBits = b.getBits();
    this.length = a.length();
    this.numWords = FixedBitSet.bits2words(length);
    this.cost = Math.min(a.approximateCardinality(), b.approximateCardinality());
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public int nextDoc() {
    while (word == 0) {
      if (++wordIndex >= numWords) {
        return doc = NO_MORE_DOCS;
      }
      word = aBits[wordIndex] & bBits[wordIndex];
    }
    final int bit = Long.numberOfTrailingZeros(word);
    word &= word - 1; // clear the lowest set bit
    return doc = (wordIndex << 6) + bit;
  }

  @Override
  public int advance(int target) {
    if (target >= length) {
      wordIndex = numWords;
      word = 0;
      return doc = NO_MORE_DOCS;
    }
    wordIndex = target >> 6;
    // the shift is mod 64, thus this clears the bits below target in this word
    word = aBits[wordIndex] & bBits[wordIndex] & (-1L << target);
    return nextDoc();
  }

  @Override
  public long cost() {
    return cost;
  }
}
