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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

public class FixedBitSetIntersectionIteratorTest extends SolrTestCase {

  @Test
  public void testNextDoc() {
    final int iters = atLeast(100);
    for (int i = 0; i < iters; i++) {
      final int length = randomLength();
      final FixedBitSet a = randomBitSet(length);
      final FixedBitSet b = randomBitSet(length);
      final FixedBitSet expected = a.clone();
      expected.and(b);

      final FixedBitSetIntersectionIterator iter = new FixedBitSetIntersectionIterator(a, b);
      assertEquals(-1, iter.docID());
      int expectedDoc = -1;
      while (true) {
        expectedDoc = nextSetBit(expected, expectedDoc + 1);
        assertEquals(expectedDoc, iter.nextDoc());
        assertEquals(expectedDoc, iter.docID());
        if (expectedDoc == NO_MORE_DOCS) {
          break;
        }
      }
    }
  }

  @Test
  public void testAdvance() {
    final int iters = atLeast(100);
    for (int i = 0; i < iters; i++) {
      final int length = randomLength();
      final FixedBitSet a = randomBitSet(length);
      final FixedBitSet b = randomBitSet(length);
      final FixedBitSet expected = a.clone();
      expected.and(b);

      final FixedBitSetIntersectionIterator iter = new FixedBitSetIntersectionIterator(a, b);
      int doc = -1;
      while (doc != NO_MORE_DOCS) {
        final int expectedDoc;
        if (random().nextBoolean()) {
          expectedDoc = nextSetBit(expected, doc + 1);
          doc = iter.nextDoc();
        } else {
          final int target = doc + 1 + random().nextInt(Math.max(1, length / 4 + 2));
          expectedDoc = nextSetBit(expected, target);
          doc = iter.advance(target);
        }
        assertEquals(expectedDoc, doc);
        assertEquals(doc, iter.docID());
      }
    }
  }

  @Test
  public void testDifferentLengths() {
    expectThrows(
        IllegalArgumentException.class,
        () -> new FixedBitSetIntersectionIterator(new FixedBitSet(10), new FixedBitSet(11)));
  }

  /** Includes lengths at and around word boundaries. */
  private static int randomLength() {
    return switch (random().nextInt(4)) {
      case 0 -> random().nextInt(3); // 0, 1, 2
      case 1 -> 64 * (1 + random().nextInt(3)) + random().nextInt(3) - 1; // 63, 64, 65, ...
      default -> random().nextInt(1000);
    };
  }

  private static FixedBitSet randomBitSet(int length) {
    final FixedBitSet bits = new FixedBitSet(length);
    final double density = random().nextDouble();
    for (int i = 0; i < length; i++) {
      if (random().nextDouble() < density) {
        bits.set(i);
      }
    }
    return bits;
  }

  private static int nextSetBit(FixedBitSet bits, int from) {
    return from >= bits.length() ? NO_MORE_DOCS : bits.nextSetBit(from);
  }
}
