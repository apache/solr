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
package org.apache.solr.util.hll;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.Hash;

/** Checks precision compatibility before union changes either sketch. */
public class HLLUnionTest extends SolrTestCase {

  public void testUnionRejectsIncompatiblePrecisionWithoutMutation() {
    for (HLLType firstType : HLLType.values()) {
      for (HLLType secondType : HLLType.values()) {
        HLL first = sketch(4, 6, firstType);
        HLL differentCount = sketch(5, 6, secondType);
        assertIncompatibleUnion(first, differentCount);
        assertIncompatibleUnion(differentCount, first);

        HLL differentWidth = sketch(4, 5, secondType);
        assertIncompatibleUnion(first, differentWidth);
        assertIncompatibleUnion(differentWidth, first);
      }
    }
  }

  private static HLL sketch(int log2m, int regwidth, HLLType type) {
    HLL hll = new HLL(log2m, regwidth, -1, true, type);
    if (type != HLLType.EMPTY) {
      hll.addRaw(Hash.fmix64(1));
      assertEquals(type, hll.getType());
    }
    return hll;
  }

  private static void assertIncompatibleUnion(HLL first, HLL second) {
    byte[] firstBefore = first.toBytes();
    byte[] secondBefore = second.toBytes();
    IllegalArgumentException error =
        expectThrows(IllegalArgumentException.class, () -> first.union(second));
    assertTrue(error.getMessage().startsWith("Cannot union HLLs with different precision:"));
    assertArrayEquals(firstBefore, first.toBytes());
    assertArrayEquals(secondBefore, second.toBytes());
  }
}
