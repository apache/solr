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
package org.apache.solr.search.facet;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.Hash;
import org.apache.solr.util.hll.HLL;
import org.apache.solr.util.hll.HLLType;

public class HLLAggTest extends SolrTestCase {

  public void testDefaultsCountUpTo768ValuesExactly() {
    HLL hll = new HLLAgg.HLLFactory().getHLL();
    addValues(hll, 768);
    assertEquals(HLLType.EXPLICIT, hll.getType());
    assertEquals(768, hll.cardinality());

    addValues(hll, 769);
    assertEquals(HLLType.FULL, hll.getType());
  }

  public void testExpthreshPropertyRaisesTheExactCount() {
    System.setProperty(HLLAgg.HLLFactory.EXPTHRESH_PROPERTY, "15");
    HLL hll = new HLLAgg.HLLFactory().getHLL();
    addValues(hll, 16384);
    assertEquals(HLLType.EXPLICIT, hll.getType());
    assertEquals(16384, hll.cardinality());

    HLL merged = HLL.fromBytes(hll.toBytes());
    merged.union(HLL.fromBytes(hll.toBytes()));
    assertEquals(16384, merged.cardinality());

    addValues(hll, 16385);
    assertEquals(HLLType.FULL, hll.getType());
  }

  public void testLog2mPropertyRaisesTheExactCountAndTheRegisters() {
    System.setProperty(HLLAgg.HLLFactory.LOG2M_PROPERTY, "16");
    HLL hll = new HLLAgg.HLLFactory().getHLL();
    addValues(hll, 6144);
    assertEquals(HLLType.EXPLICIT, hll.getType());
    assertEquals(6144, hll.cardinality());

    addValues(hll, 100_000);
    assertEquals(HLLType.FULL, hll.getType());
    assertEquals(100_000, hll.cardinality(), 100_000 * 0.02);
  }

  /** Adds the hashes of 1 to {@code count}: distinct, since fmix64 is a bijection. */
  private static void addValues(HLL hll, int count) {
    for (long i = 1; i <= count; i++) {
      hll.addRaw(Hash.fmix64(i));
    }
  }
}
