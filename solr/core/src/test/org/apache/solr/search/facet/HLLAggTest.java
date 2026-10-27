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
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Hash;
import org.apache.solr.common.util.SimpleOrderedMap;
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

  public void testMergeRejectsLargerLog2m() {
    assertDifferentLog2mRejected(13, 16);
  }

  public void testMergeRejectsSmallerLog2m() {
    assertDifferentLog2mRejected(16, 13);
  }

  public void testMergeRejectsDifferentRegisterWidths() {
    HLL narrower = new HLL(13, 5, -1, false, HLLType.EMPTY);
    HLL wider = new HLL(13, 6, -1, false, HLLType.EMPTY);
    addValues(narrower, 10_000);
    addValues(wider, 10_000);
    assertEquals(HLLType.FULL, narrower.getType());
    assertEquals(HLLType.FULL, wider.getType());

    assertIncompatiblePrecision(narrower, wider, "log2m=13, regwidth=5", "log2m=13, regwidth=6");
    assertIncompatiblePrecision(wider, narrower, "log2m=13, regwidth=6", "log2m=13, regwidth=5");
  }

  public void testMergeRejectsDifferentLog2mBeforePromotion() {
    HLL smaller = new HLL(13, 6, -1, false, HLLType.EMPTY);
    HLL larger = new HLL(16, 6, -1, false, HLLType.EMPTY);
    addValues(smaller, 100);
    addValues(larger, 100);
    assertEquals(HLLType.EXPLICIT, smaller.getType());
    assertEquals(HLLType.EXPLICIT, larger.getType());

    assertIncompatiblePrecision(smaller, larger, "log2m=13, regwidth=6", "log2m=16, regwidth=6");
    assertIncompatiblePrecision(larger, smaller, "log2m=16, regwidth=6", "log2m=13, regwidth=6");
  }

  public void testMergeAllowsDifferentExplicitThresholds() {
    HLL first = new HLL(13, 6, 15, false, HLLType.EMPTY);
    HLL second = new HLL(13, 6, 18, false, HLLType.EMPTY);
    addValues(first, 1000);
    addValues(second, 501, 1500);
    assertEquals(HLLType.EXPLICIT, first.getType());
    assertEquals(HLLType.EXPLICIT, second.getType());

    assertMergedCardinality(first, second, 1500);
    assertMergedCardinality(second, first, 1500);
  }

  public void testMergeAllowsDifferentRepresentations() {
    HLL full = new HLL(13, 6, -1, false, HLLType.EMPTY);
    HLL explicit = new HLL(13, 6, 18, false, HLLType.EMPTY);
    addValues(full, 10_000);
    addValues(explicit, 5001, 15_000);
    assertEquals(HLLType.FULL, full.getType());
    assertEquals(HLLType.EXPLICIT, explicit.getType());
    HLL expected = new HLL(13, 6, 0, false, HLLType.EMPTY);
    addValues(expected, 15_000);

    assertMergedCardinality(full, explicit, expected.cardinality());
    assertMergedCardinality(explicit, full, expected.cardinality());
  }

  public void testMergeIgnoresShardsWithoutValues() {
    System.setProperty(HLLAgg.HLLFactory.LOG2M_PROPERTY, "16");
    FacetMerger merger = new HLLAgg("unused").createFacetMerger(null);
    FacetMerger.Context context = new FacetMerger.Context(3);
    merger.merge(HLLAgg.NO_VALUES, context);
    HLL shard = new HLL(13, 6, -1, false, HLLType.EMPTY);
    addValues(shard, 100);
    merger.merge(shardResponse(shard), context);
    merger.merge(HLLAgg.NO_VALUES, context);
    assertEquals(100L, merger.getMergedResult());
  }

  private static void assertDifferentLog2mRejected(int firstLog2m, int secondLog2m) {
    HLL first = new HLL(firstLog2m, 6, -1, false, HLLType.EMPTY);
    HLL second = new HLL(secondLog2m, 6, -1, false, HLLType.EMPTY);
    addValues(first, 10_000);
    addValues(second, 10_000);
    assertEquals(HLLType.FULL, first.getType());
    assertEquals(HLLType.FULL, second.getType());
    assertIncompatiblePrecision(
        first,
        second,
        "log2m=" + firstLog2m + ", regwidth=6",
        "log2m=" + secondLog2m + ", regwidth=6");
  }

  private static void assertIncompatiblePrecision(
      HLL first, HLL second, String firstPrecision, String secondPrecision) {
    FacetMerger merger = new HLLAgg("unused").createFacetMerger(null);
    FacetMerger.Context context = new FacetMerger.Context(2);
    merger.merge(shardResponse(first), context);
    SolrException error =
        expectThrows(SolrException.class, () -> merger.merge(shardResponse(second), context));
    assertEquals(500, error.code());
    assertTrue(error.getMessage().contains(HLLAgg.HLLFactory.LOG2M_PROPERTY));
    assertTrue(error.getCause() instanceof IllegalArgumentException);
    assertEquals(
        "Cannot union HLLs with different precision: "
            + firstPrecision
            + " versus "
            + secondPrecision,
        error.getCause().getMessage());
    assertEquals(first.cardinality(), merger.getMergedResult());
  }

  private static void assertMergedCardinality(HLL first, HLL second, long expected) {
    FacetMerger merger = new HLLAgg("unused").createFacetMerger(null);
    FacetMerger.Context context = new FacetMerger.Context(2);
    merger.merge(shardResponse(first), context);
    merger.merge(shardResponse(second), context);
    assertEquals(expected, merger.getMergedResult());
  }

  private static SimpleOrderedMap<Object> shardResponse(HLL hll) {
    SimpleOrderedMap<Object> response = new SimpleOrderedMap<>();
    response.add("hll", hll.toBytes());
    return response;
  }

  /** Adds the hashes of 1 to {@code count}: distinct, since fmix64 is a bijection. */
  private static void addValues(HLL hll, int count) {
    addValues(hll, 1, count);
  }

  private static void addValues(HLL hll, int start, int end) {
    for (long i = start; i <= end; i++) {
      hll.addRaw(Hash.fmix64(i));
    }
  }
}
