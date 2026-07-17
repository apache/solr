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

package org.apache.solr.update;

import java.util.Comparator;
import java.util.List;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;
import org.junit.Test;

/** Verifies field-based {@code <segmentSort>} ordering and its validation (SOLR-17310). */
public class SegmentSortFieldTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  private IndexSchema schema() {
    return h.getCore().getLatestSchema();
  }

  @Test
  public void testFieldSortBuildsAComparator() {
    // timestamp_i_dvo is a single-valued numeric docValues field.
    Comparator<LeafReader> asc =
        SegmentLeafSorter.forConfig(SegmentSort.parse("timestamp_i_dvo asc"), schema());
    Comparator<LeafReader> desc =
        SegmentLeafSorter.forConfig(SegmentSort.parse("timestamp_i_dvo desc"), schema());
    assertNotNull(asc);
    assertNotNull(desc);
  }

  @Test
  public void testFieldSortRoundTripsSpec() {
    SegmentSort s = SegmentSort.parse("timestamp_i_dvo desc");
    assertEquals(SegmentSort.Kind.FIELD, s.kind());
    assertTrue(s.descending());
    assertEquals("timestamp_i_dvo", s.field());
    assertEquals("timestamp_i_dvo desc", s.toString());
  }

  @Test
  public void testUnknownFieldRejected() {
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () -> SegmentLeafSorter.forConfig(SegmentSort.parse("no_such_field asc"), schema()));
    assertTrue(e.getMessage(), e.getMessage().contains("does not exist"));
  }

  @Test
  public void testNonNumericFieldRejected() {
    // *_s_dvo is a string docValues field (has docValues, but is not numeric).
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () -> SegmentLeafSorter.forConfig(SegmentSort.parse("category_s_dvo asc"), schema()));
    assertTrue(e.getMessage(), e.getMessage().contains("numeric"));
  }

  @Test
  public void testNonDocValuesFieldRejected() {
    // 'name' is indexed/stored text without docValues.
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () -> SegmentLeafSorter.forConfig(SegmentSort.parse("name asc"), schema()));
    assertTrue(e.getMessage(), e.getMessage().contains("docValues"));
  }

  @Test
  public void testMultiValuedFieldRejected() {
    // *_ii_dvo is a multi-valued numeric docValues field.
    IllegalArgumentException e =
        expectThrows(
            IllegalArgumentException.class,
            () -> SegmentLeafSorter.forConfig(SegmentSort.parse("nums_ii_dvo asc"), schema()));
    assertTrue(e.getMessage(), e.getMessage().contains("single-valued"));
  }

  /**
   * A single-valued float field with negative values orders correctly. This is a regression guard:
   * Solr stores single-valued float docValues as raw {@link Float#floatToIntBits} (not sortable
   * bits), so a naive raw-long comparison would order negatives incorrectly.
   */
  @Test
  public void testFloatFieldWithNegativesOrdersByValue() throws Exception {
    // *_f_p is a single-valued pfloat with docValues always enabled.
    // Two all-negative segments chosen so raw floatToIntBits order DIVERGES from true numeric
    // order:
    // floatToIntBits(-8.0) > floatToIntBits(-2.0), so a naive raw-long compare would rank the
    // segment holding -8.0 as "larger" and sort it AFTER the -2.0 segment, which is wrong.
    assertU(adoc("id", "1", "price_f_p", "-2.0"));
    assertU(commit()); // segment A (min = -2.0)
    assertU(adoc("id", "2", "price_f_p", "-8.0"));
    assertU(commit()); // segment B (min = -8.0)

    Comparator<LeafReader> asc =
        SegmentLeafSorter.forConfig(SegmentSort.parse("price_f_p asc"), schema());
    assertNotNull(asc);

    RefCounted<SolrIndexSearcher> ref = h.getCore().getSearcher();
    try {
      List<LeafReaderContext> leaves = ref.get().getIndexReader().leaves();
      assumeTrue("test needs two segments", leaves.size() == 2);
      LeafReader a = leafContaining(leaves, "1"); // -2.0
      LeafReader b = leafContaining(leaves, "2"); // -8.0
      // asc orders by each segment's min: B(-8.0) < A(-2.0), so B must sort before A.
      assertTrue(
          "segment with the smaller (more negative) value must sort first (asc)",
          asc.compare(b, a) < 0);
      assertTrue(asc.compare(a, b) > 0);
    } finally {
      ref.decref();
    }
  }

  private static LeafReader leafContaining(List<LeafReaderContext> leaves, String id)
      throws Exception {
    for (LeafReaderContext ctx : leaves) {
      var terms = ctx.reader().terms("id");
      if (terms != null && terms.iterator().seekExact(new BytesRef(id))) {
        return ctx.reader();
      }
    }
    throw new AssertionError("no leaf contains id=" + id);
  }
}
