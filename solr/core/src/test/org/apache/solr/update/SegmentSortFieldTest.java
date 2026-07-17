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
import org.apache.lucene.index.LeafReader;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.schema.IndexSchema;
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
}
