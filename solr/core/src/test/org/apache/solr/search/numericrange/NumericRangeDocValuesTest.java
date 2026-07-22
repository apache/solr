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
package org.apache.solr.search.numericrange;

import java.util.ArrayList;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for docValues-specific behaviors (specially multiValued) of the range field types.
 *
 * <p>Plain query parity between the docValues and non-docValues fields (intersects / contains /
 * within / crosses / single-bound, across int/long/float/double and the docValues-only field) is
 * covered there instead: each of those tests randomizes its field over the indexed-only,
 * indexed+docValues, and (for int) docValues-only variants, so choosing the non-indexed
 * docValues-only field also exercises the pure {@code MultiBinaryRangeDocValuesQuery} path.
 *
 * <p>This also adds multiValued parity (several ranges packed into one docValues blob),
 * the {@link org.apache.lucene.search.IndexOrDocValuesQuery} clause
 * inside a selective conjunction (where the docValues clause leads iteration), and the fact that
 * enabling docValues does <em>not</em> enable sorting or value ({@code facet.field}) faceting even
 * though {@code facet.query} still works.
 */
public class NumericRangeDocValuesTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema-numericrange.xml");
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }

  /** Index the same int range into the indexed-only, indexed+docValues, and docValues-only fields. */
  private void addIntDoc(String id, String range) {
    assertU(
        adoc(
            "id", id,
            "price_range", range,
            "price_range_dv", range,
            "price_range_dvonly", range));
  }

  @Test
  public void testSelectiveConjunction() {
    // Exercises IndexOrDocValuesQuery inside a conjunction: the selective title clause leads and
    // the docValues range clause verifies. Must match the equivalent non-docValues query exactly.
    assertU(
        adoc(
            "id",
            "1",
            "title",
            "match",
            "price_range",
            "[100 TO 200]",
            "price_range_dv",
            "[100 TO 200]"));
    assertU(
        adoc(
            "id",
            "2",
            "title",
            "match",
            "price_range",
            "[300 TO 400]",
            "price_range_dv",
            "[300 TO 400]"));
    assertU(
        adoc(
            "id",
            "3",
            "title",
            "other",
            "price_range",
            "[100 TO 200]",
            "price_range_dv",
            "[100 TO 200]"));
    assertU(commit());

    for (String f : new String[] {"price_range", "price_range_dv"}) {
      assertQ(
          "selective AND contains on " + f,
          req("q", "+title:match +" + f + ":[120 TO 180]"),
          "//result[@numFound='1']",
          "//result/doc/str[@name='id'][.='1']");
    }
  }

  /** Indexes the given ranges into both a BKD-only and a docValues multiValued field. */
  private void addMvDoc(String id, String bkdField, String dvField, String... ranges) {
    List<String> fieldsAndValues = new ArrayList<>();
    fieldsAndValues.add("id");
    fieldsAndValues.add(id);
    for (String r : ranges) {
      fieldsAndValues.add(bkdField);
      fieldsAndValues.add(r);
      fieldsAndValues.add(dvField);
      fieldsAndValues.add(r);
    }
    assertU(adoc(fieldsAndValues.toArray(new String[0])));
  }

  /**
   * Asserts the BKD field and its docValues sibling both match exactly {@code expectedIds} (and only
   * those) for the query. Checks the matched ids, not just the count.
   */
  private void assertSameMatches(
      String criteria, String range, String bkdField, String dvField, String... expectedIds) {
    for (String f : new String[] {bkdField, dvField}) {
      List<String> tests = new ArrayList<>();
      // numFound + presence of each (unique) id together pin the exact match set.
      tests.add("//result[@numFound='" + expectedIds.length + "']");
      for (String id : expectedIds) {
        tests.add("//result/doc/str[@name='id'][.='" + id + "']");
      }
      assertQ(
          "criteria=" + criteria + " field=" + f + " " + range,
          req("q", "{!numericRange criteria=" + criteria + " field=" + f + "}" + range),
          tests.toArray(new String[0]));
    }
  }

  @Test
  public void testContainsPerRangeNotUnion() {
    // doc 1: two overlapping ranges whose *union* is [1,20]
    addMvDoc("1", "price_range_multi", "price_range_mv_dv", "[1 TO 10]", "[5 TO 20]");
    // doc 2: two disjoint ranges
    addMvDoc("2", "price_range_multi", "price_range_mv_dv", "[0 TO 5]", "[100 TO 200]");
    assertU(commit());

    // Neither individual range of doc 1 contains [1,15]
    assertSameMatches("contains", "[1 TO 15]", "price_range_multi", "price_range_mv_dv");

    // doc 2's [0,5] and [100,200] each satisfy one half-open bound of a CONTAINS([3,150]) split, so
    // a decomposition would falsely match -- native per-range matching must not.
    assertSameMatches("contains", "[3 TO 150]", "price_range_multi", "price_range_mv_dv");

    // doc 1's [1,10] actually contains [1,10] (any range suffices).
    assertSameMatches("contains", "[1 TO 10]", "price_range_multi", "price_range_mv_dv", "1");
  }

  @Test
  public void testAllRelationsMultiValued() {
    addMvDoc("1", "price_range_multi", "price_range_mv_dv", "[1 TO 10]", "[5 TO 20]");
    addMvDoc("2", "price_range_multi", "price_range_mv_dv", "[0 TO 5]", "[100 TO 200]");
    addMvDoc("3", "price_range_multi", "price_range_mv_dv", "[130 TO 160]");
    assertU(commit());

    // INTERSECTS [12,14]: only doc 1 (via its [5,20] range).
    assertSameMatches("intersects", "[12 TO 14]", "price_range_multi", "price_range_mv_dv", "1");
    // WITHIN [0,25]: doc 1 ([1,10]) and doc 2 ([0,5]) each have a range within it.
    assertSameMatches("within", "[0 TO 25]", "price_range_multi", "price_range_mv_dv", "1", "2");
    // CROSSES [8,50]: doc 1 crosses (its ranges overlap but aren't within); docs 2,3 are disjoint.
    assertSameMatches("crosses", "[8 TO 50]", "price_range_multi", "price_range_mv_dv", "1");
  }

  @Test
  public void test2dLongMultiValued() {
    addMvDoc(
        "1",
        "long_range_multi_2d",
        "long_range_mv_dv_2d",
        "[100,100 TO 200,200]",
        "[500,500 TO 600,600]");
    addMvDoc("2", "long_range_multi_2d", "long_range_mv_dv_2d", "[1,3000000000 TO 5,4000000000]");
    assertU(commit());

    // Overlaps doc 1's [100,100 TO 200,200] box in both dims.
    assertSameMatches(
        "intersects", "[150,100 TO 550,150]", "long_range_multi_2d", "long_range_mv_dv_2d", "1");
    // Overlaps doc 2's box (whose D2 spans 3e9..4e9).
    assertSameMatches(
        "intersects",
        "[2,3500000000 TO 5,3600000000]",
        "long_range_multi_2d",
        "long_range_mv_dv_2d",
        "2");
  }

  @Test
  public void testFloatMultiValued() {
    addMvDoc("1", "float_range_multi", "float_range_mv_dv", "[1.0 TO 2.0]", "[5.0 TO 6.0]");
    addMvDoc("2", "float_range_multi", "float_range_mv_dv", "[10.0 TO 20.0]");
    assertU(commit());

    // INTERSECTS [1.5,1.8]: only doc 1 (via its [1.0,2.0] range).
    assertSameMatches("intersects", "[1.5 TO 1.8]", "float_range_multi", "float_range_mv_dv", "1");
    // WITHIN [0.0,100.0]: both docs' ranges fit inside.
    assertSameMatches(
        "within", "[0.0 TO 100.0]", "float_range_multi", "float_range_mv_dv", "1", "2");
  }

  @Test
  public void test2dDoubleMultiValued() {
    addMvDoc(
        "1",
        "double_range_multi_2d",
        "double_range_mv_dv_2d",
        "[1.0,-3.1 TO 5.0,-1.1]",
        "[-5.5,10.0 TO 1.5,15.0]");
    addMvDoc("2", "double_range_multi_2d", "double_range_mv_dv_2d", "[-10,2.5 TO 0,3.5]");
    assertU(commit());

    // Ranges per doc (each a 2D bounding box):
    //   doc1 A: D1 [1.0, 5.0]    D2 [-3.1, -1.1]
    //   doc1 B: D1 [-5.5, 1.5]   D2 [10.0, 15.0]
    //   doc2 C: D1 [-10.0, 0.0]  D2 [2.5, 3.5]

    // CONTAINS: a doc range must wrap the query box in *both* dims. doc1's A wraps
    // [2,4]x[-3.0,-1.15] (1<=2, 5>=4 and -3.1<=-3.0, -1.1>=-1.15).
    // doc2's C does not (its D1 max 0 < 4). -> only doc 1.
    assertSameMatches(
        "contains",
        "[2.0,-3.0 TO 4.0,-1.15]",
        "double_range_multi_2d",
        "double_range_mv_dv_2d",
        "1");

    // WITHIN: a doc range must fit *inside* the query box in both dims. doc1's A fits inside
    // [0,6]x[-4,-1]; doc2's C sticks out on D1 (min -10 < 0). -> only doc 1.
    assertSameMatches(
        "within",
        "[0.0,-4.0 TO 6.0,-1.0]",
        "double_range_multi_2d",
        "double_range_mv_dv_2d",
        "1");

    // INTERSECTS: overlap in both dims is enough. The central box [-2,3]x[-2,3] overlaps doc1's A
    // and doc2's C, so both match (any-range / any-doc "OR" semantics). -> 2 docs.
    assertSameMatches(
        "intersects",
        "[-2.0,-2.0 TO 3.0,3.0]",
        "double_range_multi_2d",
        "double_range_mv_dv_2d",
        "1",
        "2");

    // CROSSES = intersects AND NOT within. For [-1,6]x[-4,3], doc1's A is fully *within* it (so
    // does not cross) and its B is disjoint -> doc 1 excluded; doc2's C overlaps but pokes out on
    // D1 (min -10) -> it crosses. -> only doc 2, which distinguishes crosses from within.
    assertSameMatches(
        "crosses",
        "[-1.0,-4.0 TO 6.0,3.0]",
        "double_range_multi_2d",
        "double_range_mv_dv_2d",
        "2");
  }

  @Test
  public void testSortingUnsupportedEvenWithDocValues() {
    addIntDoc("1", "[100 TO 200]");
    assertU(commit());
    // Range fields don't implement sorting: there's no canonical key for ordering intervals (by
    // min? max? width? midpoint?), multi-dimensional ranges (bbox/cube/tesseract) have no sensible
    // order at all, and the packed BINARY docValues isn't a sortable scalar. Enabling docValues
    // does not change that, getSortField still throws.
    assertQEx(
        "sorting on a range field must fail even with docValues",
        "Cannot sort on",
        req("q", "*:*", "sort", "price_range_dv asc"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testFieldFacetingUnsupportedEvenWithDocValues() {
    addIntDoc("1", "[100 TO 200]");
    addIntDoc("2", "[150 TO 250]");
    assertU(commit());
    // Value faceting (facet.field) isn't supported on range fields: Use facet.query
    // for ranges instead (see testQueryFacetingWithDocValues).
    assertQ(
        "value faceting on a range field yields no buckets even with docValues",
        req("q", "*:*", "rows", "0", "facet", "true", "facet.field", "price_range_dv"),
        "//lst[@name='facet_fields']/lst[@name='price_range_dv']",
        "count(//lst[@name='facet_fields']/lst[@name='price_range_dv']/int)=0");
  }

  @Test
  public void testQueryFacetingWithDocValues() {
    // Range fields support facet.query (count docs matching a range query), the realistic way to
    // facet ranges in Solr. (facet.field / value faceting is NOT supported: range docValues are
    // BINARY, not the SortedSet docValues that term-faceting needs)
    addIntDoc("1", "[100 TO 200]");
    addIntDoc("2", "[150 TO 250]");
    addIntDoc("3", "[50 TO 80]");
    assertU(commit());

    assertQ(
        req(
            "q", "*:*",
            "facet", "true",
            "facet.query",
                "{!numericRange criteria=intersects field=price_range_dv key=pr}[120 TO 180]"),
        "//lst[@name='facet_queries']/int[@name='pr'][.='2']");
  }
}
