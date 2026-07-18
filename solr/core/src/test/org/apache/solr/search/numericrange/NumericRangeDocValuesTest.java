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

import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests over the {@code schema-numericrange.xml} verifying that the docValues-enabled
 * range fields return exactly the same results as their indexed-only counterparts.
 *
 * <p>The docValues-enabled fields wrap their BKD query in a Lucene {@link
 * org.apache.lucene.search.IndexOrDocValuesQuery}. These tests assert that this produces identical
 * matches to the direct BKD query across intersects / contains / single-bound / multi-dimensional
 * queries, inside a selective conjunction, and for the docValues-only (non-indexed) field.
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

  /**
   * Index the same int range into the indexed-only, indexed+docValues, and docValues-only fields.
   */
  private void addIntDoc(String id, String range) {
    assertU(
        adoc(
            "id", id,
            "price_range", range,
            "price_range_dv", range,
            "price_range_dvonly", range));
  }

  private List<String> get1dIntRangeFieldNames() {
    return List.of("price_range", "price_range_dv", "price_range_dvonly");
  }

  @Test
  public void testIntersectsParity() {
    addIntDoc("1", "[100 TO 200]");
    addIntDoc("2", "[150 TO 250]");
    addIntDoc("3", "[50 TO 80]");
    assertU(commit());

    // intersects [120 TO 180] -> docs 1 and 2, for every field variant
    for (String field : get1dIntRangeFieldNames()) {
      assertQ(
          "intersects on " + field,
          req("q", "{!numericRange criteria=intersects field=" + field + "}[120 TO 180]"),
          "//result[@numFound='2']",
          "//result/doc/str[@name='id'][.='1']",
          "//result/doc/str[@name='id'][.='2']");
    }
  }

  @Test
  public void testContainsParity() {
    addIntDoc("1", "[100 TO 200]"); // fully contains [120 TO 180]
    addIntDoc("2", "[130 TO 160]"); // within, does NOT contain
    addIntDoc("3", "[150 TO 250]"); // partial overlap, does NOT contain
    assertU(commit());

    for (String field : get1dIntRangeFieldNames()) {
      assertQ(
          "contains on " + field,
          req("q", "{!numericRange criteria=contains field=" + field + "}[120 TO 180]"),
          "//result[@numFound='1']",
          "//result/doc/str[@name='id'][.='1']");
    }
  }

  @Test
  public void testStandardSyntaxContainsParity() {
    addIntDoc("1", "[100 TO 200]"); // contains [120 TO 180]
    addIntDoc("2", "[130 TO 160]"); // no
    assertU(commit());

    for (String field : get1dIntRangeFieldNames()) {
      assertQ(
          "standard-syntax contains on " + field,
          req("q", field + ":[120 TO 180]"),
          "//result[@numFound='1']",
          "//result/doc/str[@name='id'][.='1']");
    }
  }

  @Test
  public void testSingleBoundParity() {
    // A single bound (field:150) is a degenerate range [150,150]; contains == intersects for a
    // point.
    addIntDoc("1", "[100 TO 200]"); // contains 150
    addIntDoc("2", "[150 TO 150]"); // contains 150
    addIntDoc("3", "[100 TO 149]"); // no (max below 150)
    addIntDoc("4", "[151 TO 300]"); // no (min above 150)
    assertU(commit());

    for (String f : get1dIntRangeFieldNames()) {
      assertQ(
          "single-bound on " + f,
          req("q", f + ":150"),
          "//result[@numFound='2']",
          "//result/doc/str[@name='id'][.='1']",
          "//result/doc/str[@name='id'][.='2']");
    }
  }

  @Test
  public void test2DContainsParity() {
    assertU(adoc("id", "1", "bbox", "[0,0 TO 10,10]", "bbox_dv", "[0,0 TO 10,10]")); // contains
    assertU(adoc("id", "2", "bbox", "[4,4 TO 6,6]", "bbox_dv", "[4,4 TO 6,6]")); // does not contain
    assertU(commit());

    for (String f : new String[] {"bbox", "bbox_dv"}) {
      assertQ(
          "2D contains on " + f,
          req("q", "{!numericRange criteria=contains field=" + f + "}[3,3 TO 7,7]"),
          "//result[@numFound='1']",
          "//result/doc/str[@name='id'][.='1']");
    }
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

  @Test
  public void testLongParity() {
    assertU(adoc("id", "1", "long_range", "[100 TO 200]", "long_range_dv", "[100 TO 200]"));
    assertU(adoc("id", "2", "long_range", "[150 TO 250]", "long_range_dv", "[150 TO 250]"));
    assertU(adoc("id", "3", "long_range", "[50 TO 80]", "long_range_dv", "[50 TO 80]"));
    assertU(commit());

    for (String f : new String[] {"long_range", "long_range_dv"}) {
      assertQ(
          "long intersects on " + f,
          req("q", "{!numericRange criteria=intersects field=" + f + "}[120 TO 180]"),
          "//result[@numFound='2']");
    }
  }

  @Test
  public void testFloatIntersectsParity() {
    assertU(adoc("id", "1", "float_range", "[1.0 TO 2.0]", "float_range_dv", "[1.0 TO 2.0]"));
    assertU(adoc("id", "2", "float_range", "[1.5 TO 3.0]", "float_range_dv", "[1.5 TO 3.0]"));
    assertU(adoc("id", "3", "float_range", "[5.0 TO 6.0]", "float_range_dv", "[5.0 TO 6.0]"));
    assertU(commit());

    for (String f : new String[] {"float_range", "float_range_dv"}) {
      assertQ(
          "float intersects on " + f,
          req("q", "{!numericRange criteria=intersects field=" + f + "}[1.2 TO 1.8]"),
          "//result[@numFound='2']");
    }
  }

  @Test
  public void testDoubleContainsParity() {
    // doc 1 fully contains [2.0 TO 4.0]; doc 2 is within it (does not contain)
    assertU(adoc("id", "1", "double_range", "[1.0 TO 5.0]", "double_range_dv", "[1.0 TO 5.0]"));
    assertU(adoc("id", "2", "double_range", "[2.5 TO 3.5]", "double_range_dv", "[2.5 TO 3.5]"));
    assertU(commit());

    for (String f : new String[] {"double_range", "double_range_dv"}) {
      assertQ(
          "double contains on " + f,
          req("q", "{!numericRange criteria=contains field=" + f + "}[2.0 TO 4.0]"),
          "//result[@numFound='1']",
          "//result/doc/str[@name='id'][.='1']");
    }
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
