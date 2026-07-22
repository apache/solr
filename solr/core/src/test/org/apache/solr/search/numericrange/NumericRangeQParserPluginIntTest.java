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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link NumericRangeQParserPlugin} using {@link
 * org.apache.solr.schema.numericrange.IntRangeField} fields.
 *
 * <p>Each scenario picks its field at random (via {@link #randomField}) from the equivalent field
 * variants for 1D: indexed-only, indexed+docValues, and the (non-indexed) docValues-only field
 * so all of those code paths, including the pure docValues query, get incidental coverage across
 * seeds while the assertions stay identical.
 */
public class NumericRangeQParserPluginIntTest extends SolrTestCaseJ4 {

  /**
   * 1D int range variants: indexed-only, indexed+docValues, and (non-indexed) docValues-only.
   */
  private static final String[] ONE_D = {"price_range", "price_range_dv", "price_range_dvonly"};

  /** 2D int range (bounding box): indexed-only field and its indexed+docValues sibling. */
  private static final String[] TWO_D = {"bbox", "bbox_dv"};

  /** Randomly returns one of the given field variants so DV / non-DV both get covered. */
  private static String randomField(String... variants) {
    return variants[random().nextInt(variants.length)];
  }

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

  @Test
  public void test1DIntersectsQuery() {
    String f = randomField(ONE_D);
    // Index documents with 1D ranges
    assertU(adoc("id", "1", f, "[100 TO 200]"));
    assertU(adoc("id", "2", f, "[150 TO 250]"));
    assertU(adoc("id", "3", f, "[50 TO 80]"));
    assertU(adoc("id", "4", f, "[200 TO 300]"));
    assertU(commit());

    // Query: find ranges intersecting [120 TO 180]
    assertQ(
        req("q", "{!numericRange criteria=intersects field=" + f + "}[120 TO 180]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']",
        "//result/doc/str[@name='" + f + "'][.='[100 TO 200]']",
        "//result/doc/str[@name='" + f + "'][.='[150 TO 250]']");

    // Query: find ranges intersecting [0 TO 100]
    assertQ(
        req("q", "{!numericRange criteria=intersects field=" + f + "}[0 TO 100]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='3']");

    // Query: find ranges intersecting [175 TO 225]
    assertQ(
        req("q", "{!numericRange criteria=intersects field=" + f + "}[175 TO 225]"),
        "//result[@numFound='3']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']",
        "//result/doc/str[@name='id'][.='4']");
  }

  @Test
  public void test1DWithinQuery() {
    String f = randomField(ONE_D);
    assertU(adoc("id", "1", f, "[100 TO 200]"));
    assertU(adoc("id", "2", f, "[150 TO 250]"));
    assertU(adoc("id", "3", f, "[50 TO 80]"));
    assertU(commit());

    // Query: find ranges within [0 TO 300]
    assertQ(
        req("q", "{!numericRange criteria=\"within\" field=" + f + "}[0 TO 300]"),
        "//result[@numFound='3']");

    // Query: find ranges within [100 TO 200]
    assertQ(
        req("q", "{!numericRange criteria=\"within\" field=" + f + "}[100 TO 200]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");

    // Query: find ranges within [0 TO 100]
    assertQ(
        req("q", "{!numericRange criteria=\"within\" field=" + f + "}[0 TO 100]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='3']");
  }

  @Test
  public void test1DContainsQuery() {
    String f = randomField(ONE_D);
    assertU(adoc("id", "1", f, "[100 TO 200]"));
    assertU(adoc("id", "2", f, "[150 TO 250]"));
    assertU(adoc("id", "3", f, "[50 TO 300]"));
    assertU(commit());

    // Query: find ranges containing [160 TO 170]
    assertQ(
        req("q", "{!numericRange criteria=\"contains\" field=" + f + "}[160 TO 170]"),
        "//result[@numFound='3']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']",
        "//result/doc/str[@name='id'][.='3']");

    // Query: find ranges containing [0 TO 400]
    assertQ(
        req("q", "{!numericRange criteria=\"contains\" field=" + f + "}[0 TO 400]"),
        "//result[@numFound='0']");

    // Query: find ranges containing [100 TO 200]
    assertQ(
        req("q", "{!numericRange criteria=\"contains\" field=" + f + "}[100 TO 200]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='3']");
  }

  @Test
  public void test1DCrossesQuery() {
    String f = randomField(ONE_D);
    assertU(adoc("id", "1", f, "[100 TO 200]"));
    assertU(adoc("id", "2", f, "[150 TO 250]"));
    assertU(adoc("id", "3", f, "[50 TO 80]"));
    assertU(adoc("id", "4", f, "[120 TO 180]"));
    assertU(commit());

    // Query: find ranges crossing [150 TO 250]
    // Should match ranges that intersect but are not within
    assertQ(
        req("q", "{!numericRange criteria=\"crosses\" field=" + f + "}[150 TO 250]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='4']");
  }

  @Test
  public void test2DIntersectsQuery() {
    String f = randomField(TWO_D);
    // Index documents with 2D ranges (bounding boxes)
    assertU(adoc("id", "1", f, "[0,0 TO 10,10]"));
    assertU(adoc("id", "2", f, "[5,5 TO 15,15]"));
    assertU(adoc("id", "3", f, "[20,20 TO 30,30]"));
    assertU(commit());

    // Query: find bboxes intersecting [8,8 TO 12,12]
    assertQ(
        req("q", "{!numericRange criteria=intersects field=" + f + "}[8,8 TO 12,12]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']",
        "//result/doc/str[@name='" + f + "'][.='[0,0 TO 10,10]']",
        "//result/doc/str[@name='" + f + "'][.='[5,5 TO 15,15]']");

    // Query: find bboxes intersecting [25,25 TO 35,35]
    assertQ(
        req("q", "{!numericRange criteria=intersects field=" + f + "}[25,25 TO 35,35]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='3']");

    // Query: find bboxes intersecting [100,100 TO 200,200]
    assertQ(
        req("q", "{!numericRange criteria=intersects field=" + f + "}[100,100 TO 200,200]"),
        "//result[@numFound='0']");
  }

  @Test
  public void test2DWithinQuery() {
    String f = randomField(TWO_D);
    assertU(adoc("id", "1", f, "[5,5 TO 10,10]"));
    assertU(adoc("id", "2", f, "[0,0 TO 20,20]"));
    assertU(adoc("id", "3", f, "[15,15 TO 25,25]"));
    assertU(commit());

    // Query: find bboxes within [0,0 TO 20,20]
    assertQ(
        req("q", "{!numericRange criteria=\"within\" field=" + f + "}[0,0 TO 20,20]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");

    // Query: find bboxes within [0,0 TO 30,30]
    assertQ(
        req("q", "{!numericRange criteria=\"within\" field=" + f + "}[0,0 TO 30,30]"),
        "//result[@numFound='3']");
  }

  @Test
  public void test2DContainsQuery() {
    String f = randomField(TWO_D);
    assertU(adoc("id", "1", f, "[0,0 TO 10,10]")); // contains [3,3 TO 7,7]
    assertU(adoc("id", "2", f, "[4,4 TO 6,6]")); // within it, does NOT contain
    assertU(commit());

    // Query: find bboxes containing [3,3 TO 7,7]
    assertQ(
        req("q", "{!numericRange criteria=\"contains\" field=" + f + "}[3,3 TO 7,7]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");
  }

  @Test
  public void test3DQuery() {
    // Index documents with 3D ranges (bounding cubes)
    assertU(adoc("id", "1", "cube", "[0,0,0 TO 10,10,10]"));
    assertU(adoc("id", "2", "cube", "[5,5,5 TO 15,15,15]"));
    assertU(commit());

    // Query: find cubes intersecting [8,8,8 TO 12,12,12]
    assertQ(
        req("q", "{!numericRange criteria=intersects field=cube}[8,8,8 TO 12,12,12]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");
  }

  @Test
  public void test4DQuery() {
    // Index documents with 4D ranges (tesseracts)
    assertU(adoc("id", "1", "tesseract", "[0,0,0,0 TO 10,10,10,10]"));
    assertU(adoc("id", "2", "tesseract", "[5,5,5,5 TO 15,15,15,15]"));
    assertU(commit());

    // Query: find tesseracts intersecting [8,8,8,8 TO 12,12,12,12]
    assertQ(
        req("q", "{!numericRange criteria=intersects field=tesseract}[8,8,8,8 TO 12,12,12,12]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");
  }

  @Test
  public void testMultiValuedField() {
    // Index document with multiple ranges
    assertU(
        adoc("id", "1", "price_range_multi", "[100 TO 200]", "price_range_multi", "[300 TO 400]"));
    assertU(adoc("id", "2", "price_range_multi", "[150 TO 250]"));
    assertU(commit());

    // Query should match doc 1 via first range
    assertQ(
        req("q", "{!numericRange criteria=intersects field=price_range_multi}[110 TO 120]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/arr[@name='price_range_multi']/str[1][.='[100 TO 200]']",
        "//result/doc/arr[@name='price_range_multi']/str[2][.='[300 TO 400]']");

    // Query should match doc 1 via second range
    assertQ(
        req("q", "{!numericRange criteria=intersects field=price_range_multi}[310 TO 320]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");

    // Query should match both docs
    assertQ(
        req("q", "{!numericRange criteria=intersects field=price_range_multi}[150 TO 250]"),
        "//result[@numFound='2']");
  }

  @Test
  public void testMissingFieldParameter() {
    assertU(adoc("id", "1", "price_range", "[100 TO 200]"));
    assertU(commit());

    // Query without field parameter should fail
    assertQEx(
        "Missing field parameter should fail",
        "Missing required parameter: field",
        req("q", "{!numericRange criteria=intersects}[100 TO 200]"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testMissingCriteriaParameter() {
    assertU(adoc("id", "1", "price_range", "[100 TO 200]"));
    assertU(commit());

    // Query without field parameter should fail
    assertQEx(
        "Missing criteria parameter should fail",
        "Missing required parameter: criteria",
        req("q", "{!numericRange field=asdf}[100 TO 200]"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testInvalidFieldType() {
    assertU(adoc("id", "1", "title", "test"));
    assertU(commit());

    // Query on non-IntRangeField should fail
    assertQEx(
        "Query on wrong field type should fail",
        "must be a numeric range field type",
        req("q", "{!numericRange criteria=intersects field=title}[100 TO 200]"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testInvalidQueryType() {
    assertU(adoc("id", "1", "price_range", "[100 TO 200]"));
    assertU(commit());

    // Query with invalid criteria parameter should fail
    assertQEx(
        "Invalid query criteria should fail",
        "Unknown query criteria",
        req("q", "{!numericRange criteria=\"invalid\" field=price_range}[100 TO 200]"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testInvalidRangeValue() {
    assertU(adoc("id", "1", "price_range", "[100 TO 200]"));
    assertU(commit());

    // Query with invalid range format should fail
    assertQEx(
        "Invalid range format should fail",
        "Invalid range",
        req("q", "{!numericRange criteria=intersects field=price_range}invalid"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testEmptyRangeValue() {
    assertU(adoc("id", "1", "price_range", "[100 TO 200]"));
    assertU(commit());

    // Query with empty range should fail
    assertQEx(
        "Empty range value should fail",
        req("q", "{!numericRange criteria=intersects field=price_range}"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testNegativeValues() {
    String f = randomField(ONE_D);
    assertU(adoc("id", "1", f, "[-100 TO -50]"));
    assertU(adoc("id", "2", f, "[-75 TO -25]"));
    assertU(commit());

    // Query with negative range
    assertQ(
        req("q", "{!numericRange criteria=intersects field=" + f + "}[-80 TO -60]"),
        "//result[@numFound='2']");
  }

  @Test
  public void testExtremeValues() {
    String f = randomField(ONE_D);
    int min = Integer.MIN_VALUE;
    int max = Integer.MAX_VALUE;

    assertU(adoc("id", "1", f, "[" + min + " TO " + max + "]"));
    assertU(commit());

    // Query should match the extreme range
    assertQ(
        req("q", "{!numericRange criteria=intersects field=" + f + "}[0 TO 100]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");
  }

  @Test
  public void testPointRange() {
    String f = randomField(ONE_D);
    // Point range where min == max
    assertU(adoc("id", "1", f, "[100 TO 100]"));
    assertU(commit());

    // Intersects query with point
    assertQ(
        req("q", "{!numericRange criteria=intersects field=" + f + "}[100 TO 100]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");

    // Intersects query containing point
    assertQ(
        req("q", "{!numericRange criteria=intersects field=" + f + "}[50 TO 150]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");
  }

  // ------------------------------------------
  // These tests don't use the IntRangeQParser, but rather the use of IntRangeField through the more
  // standard query parser(s).  Not really the best place, but it didn't seem worth creating a whole
  // new test class for this.  Maybe worth a refactor later. When used with parsers other than
  // {!numericRange} we default to "contains" semantics.  In other words: "match only those
  // documents with a field-val that fully contains the query value.

  @Test
  public void testGetFieldQueryFullRange() {
    String f = randomField(ONE_D);
    // doc 1: narrow range, fully inside the query range  → should NOT match (doc contains query)
    // doc 2: wide range that fully contains the query range → should match
    // doc 3: range that only partially overlaps            → should NOT match
    assertU(adoc("id", "1", f, "[130 TO 160]")); // No match
    assertU(adoc("id", "2", f, "[100 TO 200]")); // Match!
    assertU(adoc("id", "3", f, "[150 TO 250]")); // No match
    assertU(commit());

    // Contains semantics: find indexed ranges that fully contain [120 TO 180]
    assertQ(
        req("q", f + ":[120 TO 180]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='2']");
  }

  @Test
  public void testGetFieldQueryFullRangeMultipleMatches() {
    String f = randomField(ONE_D);
    assertU(adoc("id", "1", f, "[0 TO 1000]")); // Match!
    assertU(adoc("id", "2", f, "[100 TO 200]")); // Match!
    assertU(adoc("id", "3", f, "[100 TO 199]")); // No match - max too low
    assertU(adoc("id", "4", f, "[101 TO 200]")); // No match - min too high
    assertU(commit());

    assertQ(
        req("q", f + ":[100 TO 200]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");
  }

  @Test
  public void testGetFieldQuerySingleBound() {
    String f = randomField(ONE_D);
    // Single-bound syntax: <field>:150 is sugar for contains([150 TO 150])
    assertU(adoc("id", "1", f, "[100 TO 200]")); // Match!
    assertU(adoc("id", "2", f, "[150 TO 150]")); // Match!
    assertU(adoc("id", "3", f, "[100 TO 149]")); // No match - max below 150
    assertU(adoc("id", "4", f, "[151 TO 300]")); // No match - min above 150
    assertU(commit());

    assertQ(
        req("q", f + ":150"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");
  }

  @Test
  public void testGetFieldQuerySingleBound2D() {
    String f = randomField(TWO_D);
    // 2D single-bound: <field>:5,5 is sugar for contains([5,5 TO 5,5])
    assertU(adoc("id", "1", f, "[0,0 TO 10,10]")); // Match!
    assertU(adoc("id", "2", f, "[5,5 TO 5,5]")); // Match!
    assertU(adoc("id", "3", f, "[0,0 TO 4,10]")); // No match - X dimension ends too low
    assertU(adoc("id", "4", f, "[6,0 TO 10,10]")); // No match - X dimension starts too high
    assertU(commit());

    assertQ(
        req("q", f + ":5,5"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");
  }

  @Test
  public void testGetFieldQueryFieldFormatting() {
    String f1 = randomField(ONE_D);
    String f2 = randomField(TWO_D);
    // Test 1D field formatting
    assertU(adoc("id", "1", f1, "[100 TO 200]"));
    // Test 2D field formatting
    assertU(adoc("id", "2", f2, "[10,20 TO 30,40]"));
    // Test 3D field formatting
    assertU(adoc("id", "3", "cube", "[5,10,15 TO 25,30,35]"));
    // Test 4D field formatting
    assertU(adoc("id", "4", "tesseract", "[1,2,3,4 TO 11,12,13,14]"));
    // Test multi-valued field formatting
    assertU(
        adoc(
            "id",
            "5",
            "price_range_multi",
            "[50 TO 100]",
            "price_range_multi",
            "[200 TO 300]",
            "price_range_multi",
            "[400 TO 500]"));
    assertU(commit());

    // Verify 1D field returns correctly formatted value
    assertQ(
        req("q", "id:1"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='" + f1 + "'][.='[100 TO 200]']");

    // Verify 2D field returns correctly formatted value
    assertQ(
        req("q", "id:2"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='" + f2 + "'][.='[10,20 TO 30,40]']");

    // Verify 3D field returns correctly formatted value
    assertQ(
        req("q", "id:3"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='cube'][.='[5,10,15 TO 25,30,35]']");

    // Verify 4D field returns correctly formatted value
    assertQ(
        req("q", "id:4"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='tesseract'][.='[1,2,3,4 TO 11,12,13,14]']");

    // Verify multi-valued field returns all values correctly formatted
    assertQ(
        req("q", "id:5"),
        "//result[@numFound='1']",
        "//result/doc/arr[@name='price_range_multi']/str[1][.='[50 TO 100]']",
        "//result/doc/arr[@name='price_range_multi']/str[2][.='[200 TO 300]']",
        "//result/doc/arr[@name='price_range_multi']/str[3][.='[400 TO 500]']");
  }
}
