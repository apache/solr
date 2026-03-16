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
 * org.apache.solr.schema.numericrange.LongRangeField} fields.
 */
public class NumericRangeQParserPluginLongTest extends SolrTestCaseJ4 {

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
    assertU(adoc("id", "1", "long_range", "[100 TO 200]"));
    assertU(adoc("id", "2", "long_range", "[150 TO 250]"));
    assertU(adoc("id", "3", "long_range", "[50 TO 80]"));
    assertU(adoc("id", "4", "long_range", "[200 TO 300]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=intersects field=long_range}[120 TO 180]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']",
        "//result/doc/str[@name='long_range'][.='[100 TO 200]']",
        "//result/doc/str[@name='long_range'][.='[150 TO 250]']");

    assertQ(
        req("q", "{!numericRange criteria=intersects field=long_range}[0 TO 100]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='3']");

    assertQ(
        req("q", "{!numericRange criteria=intersects field=long_range}[175 TO 225]"),
        "//result[@numFound='3']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']",
        "//result/doc/str[@name='id'][.='4']");
  }

  @Test
  public void test1DWithinQuery() {
    assertU(adoc("id", "1", "long_range", "[100 TO 200]"));
    assertU(adoc("id", "2", "long_range", "[150 TO 250]"));
    assertU(adoc("id", "3", "long_range", "[50 TO 80]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=\"within\" field=long_range}[0 TO 300]"),
        "//result[@numFound='3']");

    assertQ(
        req("q", "{!numericRange criteria=\"within\" field=long_range}[100 TO 200]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");

    assertQ(
        req("q", "{!numericRange criteria=\"within\" field=long_range}[0 TO 100]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='3']");
  }

  @Test
  public void test1DContainsQuery() {
    assertU(adoc("id", "1", "long_range", "[100 TO 200]"));
    assertU(adoc("id", "2", "long_range", "[150 TO 250]"));
    assertU(adoc("id", "3", "long_range", "[50 TO 300]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=\"contains\" field=long_range}[160 TO 170]"),
        "//result[@numFound='3']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']",
        "//result/doc/str[@name='id'][.='3']");

    assertQ(
        req("q", "{!numericRange criteria=\"contains\" field=long_range}[0 TO 400]"),
        "//result[@numFound='0']");

    assertQ(
        req("q", "{!numericRange criteria=\"contains\" field=long_range}[100 TO 200]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='3']");
  }

  @Test
  public void test1DCrossesQuery() {
    assertU(adoc("id", "1", "long_range", "[100 TO 200]"));
    assertU(adoc("id", "2", "long_range", "[150 TO 250]"));
    assertU(adoc("id", "3", "long_range", "[50 TO 80]"));
    assertU(adoc("id", "4", "long_range", "[120 TO 180]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=\"crosses\" field=long_range}[150 TO 250]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='4']");
  }

  @Test
  public void test2DIntersectsQuery() {
    assertU(adoc("id", "1", "long_range_2d", "[0,0 TO 10,10]"));
    assertU(adoc("id", "2", "long_range_2d", "[5,5 TO 15,15]"));
    assertU(adoc("id", "3", "long_range_2d", "[20,20 TO 30,30]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=intersects field=long_range_2d}[8,8 TO 12,12]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");

    assertQ(
        req("q", "{!numericRange criteria=intersects field=long_range_2d}[25,25 TO 35,35]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='3']");

    assertQ(
        req("q", "{!numericRange criteria=intersects field=long_range_2d}[100,100 TO 200,200]"),
        "//result[@numFound='0']");
  }

  @Test
  public void test3DQuery() {
    assertU(adoc("id", "1", "long_range_3d", "[0,0,0 TO 10,10,10]"));
    assertU(adoc("id", "2", "long_range_3d", "[5,5,5 TO 15,15,15]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=intersects field=long_range_3d}[8,8,8 TO 12,12,12]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");
  }

  @Test
  public void test4DQuery() {
    assertU(adoc("id", "1", "long_range_4d", "[0,0,0,0 TO 10,10,10,10]"));
    assertU(adoc("id", "2", "long_range_4d", "[5,5,5,5 TO 15,15,15,15]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=intersects field=long_range_4d}[8,8,8,8 TO 12,12,12,12]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");
  }

  @Test
  public void testMultiValuedField() {
    assertU(
        adoc("id", "1", "long_range_multi", "[100 TO 200]", "long_range_multi", "[300 TO 400]"));
    assertU(adoc("id", "2", "long_range_multi", "[150 TO 250]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=intersects field=long_range_multi}[110 TO 120]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/arr[@name='long_range_multi']/str[1][.='[100 TO 200]']",
        "//result/doc/arr[@name='long_range_multi']/str[2][.='[300 TO 400]']");

    assertQ(
        req("q", "{!numericRange criteria=intersects field=long_range_multi}[310 TO 320]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");

    assertQ(
        req("q", "{!numericRange criteria=intersects field=long_range_multi}[150 TO 250]"),
        "//result[@numFound='2']");
  }

  @Test
  public void testMissingFieldParameter() {
    assertQEx(
        "Missing field parameter should fail",
        "Missing required parameter: field",
        req("q", "{!numericRange criteria=intersects}[100 TO 200]"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testMissingCriteriaParameter() {
    assertQEx(
        "Missing criteria parameter should fail",
        "Missing required parameter: criteria",
        req("q", "{!numericRange field=long_range}[100 TO 200]"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testInvalidFieldType() {
    // Query on a plain string field should fail
    assertQEx(
        "Query on wrong field type should fail",
        "must be a numeric range field type",
        req("q", "{!numericRange criteria=intersects field=title}[100 TO 200]"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testInvalidQueryType() {
    assertU(adoc("id", "1", "long_range", "[100 TO 200]"));
    assertU(commit());

    assertQEx(
        "Invalid query criteria should fail",
        "Unknown query criteria",
        req("q", "{!numericRange criteria=\"invalid\" field=long_range}[100 TO 200]"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testInvalidRangeValue() {
    assertU(adoc("id", "1", "long_range", "[100 TO 200]"));
    assertU(commit());

    assertQEx(
        "Invalid range format should fail",
        "Invalid range",
        req("q", "{!numericRange criteria=intersects field=long_range}invalid"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testEmptyRangeValue() {
    assertU(adoc("id", "1", "long_range", "[100 TO 200]"));
    assertU(commit());

    assertQEx(
        "Empty range value should fail",
        req("q", "{!numericRange criteria=intersects field=long_range}"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testNegativeValues() {
    assertU(adoc("id", "1", "long_range", "[-100 TO -50]"));
    assertU(adoc("id", "2", "long_range", "[-75 TO -25]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=intersects field=long_range}[-80 TO -60]"),
        "//result[@numFound='2']");
  }

  @Test
  public void testValuesOutsideIntRange() {
    // Values that cannot be stored in an int but are valid longs
    long min = 3_000_000_000L;
    long max = 4_000_000_000L;

    assertU(adoc("id", "1", "long_range", "[" + min + " TO " + max + "]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=intersects field=long_range}[3500000000 TO 3600000000]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");
  }

  @Test
  public void testExtremeValues() {
    long min = Long.MIN_VALUE;
    long max = Long.MAX_VALUE;

    assertU(adoc("id", "1", "long_range", "[" + min + " TO " + max + "]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=intersects field=long_range}[0 TO 100]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");
  }

  @Test
  public void testPointRange() {
    assertU(adoc("id", "1", "long_range", "[100 TO 100]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=intersects field=long_range}[100 TO 100]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");

    assertQ(
        req("q", "{!numericRange criteria=intersects field=long_range}[50 TO 150]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");
  }

  // ------------------------------------------
  // Tests for getFieldQuery and getSpecializedRangeQuery via the standard query parser.
  // These default to "contains" semantics.

  @Test
  public void testGetFieldQueryFullRange() {
    // doc 1: narrow range, fully inside the query range  → should NOT match (doc contains query)
    // doc 2: wide range that fully contains the query range → should match
    // doc 3: range that only partially overlaps            → should NOT match
    assertU(adoc("id", "1", "long_range", "[130 TO 160]")); // No match
    assertU(adoc("id", "2", "long_range", "[100 TO 200]")); // Match!
    assertU(adoc("id", "3", "long_range", "[150 TO 250]")); // No match
    assertU(commit());

    // Contains semantics: find indexed ranges that fully contain [120 TO 180]
    assertQ(
        req("q", "long_range:[120 TO 180]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='2']");
  }

  @Test
  public void testGetFieldQueryFullRangeMultipleMatches() {
    assertU(adoc("id", "1", "long_range", "[0 TO 1000]")); // Match!
    assertU(adoc("id", "2", "long_range", "[100 TO 200]")); // Match!
    assertU(adoc("id", "3", "long_range", "[100 TO 199]")); // No match - max too low
    assertU(adoc("id", "4", "long_range", "[101 TO 200]")); // No match - min too high
    assertU(commit());

    assertQ(
        req("q", "long_range:[100 TO 200]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");
  }

  @Test
  public void testGetFieldQuerySingleBound() {
    // Single-bound syntax: long_range:150 is sugar for contains([150 TO 150])
    assertU(adoc("id", "1", "long_range", "[100 TO 200]")); // Match!
    assertU(adoc("id", "2", "long_range", "[150 TO 150]")); // Match!
    assertU(adoc("id", "3", "long_range", "[100 TO 149]")); // No match - max below 150
    assertU(adoc("id", "4", "long_range", "[151 TO 300]")); // No match - min above 150
    assertU(commit());

    assertQ(
        req("q", "long_range:150"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");
  }

  @Test
  public void testGetFieldQuerySingleBound2D() {
    // 2D single-bound: bbox:5,5 is sugar for contains([5,5 TO 5,5])
    assertU(adoc("id", "1", "bbox", "[0,0 TO 10,10]")); // Match!
    assertU(adoc("id", "2", "bbox", "[5,5 TO 5,5]")); // Match!
    assertU(adoc("id", "3", "bbox", "[0,0 TO 4,10]")); // No match - X dimension ends too low
    assertU(adoc("id", "4", "bbox", "[6,0 TO 10,10]")); // No match - X dimension starts too high
    assertU(commit());

    assertQ(
        req("q", "bbox:5,5"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");
  }

  @Test
  public void testGetFieldQueryFieldFormatting() {
    // Test 1D field formatting
    assertU(adoc("id", "1", "long_range", "[100 TO 200]"));
    // Test 2D field formatting
    assertU(adoc("id", "2", "bbox", "[10,20 TO 30,40]"));
    // Test 3D field formatting
    assertU(adoc("id", "3", "cube", "[5,10,15 TO 25,30,35]"));
    // Test 4D field formatting
    assertU(adoc("id", "4", "tesseract", "[1,2,3,4 TO 11,12,13,14]"));
    // Test multi-valued field formatting
    assertU(
        adoc(
            "id",
            "5",
            "long_range_multi",
            "[50 TO 100]",
            "long_range_multi",
            "[200 TO 300]",
            "long_range_multi",
            "[400 TO 500]"));
    assertU(commit());

    // Verify 1D field returns correctly formatted value
    assertQ(
        req("q", "id:1"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='long_range'][.='[100 TO 200]']");

    // Verify 2D field returns correctly formatted value
    assertQ(
        req("q", "id:2"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='bbox'][.='[10,20 TO 30,40]']");

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
        "//result/doc/arr[@name='long_range_multi']/str[1][.='[50 TO 100]']",
        "//result/doc/arr[@name='long_range_multi']/str[2][.='[200 TO 300]']",
        "//result/doc/arr[@name='long_range_multi']/str[3][.='[400 TO 500]']");
  }
}
