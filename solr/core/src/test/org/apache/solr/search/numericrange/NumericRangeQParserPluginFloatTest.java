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
 * org.apache.solr.schema.numericrange.FloatRangeField} fields.
 */
public class NumericRangeQParserPluginFloatTest extends SolrTestCaseJ4 {

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
    assertU(adoc("id", "1", "float_range", "[1.0 TO 2.0]"));
    assertU(adoc("id", "2", "float_range", "[1.5 TO 2.5]"));
    assertU(adoc("id", "3", "float_range", "[0.5 TO 0.8]"));
    assertU(adoc("id", "4", "float_range", "[2.0 TO 3.0]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=intersects field=float_range}[1.2 TO 1.8]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']",
        "//result/doc/str[@name='float_range'][.='[1.0 TO 2.0]']",
        "//result/doc/str[@name='float_range'][.='[1.5 TO 2.5]']");

    assertQ(
        req("q", "{!numericRange criteria=intersects field=float_range}[0.0 TO 1.0]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='3']");

    assertQ(
        req("q", "{!numericRange criteria=intersects field=float_range}[1.75 TO 2.25]"),
        "//result[@numFound='3']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']",
        "//result/doc/str[@name='id'][.='4']");
  }

  @Test
  public void test1DWithinQuery() {
    assertU(adoc("id", "1", "float_range", "[1.0 TO 2.0]"));
    assertU(adoc("id", "2", "float_range", "[1.5 TO 2.5]"));
    assertU(adoc("id", "3", "float_range", "[0.5 TO 0.8]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=\"within\" field=float_range}[0.0 TO 3.0]"),
        "//result[@numFound='3']");

    assertQ(
        req("q", "{!numericRange criteria=\"within\" field=float_range}[1.0 TO 2.0]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");

    assertQ(
        req("q", "{!numericRange criteria=\"within\" field=float_range}[0.0 TO 1.0]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='3']");
  }

  @Test
  public void test1DContainsQuery() {
    assertU(adoc("id", "1", "float_range", "[1.0 TO 2.0]"));
    assertU(adoc("id", "2", "float_range", "[1.5 TO 2.5]"));
    assertU(adoc("id", "3", "float_range", "[0.5 TO 3.0]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=\"contains\" field=float_range}[1.6 TO 1.7]"),
        "//result[@numFound='3']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']",
        "//result/doc/str[@name='id'][.='3']");

    assertQ(
        req("q", "{!numericRange criteria=\"contains\" field=float_range}[0.0 TO 4.0]"),
        "//result[@numFound='0']");

    assertQ(
        req("q", "{!numericRange criteria=\"contains\" field=float_range}[1.0 TO 2.0]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='3']");
  }

  @Test
  public void test1DCrossesQuery() {
    assertU(adoc("id", "1", "float_range", "[1.0 TO 2.0]"));
    assertU(adoc("id", "2", "float_range", "[1.5 TO 2.5]"));
    assertU(adoc("id", "3", "float_range", "[0.5 TO 0.8]"));
    assertU(adoc("id", "4", "float_range", "[1.2 TO 1.8]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=\"crosses\" field=float_range}[1.5 TO 2.5]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='4']");
  }

  @Test
  public void test2DIntersectsQuery() {
    assertU(adoc("id", "1", "float_range_2d", "[0.0,0.0 TO 1.0,1.0]"));
    assertU(adoc("id", "2", "float_range_2d", "[0.5,0.5 TO 1.5,1.5]"));
    assertU(adoc("id", "3", "float_range_2d", "[2.0,2.0 TO 3.0,3.0]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=intersects field=float_range_2d}[0.8,0.8 TO 1.2,1.2]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");

    assertQ(
        req("q", "{!numericRange criteria=intersects field=float_range_2d}[2.5,2.5 TO 3.5,3.5]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='3']");

    assertQ(
        req(
            "q",
            "{!numericRange criteria=intersects field=float_range_2d}[10.0,10.0 TO 20.0,20.0]"),
        "//result[@numFound='0']");
  }

  @Test
  public void test3DQuery() {
    assertU(adoc("id", "1", "float_range_3d", "[0.0,0.0,0.0 TO 1.0,1.0,1.0]"));
    assertU(adoc("id", "2", "float_range_3d", "[0.5,0.5,0.5 TO 1.5,1.5,1.5]"));
    assertU(commit());

    assertQ(
        req(
            "q",
            "{!numericRange criteria=intersects field=float_range_3d}[0.8,0.8,0.8 TO 1.2,1.2,1.2]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");
  }

  @Test
  public void test4DQuery() {
    assertU(adoc("id", "1", "float_range_4d", "[0.0,0.0,0.0,0.0 TO 1.0,1.0,1.0,1.0]"));
    assertU(adoc("id", "2", "float_range_4d", "[0.5,0.5,0.5,0.5 TO 1.5,1.5,1.5,1.5]"));
    assertU(commit());

    assertQ(
        req(
            "q",
            "{!numericRange criteria=intersects field=float_range_4d}[0.8,0.8,0.8,0.8 TO 1.2,1.2,1.2,1.2]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");
  }

  @Test
  public void testMultiValuedField() {
    assertU(
        adoc("id", "1", "float_range_multi", "[1.0 TO 2.0]", "float_range_multi", "[3.0 TO 4.0]"));
    assertU(adoc("id", "2", "float_range_multi", "[1.5 TO 2.5]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=intersects field=float_range_multi}[1.1 TO 1.2]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/arr[@name='float_range_multi']/str[1][.='[1.0 TO 2.0]']",
        "//result/doc/arr[@name='float_range_multi']/str[2][.='[3.0 TO 4.0]']");

    assertQ(
        req("q", "{!numericRange criteria=intersects field=float_range_multi}[3.1 TO 3.2]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");

    assertQ(
        req("q", "{!numericRange criteria=intersects field=float_range_multi}[1.5 TO 2.5]"),
        "//result[@numFound='2']");
  }

  @Test
  public void testNegativeValues() {
    assertU(adoc("id", "1", "float_range", "[-1.0 TO -0.5]"));
    assertU(adoc("id", "2", "float_range", "[-0.75 TO -0.25]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=intersects field=float_range}[-0.8 TO -0.6]"),
        "//result[@numFound='2']");
  }

  @Test
  public void testPointRange() {
    assertU(adoc("id", "1", "float_range", "[1.5 TO 1.5]"));
    assertU(commit());

    assertQ(
        req("q", "{!numericRange criteria=intersects field=float_range}[1.5 TO 1.5]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");

    assertQ(
        req("q", "{!numericRange criteria=intersects field=float_range}[0.5 TO 1.75]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");
  }

  @Test
  public void testMissingFieldParameter() {
    assertQEx(
        "Missing field parameter should fail",
        "Missing required parameter: field",
        req("q", "{!numericRange criteria=intersects}[1.0 TO 2.0]"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testMissingCriteriaParameter() {
    assertQEx(
        "Missing criteria parameter should fail",
        "Missing required parameter: criteria",
        req("q", "{!numericRange field=float_range}[1.0 TO 2.0]"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testInvalidFieldType() {
    // Query on a plain string field should fail
    assertQEx(
        "Query on wrong field type should fail",
        "must be a numeric range field type",
        req("q", "{!numericRange criteria=intersects field=title}[1.0 TO 2.0]"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testInvalidQueryType() {
    assertU(adoc("id", "1", "float_range", "[1.0 TO 2.0]"));
    assertU(commit());

    assertQEx(
        "Invalid query criteria should fail",
        "Unknown query criteria",
        req("q", "{!numericRange criteria=\"invalid\" field=float_range}[1.0 TO 2.0]"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testInvalidRangeValue() {
    assertU(adoc("id", "1", "float_range", "[1.0 TO 2.0]"));
    assertU(commit());

    assertQEx(
        "Invalid range format should fail",
        "Invalid range",
        req("q", "{!numericRange criteria=intersects field=float_range}invalid"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testEmptyRangeValue() {
    assertU(adoc("id", "1", "float_range", "[1.0 TO 2.0]"));
    assertU(commit());

    assertQEx(
        "Empty range value should fail",
        req("q", "{!numericRange criteria=intersects field=float_range}"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  // ------------------------------------------
  // Tests for getFieldQuery and getSpecializedRangeQuery via the standard query parser.
  // These default to "contains" semantics.

  @Test
  public void testGetFieldQueryFullRange() {
    // doc 1: narrow range, fully inside the query range  → should NOT match (doc contains query)
    // doc 2: wide range that fully contains the query range → should match
    // doc 3: range that only partially overlaps            → should NOT match
    assertU(adoc("id", "1", "float_range", "[1.3 TO 1.6]")); // No match
    assertU(adoc("id", "2", "float_range", "[1.0 TO 2.0]")); // Match!
    assertU(adoc("id", "3", "float_range", "[1.5 TO 2.5]")); // No match
    assertU(commit());

    // Contains semantics: find indexed ranges that fully contain [1.2 TO 1.8]
    assertQ(
        req("q", "float_range:[1.2 TO 1.8]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='2']");
  }

  @Test
  public void testGetFieldQueryFullRangeMultipleMatches() {
    assertU(adoc("id", "1", "float_range", "[0.0 TO 10.0]")); // Match!
    assertU(adoc("id", "2", "float_range", "[1.0 TO 2.0]")); // Match!
    assertU(adoc("id", "3", "float_range", "[1.0 TO 1.99]")); // No match - max too low
    assertU(adoc("id", "4", "float_range", "[1.01 TO 2.0]")); // No match - min too high
    assertU(commit());

    assertQ(
        req("q", "float_range:[1.0 TO 2.0]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");
  }

  @Test
  public void testGetFieldQuerySingleBound() {
    // Single-bound syntax: float_range:1.5 is sugar for contains([1.5 TO 1.5])
    assertU(adoc("id", "1", "float_range", "[1.0 TO 2.0]")); // Match!
    assertU(adoc("id", "2", "float_range", "[1.5 TO 1.5]")); // Match!
    assertU(adoc("id", "3", "float_range", "[1.0 TO 1.49]")); // No match - max below 1.5
    assertU(adoc("id", "4", "float_range", "[1.51 TO 3.0]")); // No match - min above 1.5
    assertU(commit());

    assertQ(
        req("q", "float_range:1.5"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");
  }

  @Test
  public void testGetFieldQuerySingleBound2D() {
    // 2D single-bound: float_range_2d:0.5,0.5 is sugar for contains([0.5,0.5 TO 0.5,0.5])
    assertU(adoc("id", "1", "float_range_2d", "[0.0,0.0 TO 1.0,1.0]")); // Match!
    assertU(adoc("id", "2", "float_range_2d", "[0.5,0.5 TO 0.5,0.5]")); // Match!
    assertU(adoc("id", "3", "float_range_2d", "[0.0,0.0 TO 0.4,1.0]")); // No match - X too low
    assertU(adoc("id", "4", "float_range_2d", "[0.6,0.0 TO 1.0,1.0]")); // No match - X too high
    assertU(commit());

    assertQ(
        req("q", "float_range_2d:0.5,0.5"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");
  }

  @Test
  public void testGetFieldQueryFieldFormatting() {
    assertU(adoc("id", "1", "float_range", "[1.0 TO 2.0]"));
    assertU(adoc("id", "2", "float_range_2d", "[1.0,2.0 TO 3.0,4.0]"));
    assertU(adoc("id", "3", "float_range_3d", "[0.5,1.0,1.5 TO 2.5,3.0,3.5]"));
    assertU(adoc("id", "4", "float_range_4d", "[0.1,0.2,0.3,0.4 TO 1.1,1.2,1.3,1.4]"));
    assertU(
        adoc(
            "id",
            "5",
            "float_range_multi",
            "[0.5 TO 1.0]",
            "float_range_multi",
            "[2.0 TO 3.0]",
            "float_range_multi",
            "[4.0 TO 5.0]"));
    assertU(commit());

    // Verify 1D field returns correctly formatted value
    assertQ(
        req("q", "id:1"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='float_range'][.='[1.0 TO 2.0]']");

    // Verify 2D field returns correctly formatted value
    assertQ(
        req("q", "id:2"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='float_range_2d'][.='[1.0,2.0 TO 3.0,4.0]']");

    // Verify 3D field returns correctly formatted value
    assertQ(
        req("q", "id:3"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='float_range_3d'][.='[0.5,1.0,1.5 TO 2.5,3.0,3.5]']");

    // Verify 4D field returns correctly formatted value
    assertQ(
        req("q", "id:4"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='float_range_4d'][.='[0.1,0.2,0.3,0.4 TO 1.1,1.2,1.3,1.4]']");

    // Verify multi-valued field returns all values correctly formatted
    assertQ(
        req("q", "id:5"),
        "//result[@numFound='1']",
        "//result/doc/arr[@name='float_range_multi']/str[1][.='[0.5 TO 1.0]']",
        "//result/doc/arr[@name='float_range_multi']/str[2][.='[2.0 TO 3.0]']",
        "//result/doc/arr[@name='float_range_multi']/str[3][.='[4.0 TO 5.0]']");
  }
}
