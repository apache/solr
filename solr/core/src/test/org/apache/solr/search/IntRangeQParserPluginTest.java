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
package org.apache.solr.search;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests for {@link IntRangeQParserPlugin} */
public class IntRangeQParserPluginTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema-intrange.xml");
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }

  @Test
  public void test1DIntersectsQuery() {
    // Index documents with 1D ranges
    assertU(adoc("id", "1", "price_range", "[100 TO 200]"));
    assertU(adoc("id", "2", "price_range", "[150 TO 250]"));
    assertU(adoc("id", "3", "price_range", "[50 TO 80]"));
    assertU(adoc("id", "4", "price_range", "[200 TO 300]"));
    assertU(commit());

    // Query: find ranges intersecting [120 TO 180]
    assertQ(
        req("q", "{!myRange field=price_range}[120 TO 180]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");

    // Query: find ranges intersecting [0 TO 100]
    assertQ(
        req("q", "{!myRange field=price_range}[0 TO 100]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='3']");

    // Query: find ranges intersecting [175 TO 225]
    assertQ(
        req("q", "{!myRange field=price_range}[175 TO 225]"),
        "//result[@numFound='3']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']",
        "//result/doc/str[@name='id'][.='4']");
  }

  @Test
  public void test1DWithinQuery() {
    assertU(adoc("id", "1", "price_range", "[100 TO 200]"));
    assertU(adoc("id", "2", "price_range", "[150 TO 250]"));
    assertU(adoc("id", "3", "price_range", "[50 TO 80]"));
    assertU(commit());

    // Query: find ranges within [0 TO 300]
    assertQ(
        req("q", "{!myRange criteria=\"within\" field=price_range}[0 TO 300]"),
        "//result[@numFound='3']");

    // Query: find ranges within [100 TO 200]
    assertQ(
        req("q", "{!myRange criteria=\"within\" field=price_range}[100 TO 200]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");

    // Query: find ranges within [0 TO 100]
    assertQ(
        req("q", "{!myRange criteria=\"within\" field=price_range}[0 TO 100]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='3']");
  }

  @Test
  public void test1DContainsQuery() {
    assertU(adoc("id", "1", "price_range", "[100 TO 200]"));
    assertU(adoc("id", "2", "price_range", "[150 TO 250]"));
    assertU(adoc("id", "3", "price_range", "[50 TO 300]"));
    assertU(commit());

    // Query: find ranges containing [160 TO 170]
    assertQ(
        req("q", "{!myRange criteria=\"contains\" field=price_range}[160 TO 170]"),
        "//result[@numFound='3']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']",
        "//result/doc/str[@name='id'][.='3']");

    // Query: find ranges containing [0 TO 400]
    assertQ(
        req("q", "{!myRange criteria=\"contains\" field=price_range}[0 TO 400]"),
        "//result[@numFound='0']");

    // Query: find ranges containing [100 TO 200]
    assertQ(
        req("q", "{!myRange criteria=\"contains\" field=price_range}[100 TO 200]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='3']");
  }

  @Test
  public void test1DCrossesQuery() {
    assertU(adoc("id", "1", "price_range", "[100 TO 200]"));
    assertU(adoc("id", "2", "price_range", "[150 TO 250]"));
    assertU(adoc("id", "3", "price_range", "[50 TO 80]"));
    assertU(adoc("id", "4", "price_range", "[120 TO 180]"));
    assertU(commit());

    // Query: find ranges crossing [150 TO 250]
    // Should match ranges that intersect but are not within
    assertQ(
        req("q", "{!myRange criteria=\"crosses\" field=price_range}[150 TO 250]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='4']");
  }

  @Test
  public void test2DIntersectsQuery() {
    // Index documents with 2D ranges (bounding boxes)
    assertU(adoc("id", "1", "bbox", "[0,0 TO 10,10]"));
    assertU(adoc("id", "2", "bbox", "[5,5 TO 15,15]"));
    assertU(adoc("id", "3", "bbox", "[20,20 TO 30,30]"));
    assertU(commit());

    // Query: find bboxes intersecting [8,8 TO 12,12]
    assertQ(
        req("q", "{!myRange field=bbox}[8,8 TO 12,12]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");

    // Query: find bboxes intersecting [25,25 TO 35,35]
    assertQ(
        req("q", "{!myRange field=bbox}[25,25 TO 35,35]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='3']");

    // Query: find bboxes intersecting [100,100 TO 200,200]
    assertQ(req("q", "{!myRange field=bbox}[100,100 TO 200,200]"), "//result[@numFound='0']");
  }

  @Test
  public void test2DWithinQuery() {
    assertU(adoc("id", "1", "bbox", "[5,5 TO 10,10]"));
    assertU(adoc("id", "2", "bbox", "[0,0 TO 20,20]"));
    assertU(adoc("id", "3", "bbox", "[15,15 TO 25,25]"));
    assertU(commit());

    // Query: find bboxes within [0,0 TO 20,20]
    assertQ(
        req("q", "{!myRange criteria=\"within\" field=bbox}[0,0 TO 20,20]"),
        "//result[@numFound='2']",
        "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='id'][.='2']");

    // Query: find bboxes within [0,0 TO 30,30]
    assertQ(
        req("q", "{!myRange criteria=\"within\" field=bbox}[0,0 TO 30,30]"),
        "//result[@numFound='3']");
  }

  @Test
  public void test3DQuery() {
    // Index documents with 3D ranges (bounding cubes)
    assertU(adoc("id", "1", "cube", "[0,0,0 TO 10,10,10]"));
    assertU(adoc("id", "2", "cube", "[5,5,5 TO 15,15,15]"));
    assertU(commit());

    // Query: find cubes intersecting [8,8,8 TO 12,12,12]
    assertQ(
        req("q", "{!myRange field=cube}[8,8,8 TO 12,12,12]"),
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
        req("q", "{!myRange field=tesseract}[8,8,8,8 TO 12,12,12,12]"),
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
        req("q", "{!myRange field=price_range_multi}[110 TO 120]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");

    // Query should match doc 1 via second range
    assertQ(
        req("q", "{!myRange field=price_range_multi}[310 TO 320]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");

    // Query should match both docs
    assertQ(req("q", "{!myRange field=price_range_multi}[150 TO 250]"), "//result[@numFound='2']");
  }

  @Test
  public void testMissingFieldParameter() {
    assertU(adoc("id", "1", "price_range", "[100 TO 200]"));
    assertU(commit());

    // Query without field parameter should fail
    assertQEx(
        "Missing field parameter should fail",
        "Missing required parameter: field",
        req("q", "{!myRange}[100 TO 200]"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testInvalidFieldType() {
    assertU(adoc("id", "1", "title", "test"));
    assertU(commit());

    // Query on non-IntRangeField should fail
    assertQEx(
        "Query on wrong field type should fail",
        "must be of type IntRangeField",
        req("q", "{!myRange field=title}[100 TO 200]"),
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
        req("q", "{!myRange criteria=\"invalid\" field=price_range}[100 TO 200]"),
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
        req("q", "{!myRange field=price_range}invalid"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testEmptyRangeValue() {
    assertU(adoc("id", "1", "price_range", "[100 TO 200]"));
    assertU(commit());

    // Query with empty range should fail
    assertQEx(
        "Empty range value should fail",
        req("q", "{!myRange field=price_range}"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testDefaultQueryTypeIsIntersects() {
    assertU(adoc("id", "1", "price_range", "[100 TO 200]"));
    assertU(adoc("id", "2", "price_range", "[250 TO 300]"));
    assertU(commit());

    // Query without type parameter should default to intersects
    assertQ(
        req("q", "{!myRange field=price_range}[150 TO 180]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");
  }

  @Test
  public void testNegativeValues() {
    assertU(adoc("id", "1", "price_range", "[-100 TO -50]"));
    assertU(adoc("id", "2", "price_range", "[-75 TO -25]"));
    assertU(commit());

    // Query with negative range
    assertQ(req("q", "{!myRange field=price_range}[-80 TO -60]"), "//result[@numFound='2']");
  }

  @Test
  public void testExtremeValues() {
    int min = Integer.MIN_VALUE;
    int max = Integer.MAX_VALUE;

    assertU(adoc("id", "1", "price_range", "[" + min + " TO " + max + "]"));
    assertU(commit());

    // Query should match the extreme range
    assertQ(
        req("q", "{!myRange field=price_range}[0 TO 100]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");
  }

  @Test
  public void testPointRange() {
    // Point range where min == max
    assertU(adoc("id", "1", "price_range", "[100 TO 100]"));
    assertU(commit());

    // Intersects query with point
    assertQ(
        req("q", "{!myRange field=price_range}[100 TO 100]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");

    // Intersects query containing point
    assertQ(
        req("q", "{!myRange field=price_range}[50 TO 150]"),
        "//result[@numFound='1']",
        "//result/doc/str[@name='id'][.='1']");
  }
}
