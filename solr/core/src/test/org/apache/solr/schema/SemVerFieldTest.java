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
package org.apache.solr.schema;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;
import org.junit.Test;

public class SemVerFieldTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml", "schema-semver.xml");
  }

  @Test
  public void testParseSemVer() {
    assertEquals(0L, SemVerField.parseSemVer("0"));
    assertEquals(1_000_000_000_000_000L, SemVerField.parseSemVer("1"));
    assertEquals(2_003_001_000_000_000L, SemVerField.parseSemVer("2.3.1"));
    assertEquals(1_002_003_004_005_006L, SemVerField.parseSemVer("1.2.3.4.5.6"));
    assertEquals(999_999_999_999_999_999L, SemVerField.parseSemVer("999.999.999.999.999.999"));
    assertEquals(SemVerField.parseSemVer("2.3"), SemVerField.parseSemVer("2.3.0"));
    assertEquals(SemVerField.parseSemVer("2.3"), SemVerField.parseSemVer("2.3.0.0.0.0"));
  }

  @Test
  public void testLongToSemVer() {
    assertEquals("0", SemVerField.longToSemVer(0L));
    assertEquals("1", SemVerField.longToSemVer(1_000_000_000_000_000L));
    assertEquals("2.3.1", SemVerField.longToSemVer(2_003_001_000_000_000L));
    assertEquals("1.2.3.4.5.6", SemVerField.longToSemVer(1_002_003_004_005_006L));
    assertEquals("0.0.0.0.0.1", SemVerField.longToSemVer(1L));
  }

  @Test
  public void testRoundTrip() {
    String[] versions = {"0", "1", "1.2.3", "999.999.999.999.999.999", "0.0.0.0.0.1"};
    for (String v : versions) {
      long parsed = SemVerField.parseSemVer(v);
      String formatted = SemVerField.longToSemVer(parsed);
      assertEquals("Round trip for " + v, parsed, SemVerField.parseSemVer(formatted));
    }
  }

  @Test
  public void testParseSemVerErrors() {
    assertThrows(SolrException.class, () -> SemVerField.parseSemVer("1.2.3.4.5.6.7"));
    assertThrows(SolrException.class, () -> SemVerField.parseSemVer(""));
    assertThrows(SolrException.class, () -> SemVerField.parseSemVer("abc"));
    assertThrows(SolrException.class, () -> SemVerField.parseSemVer("1.2.abc"));
    assertThrows(SolrException.class, () -> SemVerField.parseSemVer("1000.0.0"));
    assertThrows(SolrException.class, () -> SemVerField.parseSemVer("-1.0.0"));
    assertThrows(SolrException.class, () -> SemVerField.parseSemVer("1.2."));
  }

  @Test
  public void testIndexAndExactQuery() {
    clearIndex();
    assertU(adoc("id", "1", "version", "1.2.3"));
    assertU(adoc("id", "2", "version", "2.0.0"));
    assertU(adoc("id", "3", "version", "1.2.3"));
    assertU(commit());

    assertQ("Exact match should find 2 docs", req("q", "version:1.2.3"), "//*[@numFound='2']");

    assertQ(
        "Exact match single doc",
        req("q", "version:2.0.0"),
        "//*[@numFound='1']",
        "//result/doc[1]/str[@name='id'][.='2']");

    assertQ("Trailing zeros are equivalent", req("q", "version:2.0"), "//*[@numFound='1']");
  }

  @Test
  public void testRangeQuery() {
    clearIndex();
    assertU(adoc("id", "1", "version", "1.0.0"));
    assertU(adoc("id", "2", "version", "1.5.0"));
    assertU(adoc("id", "3", "version", "2.0.0"));
    assertU(adoc("id", "4", "version", "3.0.0"));
    assertU(commit());

    assertQ("Inclusive range", req("q", "version:[1.0.0 TO 2.0.0]"), "//*[@numFound='3']");

    assertQ("Exclusive upper bound", req("q", "version:[1.0.0 TO 2.0.0}"), "//*[@numFound='2']");

    assertQ("Open-ended upper", req("q", "version:[2.0.0 TO *]"), "//*[@numFound='2']");

    assertQ("Open-ended lower", req("q", "version:[* TO 1.5.0]"), "//*[@numFound='2']");
  }

  @Test
  public void testSorting() {
    clearIndex();
    assertU(adoc("id", "1", "version", "3.1.0"));
    assertU(adoc("id", "2", "version", "1.0.0"));
    assertU(adoc("id", "3", "version", "2.5.0"));
    assertU(commit());

    assertQ(
        "Sort ascending",
        req("fl", "id", "q", "*:*", "sort", "version asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='3']",
        "//result/doc[3]/str[@name='id'][.='1']");

    assertQ(
        "Sort descending",
        req("fl", "id", "q", "*:*", "sort", "version desc"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='3']",
        "//result/doc[3]/str[@name='id'][.='2']");
  }

  @Test
  public void testStoredValue() {
    clearIndex();
    assertU(adoc("id", "1", "version", "2.3.1"));
    assertU(commit());

    assertQ(
        "Stored value should be semver string",
        req("q", "id:1", "fl", "version"),
        "//result/doc[1]/str[@name='version'][.='2.3.1']");
  }

  @Test
  public void testSortMissingLast() {
    clearIndex();
    assertU(adoc("id", "1"));
    assertU(adoc("id", "2", "version_last", "2.0.0"));
    assertU(adoc("id", "3", "version_last", "1.0.0"));
    assertU(commit());

    assertQ(
        "Sort asc, missing last",
        req("fl", "id", "q", "*:*", "sort", "version_last asc"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='3']",
        "//result/doc[2]/str[@name='id'][.='2']",
        "//result/doc[3]/str[@name='id'][.='1']");

    assertQ(
        "Sort desc, missing last",
        req("fl", "id", "q", "*:*", "sort", "version_last desc"),
        "//*[@numFound='3']",
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='3']",
        "//result/doc[3]/str[@name='id'][.='1']");
  }
}
