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
package org.apache.solr.util;

import com.github.zafarkhaja.semver.ParseException;
import org.apache.solr.SolrTestCase;

public class TestSolrVersion extends SolrTestCase {
  private static final SolrVersion SOLR_9_0_1 = SolrVersion.valueOf("9.0.1");

  public void testToString() {
    assertEquals("9.0.0", SolrVersion.forIntegers(9, 0, 0).toString());
  }

  public void testVersionComponents() {
    SolrVersion v9_0_1_rc1 = SolrVersion.valueOf("9.0.1-rc1.0+b123");
    assertEquals("9.0.1-rc1.0+b123", v9_0_1_rc1.toString());
    assertEquals(9, v9_0_1_rc1.getMajorVersion());
    assertEquals(0, v9_0_1_rc1.getMinorVersion());
    assertEquals(1, v9_0_1_rc1.getPatchVersion());
    assertEquals("rc1.0", v9_0_1_rc1.getPrereleaseVersion());
  }

  public void testLatestInitialized() {
    assertTrue(SolrVersion.LATEST.greaterThanOrEqualTo(SolrVersion.valueOf("9.0.0")));
  }

  public void testForwardsCompatibility() {
    assertTrue(SolrVersion.valueOf("9.10.20").greaterThanOrEqualTo(SolrVersion.forIntegers(9, 0, 0)));
  }

  public void testParseExceptions() {
    expectThrows(
        ParseException.class,
        () -> SolrVersion.valueOf("SOLR_7_0_0"));
  }

  public void testSatisfies() {
    assertTrue(SOLR_9_0_1.satisfies("~9.0"));
    assertTrue(SOLR_9_0_1.satisfies("9.x"));
    assertTrue(SOLR_9_0_1.satisfies("9"));
    assertTrue(SOLR_9_0_1.satisfies("<9.1"));
    assertTrue(SOLR_9_0_1.satisfies(">9.0"));
    assertFalse(SOLR_9_0_1.satisfies("8.x"));
  }

  public void testSatisfiesParseFailure() {
    assertThrows(SolrVersion.InvalidSemVerExpressionException.class, () ->
        SOLR_9_0_1.satisfies(":"));
  }
}
