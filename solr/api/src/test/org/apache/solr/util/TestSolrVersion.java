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

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.api.util.SolrVersion;
import org.semver4j.SemverException;

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
    assertTrue(
        SolrVersion.valueOf("9.10.20").greaterThanOrEqualTo(SolrVersion.forIntegers(9, 0, 0)));
  }

  public void testParseExceptions() {
    expectThrows(SemverException.class, () -> SolrVersion.valueOf("SOLR_7_0_0"));
  }

  public void testSatisfies() {
    assertTrue(SOLR_9_0_1.satisfies("~9.0"));
    assertTrue(SOLR_9_0_1.satisfies("9.x"));
    assertTrue(SOLR_9_0_1.satisfies("9"));
    assertTrue(SOLR_9_0_1.satisfies("<=9"));
    assertTrue(SOLR_9_0_1.satisfies("<9.1"));
    assertFalse(SOLR_9_0_1.satisfies(">9.0"));
    assertFalse(SOLR_9_0_1.satisfies("8.x"));
    assertTrue(SOLR_9_0_1.satisfies("8 - 9"));
  }

  public void testPrereleaseIgnoredBySatisfies() {
    SolrVersion snapshot = SolrVersion.valueOf("10.1.0-SNAPSHOT");
    assertTrue(snapshot.satisfies(">=9.0.0"));
    assertTrue(snapshot.satisfies("10.x"));
    assertTrue(snapshot.satisfies("~10.1"));
    assertFalse(snapshot.satisfies("<10.1"));
  }

  public void testPrereleaseRangeBoundaries() {
    SolrVersion release = SolrVersion.valueOf("10.1.0");
    for (String versionString : new String[] {"10.1.0-SNAPSHOT", "10.1.0-rc.1+b123"}) {
      SolrVersion version = SolrVersion.valueOf(versionString);
      assertTrue(versionString, version.satisfies(">=10.1.0"));
      assertTrue(versionString, version.satisfies("<=10.1.0"));
      assertTrue(versionString, version.satisfies("10.1.0"));
      assertFalse(versionString, version.satisfies("<10.1.0"));
      assertFalse(versionString, version.satisfies(">10.1.0"));
      assertTrue(versionString, version.compareTo(release) < 0);
      assertNotEquals(release, version);
    }
  }

  public void testWithoutPrerelease() {
    SolrVersion snapshot = SolrVersion.valueOf("10.1.0-SNAPSHOT");
    assertTrue(snapshot.lessThan(SolrVersion.valueOf("10.1.0")));
    assertEquals(SolrVersion.valueOf("10.1.0"), snapshot.withoutPrerelease());
  }

  public void testWithoutPrereleasePreservesBuildMetadata() {
    SolrVersion prerelease = SolrVersion.valueOf("10.1.0-rc.1+build.123");
    SolrVersion normalized = prerelease.withoutPrerelease();
    assertEquals("10.1.0+build.123", normalized.toString());
    assertEquals("10.1.0-rc.1+build.123", prerelease.toString());
    assertTrue(prerelease.lessThan(normalized));
  }

  public void testWithoutPrereleaseOnRelease() {
    for (String versionString : new String[] {"10.1.2", "10.1.2+build.123"}) {
      SolrVersion release = SolrVersion.valueOf(versionString);
      SolrVersion normalized = release.withoutPrerelease();
      assertEquals(versionString, normalized.toString());
      assertEquals(versionString, normalized.withoutPrerelease().toString());
    }
  }

  public void testWithoutPrereleaseOrdering() {
    SolrVersion normalized = SolrVersion.valueOf("10.1.0-SNAPSHOT").withoutPrerelease();
    for (String versionString : new String[] {"10.1.0", "10.1.0-alpha.1", "10.1.0-rc.1+b123"}) {
      SolrVersion sameRelease = SolrVersion.valueOf(versionString).withoutPrerelease();
      assertFalse(versionString, normalized.lessThan(sameRelease));
      assertFalse(versionString, sameRelease.lessThan(normalized));
    }
    for (String versionString :
        new String[] {"10.1.1-SNAPSHOT", "10.2.0-SNAPSHOT", "11.0.0-SNAPSHOT"}) {
      SolrVersion higherRelease = SolrVersion.valueOf(versionString).withoutPrerelease();
      assertTrue(versionString, normalized.lessThan(higherRelease));
      assertFalse(versionString, higherRelease.lessThan(normalized));
    }
  }
}
