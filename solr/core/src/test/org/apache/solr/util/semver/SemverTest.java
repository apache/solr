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

// Forked and adapted from https://github.com/vdurmont/semver4j - MIT license
// Copyright (c) 2015-present Vincent DURMONT vdurmont@gmail.com

package org.apache.solr.util.semver;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(JUnit4.class)
public class SemverTest {
  @Test(expected = SemverException.class)
  public void constructor_with_empty_build_fails() {
    new Semver("1.0.0+");
  }

  @Test public void default_constructor_test_full_version() {
    String version = "1.2.3-beta.11+sha.0nsfgkjkjsdf";
    Semver semver = new Semver(version);
    assertIsSemver(semver, version, 1, 2, 3, new String[]{"beta", "11"}, "sha.0nsfgkjkjsdf");
  }

  @Test(expected = SemverException.class)
  public void default_constructor_test_only_major_and_minor() {
    String version = "1.2-beta.11+sha.0nsfgkjkjsdf";
    new Semver(version);
  }

  @Test(expected = SemverException.class)
  public void default_constructor_test_only_major() {
    String version = "1-beta.11+sha.0nsfgkjkjsdf";
    new Semver(version);
  }

  @Test public void npm_constructor_test_full_version() {
    String version = "1.2.3-beta.11+sha.0nsfgkjkjsdf";
    Semver semver = new Semver(version, Semver.SemverType.NPM);
    assertIsSemver(semver, version, 1, 2, 3, new String[]{"beta", "11"}, "sha.0nsfgkjkjsdf");
  }

  @Test public void npm_constructor_test_only_major_and_minor() {
    String version = "1.2-beta.11+sha.0nsfgkjkjsdf";
    Semver semver = new Semver(version, Semver.SemverType.NPM);
    assertIsSemver(semver, version, 1, 2, null, new String[]{"beta", "11"}, "sha.0nsfgkjkjsdf");
  }

  @Test public void npm_constructor_test_only_major() {
    String version = "1-beta.11+sha.0nsfgkjkjsdf";
    Semver semver = new Semver(version, Semver.SemverType.NPM);
    assertIsSemver(semver, version, 1, null, null, new String[]{"beta", "11"}, "sha.0nsfgkjkjsdf");
  }

  @Test public void npm_constructor_with_leading_v() {
    String version = "v1.2.3-beta.11+sha.0nsfgkjkjsdf";
    Semver semver = new Semver(version, Semver.SemverType.NPM);
    assertIsSemver(semver, "1.2.3-beta.11+sha.0nsfgkjkjsdf", 1, 2, 3, new String[]{"beta", "11"}, "sha.0nsfgkjkjsdf");

    String versionWithSpace = "v 1.2.3-beta.11+sha.0nsfgkjkjsdf";
    Semver semverWithSpace = new Semver(versionWithSpace, Semver.SemverType.NPM);
    assertIsSemver(semverWithSpace, "1.2.3-beta.11+sha.0nsfgkjkjsdf", 1, 2, 3, new String[]{"beta", "11"}, "sha.0nsfgkjkjsdf");
  }

  @Test public void cocoapods_constructor_test_full_version() {
    String version = "1.2.3-beta.11+sha.0nsfgkjkjsdf";
    Semver semver = new Semver(version, Semver.SemverType.COCOAPODS);
    assertIsSemver(semver, version, 1, 2, 3, new String[]{"beta", "11"}, "sha.0nsfgkjkjsdf");
  }

  @Test public void cocoapods_constructor_test_only_major_and_minor() {
    String version = "1.2-beta.11+sha.0nsfgkjkjsdf";
    Semver semver = new Semver(version, Semver.SemverType.COCOAPODS);
    assertIsSemver(semver, version, 1, 2, null, new String[]{"beta", "11"}, "sha.0nsfgkjkjsdf");
  }

  @Test public void cocoapods_constructor_test_only_major() {
    String version = "1-beta.11+sha.0nsfgkjkjsdf";
    Semver semver = new Semver(version, Semver.SemverType.COCOAPODS);
    assertIsSemver(semver, version, 1, null, null, new String[]{"beta", "11"}, "sha.0nsfgkjkjsdf");
  }

  @Test
  public void loose_constructor_test_only_major_and_minor() {
    String version = "1.2-beta.11+sha.0nsfgkjkjsdf";
    Semver semver = new Semver(version, Semver.SemverType.LOOSE);
    assertIsSemver(semver, version, 1, 2, null, new String[]{"beta", "11"}, "sha.0nsfgkjkjsdf");
  }

  @Test
  public void loose_constructor_test_only_major() {
    String version = "1-beta.11+sha.0nsfgkjkjsdf";
    Semver semver = new Semver(version, Semver.SemverType.LOOSE);
    assertIsSemver(semver, version, 1, null, null, new String[]{"beta", "11"}, "sha.0nsfgkjkjsdf");
  }

  @Test public void default_constructor_test_myltiple_hyphen_signs() {
    String version = "1.2.3-beta.1-1.ab-c+sha.0nsfgkjkjs-df";
    Semver semver = new Semver(version);
    assertIsSemver(semver, version, 1, 2, 3, new String[]{"beta", "1-1", "ab-c"}, "sha.0nsfgkjkjs-df");
  }

  private static void assertIsSemver(Semver semver, String value, Integer major, Integer minor, Integer patch, String[] suffixTokens, String build) {
    assertEquals(value, semver.getValue());
    assertEquals(major, semver.getMajor());
    assertEquals(minor, semver.getMinor());
    assertEquals(patch, semver.getPatch());
    assertEquals(suffixTokens.length, semver.getSuffixTokens().length);
    for (int i = 0; i < suffixTokens.length; i++) {
      assertEquals(suffixTokens[i], semver.getSuffixTokens()[i]);
    }
    assertEquals(build, semver.getBuild());
  }

  @Test public void statisfies_works_will_all_the_types() {
    // Used to prevent bugs when we add a new type
    for (Semver.SemverType type : Semver.SemverType.values()) {
      String version = "1.2.3";
      Semver semver = new Semver(version, type);
      assertTrue(semver.satisfies("1.2.3"));
      assertFalse(semver.satisfies("4.5.6"));
    }
  }

  @Test public void isGreaterThan_test() {
    // 1.0.0-alpha < 1.0.0-alpha.1 < 1.0.0-alpha.beta < 1.0.0-beta < 1.0.0-beta.2 < 1.0.0-beta.11 < 1.0.0-rc.1 < 1.0.0

    assertTrue(new Semver("1.0.0-alpha.1").isGreaterThan("1.0.0-alpha"));
    assertTrue(new Semver("1.0.0-alpha.beta").isGreaterThan("1.0.0-alpha.1"));
    assertTrue(new Semver("1.0.0-beta").isGreaterThan("1.0.0-alpha.beta"));
    assertTrue(new Semver("1.0.0-beta.2").isGreaterThan("1.0.0-beta"));
    assertTrue(new Semver("1.0.0-beta.11").isGreaterThan("1.0.0-beta.2"));
    assertTrue(new Semver("1.0.0-rc.1").isGreaterThan("1.0.0-beta.11"));
    assertTrue(new Semver("1.0.0").isGreaterThan("1.0.0-rc.1"));


    assertFalse(new Semver("1.0.0-alpha").isGreaterThan("1.0.0-alpha.1"));
    assertFalse(new Semver("1.0.0-alpha.1").isGreaterThan("1.0.0-alpha.beta"));
    assertFalse(new Semver("1.0.0-alpha.beta").isGreaterThan("1.0.0-beta"));
    assertFalse(new Semver("1.0.0-beta").isGreaterThan("1.0.0-beta.2"));
    assertFalse(new Semver("1.0.0-beta.2").isGreaterThan("1.0.0-beta.11"));
    assertFalse(new Semver("1.0.0-beta.11").isGreaterThan("1.0.0-rc.1"));
    assertFalse(new Semver("1.0.0-rc.1").isGreaterThan("1.0.0"));

    assertFalse(new Semver("1.0.0").isGreaterThan("1.0.0"));
    assertFalse(new Semver("1.0.0-alpha.12").isGreaterThan("1.0.0-alpha.12"));

    assertFalse(new Semver("0.0.1").isGreaterThan("5.0.0"));
    assertFalse(new Semver("1.0.0-alpha.12.ab-c").isGreaterThan("1.0.0-alpha.12.ab-c"));
  }

  @Test public void isLowerThan_test() {
    // 1.0.0-alpha < 1.0.0-alpha.1 < 1.0.0-alpha.beta < 1.0.0-beta < 1.0.0-beta.2 < 1.0.0-beta.11 < 1.0.0-rc.1 < 1.0.0

    assertFalse(new Semver("1.0.0-alpha.1").isLowerThan("1.0.0-alpha"));
    assertFalse(new Semver("1.0.0-alpha.beta").isLowerThan("1.0.0-alpha.1"));
    assertFalse(new Semver("1.0.0-beta").isLowerThan("1.0.0-alpha.beta"));
    assertFalse(new Semver("1.0.0-beta.2").isLowerThan("1.0.0-beta"));
    assertFalse(new Semver("1.0.0-beta.11").isLowerThan("1.0.0-beta.2"));
    assertFalse(new Semver("1.0.0-rc.1").isLowerThan("1.0.0-beta.11"));
    assertFalse(new Semver("1.0.0").isLowerThan("1.0.0-rc.1"));

    assertTrue(new Semver("1.0.0-alpha").isLowerThan("1.0.0-alpha.1"));
    assertTrue(new Semver("1.0.0-alpha.1").isLowerThan("1.0.0-alpha.beta"));
    assertTrue(new Semver("1.0.0-alpha.beta").isLowerThan("1.0.0-beta"));
    assertTrue(new Semver("1.0.0-beta").isLowerThan("1.0.0-beta.2"));
    assertTrue(new Semver("1.0.0-beta.2").isLowerThan("1.0.0-beta.11"));
    assertTrue(new Semver("1.0.0-beta.11").isLowerThan("1.0.0-rc.1"));
    assertTrue(new Semver("1.0.0-rc.1").isLowerThan("1.0.0"));

    assertFalse(new Semver("1.0.0").isLowerThan("1.0.0"));
    assertFalse(new Semver("1.0.0-alpha.12").isLowerThan("1.0.0-alpha.12"));
    assertFalse(new Semver("1.0.0-alpha.12.x-yz").isLowerThan("1.0.0-alpha.12.x-yz"));
  }

  @Test public void isEquivalentTo_isEqualTo_and_build() {
    Semver semver = new Semver("1.0.0+ksadhjgksdhgksdhgfj");
    String version2 = "1.0.0+sdgfsdgsdhsdfgdsfgf";
    assertFalse(semver.isEqualTo(version2));
    assertTrue(semver.isEquivalentTo(version2));
  }

  @Test public void statisfies_calls_the_requirement() {
    Requirement req = mock(Requirement.class);
    Semver semver = new Semver("1.2.2");
    semver.satisfies(req);
    verify(req).isSatisfiedBy(semver);
  }

  @Test public void withIncMajor_test() {
    Semver semver = new Semver("1.2.3-Beta.4+SHA123456789");
    semver.withIncMajor(2).isEqualTo("3.2.3-Beta.4+SHA123456789");
  }

  @Test public void withIncMinor_test() {
    Semver semver = new Semver("1.2.3-Beta.4+SHA123456789");
    semver.withIncMinor(2).isEqualTo("1.4.3-Beta.4+SHA123456789");
  }

  @Test public void withIncPatch_test() {
    Semver semver = new Semver("1.2.3-Beta.4+SHA123456789");
    semver.withIncPatch(2).isEqualTo("1.2.5-Beta.4+SHA123456789");
  }

  @Test public void withClearedSuffix_test() {
    Semver semver = new Semver("1.2.3-Beta.4+SHA123456789");
    semver.withClearedSuffix().isEqualTo("1.2.3+SHA123456789");
  }

  @Test public void withClearedBuild_test() {
    Semver semver = new Semver("1.2.3-Beta.4+sha123456789");
    semver.withClearedBuild().isEqualTo("1.2.3-Beta.4");
  }

  @Test public void withClearedBuild_test_multiple_hyphen_signs() {
    Semver semver = new Semver("1.2.3-Beta.4-test+sha12345-6789");
    semver.withClearedBuild().isEqualTo("1.2.3-Beta.4-test");
  }

  @Test public void withClearedSuffixAndBuild_test() {
    Semver semver = new Semver("1.2.3-Beta.4+SHA123456789");
    semver.withClearedSuffixAndBuild().isEqualTo("1.2.3");
  }

  @Test public void withSuffix_test_change_suffix() {
    Semver semver = new Semver("1.2.3-Alpha.4+SHA123456789");
    Semver result = semver.withSuffix("Beta.1");

    assertEquals("1.2.3-Beta.1+SHA123456789", result.toString());
    assertArrayEquals(new String[] { "Beta", "1" }, result.getSuffixTokens());
  }

  @Test public void withSuffix_test_add_suffix() {
    Semver semver = new Semver("1.2.3+SHA123456789");
    Semver result = semver.withSuffix("Beta.1");

    assertEquals("1.2.3-Beta.1+SHA123456789", result.toString());
    assertArrayEquals(new String[] { "Beta", "1" }, result.getSuffixTokens());
  }

  @Test public void withBuild_test_change_build() {
    Semver semver = new Semver("1.2.3-Alpha.4+SHA123456789");
    Semver result = semver.withBuild("SHA987654321");

    assertEquals("1.2.3-Alpha.4+SHA987654321", result.toString());
    assertEquals("SHA987654321", result.getBuild());
  }

  @Test public void withBuild_test_add_build() {
    Semver semver = new Semver("1.2.3-Alpha.4");
    Semver result = semver.withBuild("SHA987654321");

    assertEquals("1.2.3-Alpha.4+SHA987654321", result.toString());
    assertEquals("SHA987654321", result.getBuild());
  }

  @Test public void nextMajor_test() {
    Semver semver = new Semver("1.2.3-beta.4+sha123456789");
    semver.nextMajor().isEqualTo("2.0.0");
  }

  @Test public void nextMinor_test() {
    Semver semver = new Semver("1.2.3-beta.4+sha123456789");
    semver.nextMinor().isEqualTo("1.3.0");
  }

  @Test public void nextPatch_test() {
    Semver semver = new Semver("1.2.3-beta.4+sha123456789");
    semver.nextPatch().isEqualTo("1.2.4");
  }

  @Test public void toStrict_test() {
    String[][] versionGroups = new String[][]{
        new String[]{"3.0.0-beta.4+sha123456789", "3.0-beta.4+sha123456789", "3-beta.4+sha123456789"},
        new String[]{"3.0.0+sha123456789", "3.0+sha123456789", "3+sha123456789"},
        new String[]{"3.0.0-beta.4", "3.0-beta.4", "3-beta.4"},
        new String[]{"3.0.0", "3.0", "3"},
    };

    Semver.SemverType[] types = new Semver.SemverType[]{
        Semver.SemverType.NPM,
        Semver.SemverType.IVY,
        Semver.SemverType.LOOSE,
        Semver.SemverType.COCOAPODS,
    };

    for(String[] versions: versionGroups) {
      Semver strict = new Semver(versions[0]);
      assertEquals(strict, strict.toStrict());
      for(Semver.SemverType type: types) {
        for(String version: versions) {
          Semver sem = new Semver(version, type);
          assertEquals(strict, sem.toStrict());
        }
      }
    }
  }

  @Test public void diff() {
    Semver sem = new Semver("1.2.3-beta.4+sha899d8g79f87");
    assertEquals(Semver.VersionDiff.NONE, sem.diff("1.2.3-beta.4+sha899d8g79f87"));
    assertEquals(Semver.VersionDiff.MAJOR, sem.diff("2.3.4-alpha.5+sha32iddfu987"));
    assertEquals(Semver.VersionDiff.MINOR, sem.diff("1.3.4-alpha.5+sha32iddfu987"));
    assertEquals(Semver.VersionDiff.PATCH, sem.diff("1.2.4-alpha.5+sha32iddfu987"));
    assertEquals(Semver.VersionDiff.SUFFIX, sem.diff("1.2.3-alpha.4+sha32iddfu987"));
    assertEquals(Semver.VersionDiff.SUFFIX, sem.diff("1.2.3-beta.5+sha32iddfu987"));
    assertEquals(Semver.VersionDiff.BUILD, sem.diff("1.2.3-beta.4+sha32iddfu987"));
    assertEquals(Semver.VersionDiff.BUILD, sem.diff("1.2.3-beta.4+sha899-d8g79f87"));
  }

  @Test public void compareTo_test() {
    // GIVEN
    Semver[] array = new Semver[]{
        new Semver("1.2.3"),
        new Semver("1.2.3-rc3"),
        new Semver("1.2.3-rc2"),
        new Semver("1.2.3-rc1"),
        new Semver("1.2.2"),
        new Semver("1.2.2-rc2"),
        new Semver("1.2.2-rc1"),
        new Semver("1.2.0")
    };
    int len = array.length;
    List<Semver> list = new ArrayList<Semver>(len);
    Collections.addAll(list, array);

    // WHEN
    Collections.sort(list);

    // THEN
    for (int i = 0; i < list.size(); i++) {
      assertEquals(array[len - 1 - i], list.get(i));
    }
  }

  @Test public void compareTo_without_path_or_minor() {
    assertTrue(new Semver("1.2.3", Semver.SemverType.LOOSE).isGreaterThan("1.2"));
    assertTrue(new Semver("1.3", Semver.SemverType.LOOSE).isGreaterThan("1.2.3"));
    assertTrue(new Semver("1.2.3", Semver.SemverType.LOOSE).isGreaterThan("1"));
    assertTrue(new Semver("2", Semver.SemverType.LOOSE).isGreaterThan("1.2.3"));
  }

  @Test public void getValue_returns_the_original_value_trimmed_and_with_the_same_case() {
    String version = "  1.2.3-BETA.11+sHa.0nSFGKjkjsdf  ";
    Semver semver = new Semver(version);
    assertEquals("1.2.3-BETA.11+sHa.0nSFGKjkjsdf", semver.getValue());
  }

  @Test
  public void compareTo_with_buildNumber() {
    Semver v3 = new Semver("1.24.1-rc3+903423.234");
    Semver v4 = new Semver("1.24.1-rc3+903423.235");
    assertEquals(0, v3.compareTo(v4));
  }

  @Test public void isStable_test() {
    assertTrue(new Semver("1.2.3+sHa.0nSFGKjkjsdf").isStable());
    assertTrue(new Semver("1.2.3").isStable());
    assertFalse(new Semver("1.2.3-BETA.11+sHa.0nSFGKjkjsdf").isStable());
    assertFalse(new Semver("0.1.2+sHa.0nSFGKjkjsdf").isStable());
    assertFalse(new Semver("0.1.2").isStable());
  }
}
