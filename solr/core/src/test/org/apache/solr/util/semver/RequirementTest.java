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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

@RunWith(JUnit4.class)
public class RequirementTest {
  @Test public void buildStrict() {
    String version = "1.2.3";
    Requirement requirement = Requirement.buildStrict(version);
    assertIsRange(requirement, version, Range.RangeOperator.EQ);
  }

  @Test public void buildLoose() {
    String version = "0.27";
    Requirement requirement = Requirement.buildLoose(version);
    assertIsRange(requirement, version, Range.RangeOperator.EQ);
  }

  @Test public void buildNPM_with_a_full_version() {
    String version = "1.2.3";
    Requirement requirement = Requirement.buildNPM(version);
    assertIsRange(requirement, version, Range.RangeOperator.EQ);
  }

  @Test public void buildNPM_with_a_version_with_a_leading_v() {
    assertIsRange(Requirement.buildNPM("v1.2.3"), "1.2.3", Range.RangeOperator.EQ);
    assertIsRange(Requirement.buildNPM("v 1.2.3"), "1.2.3", Range.RangeOperator.EQ);
  }

  @Test public void buildNPM_with_a_version_with_a_leading_equal() {
    assertIsRange(Requirement.buildNPM("=1.2.3"), "1.2.3", Range.RangeOperator.EQ);
    assertIsRange(Requirement.buildNPM("= 1.2.3"), "1.2.3", Range.RangeOperator.EQ);
  }

  @Test public void buildNPM_with_a_range() {
    Requirement req = Requirement.buildNPM(">=1.2.3 <4.5.6");
    rangeTest(req, "1.2.3", "4.5.6", true);
  }

  @Test public void buildNPM_with_a_OR_operator() {
    Requirement req = Requirement.buildNPM(">=1.2.3 || >4.5.6");

    assertNull(req.range);
    assertEquals(Requirement.RequirementOperator.OR, req.op);

    Requirement req1 = req.req1;
    assertEquals(Range.RangeOperator.GTE, req1.range.op);
    assertEquals("1.2.3", req1.range.version.getValue());

    Requirement req2 = req.req2;
    assertEquals(Range.RangeOperator.GT, req2.range.op);
    assertEquals("4.5.6", req2.range.version.getValue());
  }

  @Test public void buildNPM_with_OR_and_AND_operators() {
    Requirement req = Requirement.buildNPM(">1.2.1 <1.2.8 || >2.0.0 <3.0.0");

    assertNull(req.range);
    assertEquals(Requirement.RequirementOperator.OR, req.op);

    // >1.2.1 <1.2.8
    Requirement req1 = req.req1;
    assertNull(req1.range);
    assertEquals(Requirement.RequirementOperator.AND, req1.op);

    Requirement req1_1 = req1.req1;
    assertNull(req1_1.op);
    assertEquals(Range.RangeOperator.GT, req1_1.range.op);
    assertEquals("1.2.1", req1_1.range.version.getValue());

    Requirement req1_2 = req1.req2;
    assertNull(req1_2.op);
    assertEquals(Range.RangeOperator.LT, req1_2.range.op);
    assertEquals("1.2.8", req1_2.range.version.getValue());

    // >2.0.0 < 3.0.0
    Requirement req2 = req.req2;
    assertNull(req2.range);
    assertEquals(Requirement.RequirementOperator.AND, req2.op);

    Requirement req2_1 = req2.req1;
    assertNull(req2_1.op);
    assertEquals(Range.RangeOperator.GT, req2_1.range.op);
    assertEquals("2.0.0", req2_1.range.version.getValue());

    Requirement req2_2 = req2.req2;
    assertNull(req2_2.op);
    assertEquals(Range.RangeOperator.LT, req2_2.range.op);
    assertEquals("3.0.0", req2_2.range.version.getValue());
  }

  @Test public void tildeRequirement_npm_full_version() {
    // ~1.2.3 := >=1.2.3 <1.(2+1).0 := >=1.2.3 <1.3.0
    tildeTest("1.2.3", "1.2.3", "1.3.0", Semver.SemverType.NPM);
  }

  @Test public void tildeRequirement_npm_only_major_and_minor() {
    // ~1.2 := >=1.2.0 <1.(2+1).0 := >=1.2.0 <1.3.0
    tildeTest("1.2", "1.2.0", "1.3.0", Semver.SemverType.NPM);
    tildeTest("1.2.x", "1.2.0", "1.3.0", Semver.SemverType.NPM);
    tildeTest("1.2.*", "1.2.0", "1.3.0", Semver.SemverType.NPM);
  }

  @Test public void tildeRequirement_npm_only_major() {
    // ~1 := >=1.0.0 <(1+1).0.0 := >=1.0.0 <2.0.0
    tildeTest("1", "1.0.0", "2.0.0", Semver.SemverType.NPM);
    tildeTest("1.x", "1.0.0", "2.0.0", Semver.SemverType.NPM);
    tildeTest("1.x.x", "1.0.0", "2.0.0", Semver.SemverType.NPM);
    tildeTest("1.*", "1.0.0", "2.0.0", Semver.SemverType.NPM);
    tildeTest("1.*.*", "1.0.0", "2.0.0", Semver.SemverType.NPM);
  }

  @Test public void tildeRequirement_npm_full_version_major_0() {
    // ~0.2.3 := >=0.2.3 <0.(2+1).0 := >=0.2.3 <0.3.0
    tildeTest("0.2.3", "0.2.3", "0.3.0", Semver.SemverType.NPM);
  }

  @Test public void tildeRequirement_npm_only_major_and_minor_with_major_0() {
    // ~0.2 := >=0.2.0 <0.(2+1).0 := >=0.2.0 <0.3.0
    tildeTest("0.2", "0.2.0", "0.3.0", Semver.SemverType.NPM);
    tildeTest("0.2.x", "0.2.0", "0.3.0", Semver.SemverType.NPM);
    tildeTest("0.2.*", "0.2.0", "0.3.0", Semver.SemverType.NPM);
  }

  @Test public void tildeRequirement_npm_only_major_with_major_0() {
    // ~0 := >=0.0.0 <(0+1).0.0 := >=0.0.0 <1.0.0
    tildeTest("0", "0.0.0", "1.0.0", Semver.SemverType.NPM);
    tildeTest("0.x", "0.0.0", "1.0.0", Semver.SemverType.NPM);
    tildeTest("0.x.x", "0.0.0", "1.0.0", Semver.SemverType.NPM);
    tildeTest("0.*", "0.0.0", "1.0.0", Semver.SemverType.NPM);
    tildeTest("0.*.*", "0.0.0", "1.0.0", Semver.SemverType.NPM);
  }

  @Test public void tildeRequirement_npm_with_suffix() {
    // ~1.2.3-beta.2 := >=1.2.3-beta.2 <1.3.0
    tildeTest("1.2.3-beta.2", "1.2.3-beta.2", "1.3.0", Semver.SemverType.NPM);
  }

  @Test public void caretRequirement_npm_full_version() {
    // ^1.2.3 := >=1.2.3 <2.0.0
    caretTest("1.2.3", "1.2.3", "2.0.0");
  }

  @Test public void caretRequirement_npm_full_version_with_major_0() {
    // ^0.2.3 := >=0.2.3 <0.3.0
    caretTest("0.2.3", "0.2.3", "0.3.0");
  }

  @Test public void caretRequirement_npm_full_version_with_major_and_minor_0() {
    // ^0.0.3 := >=0.0.3 <0.0.4
    caretTest("0.0.3", "0.0.3", "0.0.4");
  }

  @Test public void caretRequirement_npm_with_suffix() {
    // ^1.2.3-beta.2 := >=1.2.3-beta.2 <2.0.0
    caretTest("1.2.3-beta.2", "1.2.3-beta.2", "2.0.0");
  }

  @Test public void caretRequirement_npm_with_major_and_minor_0_and_suffix() {
    // ^0.0.3-beta := >=0.0.3-beta <0.0.4
    caretTest("0.0.3-beta", "0.0.3-beta", "0.0.4");
  }

  @Test public void caretRequirement_npm_without_patch() {
    // ^1.2.x := >=1.2.0 <2.0.0
    caretTest("1.2", "1.2.0", "2.0.0");
    caretTest("1.2.x", "1.2.0", "2.0.0");
    caretTest("1.2.*", "1.2.0", "2.0.0");
  }

  @Test public void caretRequirement_npm_with_only_major() {
    // ^1.x.x := >=1.0.0 <2.0.0
    caretTest("1", "1.0.0", "2.0.0");
    caretTest("1.x", "1.0.0", "2.0.0");
    caretTest("1.x.x", "1.0.0", "2.0.0");
    caretTest("1.*", "1.0.0", "2.0.0");
    caretTest("1.*.*", "1.0.0", "2.0.0");
  }

  @Test public void caretRequirement_npm_with_only_major_0() {
    // ^0.x := >=0.0.0 <1.0.0
    caretTest("0", "0.0.0", "1.0.0");
    caretTest("0.x", "0.0.0", "1.0.0");
    caretTest("0.x.x", "0.0.0", "1.0.0");
    caretTest("0.*", "0.0.0", "1.0.0");
    caretTest("0.*.*", "0.0.0", "1.0.0");
  }

  @Test public void caretRequirement_npm_without_patch_with_major_and_minor_0() {
    // ^0.0.x := >=0.0.0 <0.1.0
    caretTest("0.0", "0.0.0", "0.1.0");
    caretTest("0.0.x", "0.0.0", "0.1.0");
    caretTest("0.0.*", "0.0.0", "0.1.0");
  }

  @Test public void hyphenRequirement() {
    // 1.2.3 - 2.3.4 := >=1.2.3 <=2.3.4
    hyphenTest("1.2.3", "2.3.4", "1.2.3", "2.3.4", false);
  }

  @Test public void hyphenRequirement_with_partial_lower_bound() {
    // 1.2 - 2.3.4 := >=1.2.0 <=2.3.4
    hyphenTest("1.2", "2.3.4", "1.2.0", "2.3.4", false);
    hyphenTest("1.2.x", "2.3.4", "1.2.0", "2.3.4", false);
    hyphenTest("1.2.*", "2.3.4", "1.2.0", "2.3.4", false);

    // 1 - 2.3.4 := >=1.0.0 <=2.3.4
    hyphenTest("1", "2.3.4", "1.0.0", "2.3.4", false);
    hyphenTest("1.x", "2.3.4", "1.0.0", "2.3.4", false);
    hyphenTest("1.x.x", "2.3.4", "1.0.0", "2.3.4", false);
    hyphenTest("1.*", "2.3.4", "1.0.0", "2.3.4", false);
    hyphenTest("1.*.*", "2.3.4", "1.0.0", "2.3.4", false);
  }

  @Test public void hyphenRequirement_with_partial_upper_bound() {
    // 1.2.3 - 2.3 := >=1.2.3 <2.4.0
    hyphenTest("1.2.3", "2.3", "1.2.3", "2.4.0", true);
    hyphenTest("1.2.3", "2.3.x", "1.2.3", "2.4.0", true);
    hyphenTest("1.2.3", "2.3.*", "1.2.3", "2.4.0", true);

    // 1.2.3 - 2 := >=1.2.3 <3.0.0
    hyphenTest("1.2.3", "2", "1.2.3", "3.0.0", true);
    hyphenTest("1.2.3", "2.x", "1.2.3", "3.0.0", true);
    hyphenTest("1.2.3", "2.x.x", "1.2.3", "3.0.0", true);
    hyphenTest("1.2.3", "2.*", "1.2.3", "3.0.0", true);
    hyphenTest("1.2.3", "2.*.*", "1.2.3", "3.0.0", true);
  }

  @Test public void buildNPM_with_hyphen() {
    Requirement[] reqs = new Requirement[]{
        Requirement.buildNPM("1.2.3-2.3.4"),
        Requirement.buildNPM("1.2.3 -2.3.4"),
        Requirement.buildNPM("1.2.3- 2.3.4"),
        Requirement.buildNPM("1.2.3 - 2.3.4")
    };

    for (Requirement req : reqs) {
      rangeTest(req, "1.2.3", "2.3.4", false);
    }
  }

  @Test public void buildNPM_with_a_wildcard() {
    Requirement req = Requirement.buildNPM("*");
    assertNull(req.op);
    assertNull(req.req1);
    assertNull(req.req2);
    assertEquals(Range.RangeOperator.GTE, req.range.op);
    assertEquals(new Semver("0.0.0"), req.range.version);
  }

  @Test public void buildCocoapods_with_a_tilde() {
    Requirement[] reqs = new Requirement[]{
        Requirement.buildCocoapods(" ~> 1.2.3 "),
        Requirement.buildCocoapods(" ~> 1.2.3"),
        Requirement.buildCocoapods("~> 1.2.3 "),
        Requirement.buildCocoapods(" ~>1.2.3 "),
        Requirement.buildCocoapods("~>1.2.3 "),
        Requirement.buildCocoapods("~> 1.2.3"),
        Requirement.buildCocoapods("~>1.2.3"),
    };

    for (Requirement req : reqs) {
      rangeTest(req, "1.2.3", "1.3.0", true);
    }
  }

  @Test public void buildCocoapods_with_a_wildcard() {
    Requirement req = Requirement.buildCocoapods("*");
    assertNull(req.op);
    assertNull(req.req1);
    assertNull(req.req2);
    assertEquals(Range.RangeOperator.GTE, req.range.op);
    assertEquals(new Semver("0.0.0"), req.range.version);
  }

  @Test public void buildIvy_with_a_dynamic_patch() {
    Requirement req = Requirement.buildIvy("1.2.+");
    assertEquals(Requirement.RequirementOperator.AND, req.op);
    assertNull(req.range);
    assertIsRange(req.req1, "1.2.0", Range.RangeOperator.GTE);
    assertIsRange(req.req2, "1.3.0", Range.RangeOperator.LT);
  }

  @Test public void buildIvy_with_a_dynamic_minor() {
    Requirement req = Requirement.buildIvy("1.+");
    assertEquals(Requirement.RequirementOperator.AND, req.op);
    assertNull(req.range);
    assertIsRange(req.req1, "1.0.0", Range.RangeOperator.GTE);
    assertIsRange(req.req2, "2.0.0", Range.RangeOperator.LT);
  }

  @Test public void buildIvy_with_latest() {
    Requirement req = Requirement.buildIvy("latest.integration");
    assertIsRange(req, "0.0.0", Range.RangeOperator.GTE);
  }

  @Test public void buildIvy_with_mathematical_bounded_ranges() {
    rangeTest(Requirement.buildIvy("[1.0,2.0]"), "1.0.0", false, "2.0.0", false);
    rangeTest(Requirement.buildIvy("[1.0,2.0["), "1.0.0", false, "2.0.0", true);
    rangeTest(Requirement.buildIvy("]1.0,2.0]"), "1.0.0", true, "2.0.0", false);
    rangeTest(Requirement.buildIvy("]1.0,2.0["), "1.0.0", true, "2.0.0", true);
  }

  @Test public void buildIvy_with_mathematical_unbounded_ranges() {
    assertIsRange(Requirement.buildIvy("[1.0,)"), "1.0.0", Range.RangeOperator.GTE);
    assertIsRange(Requirement.buildIvy("]1.0,)"), "1.0.0", Range.RangeOperator.GT);
    assertIsRange(Requirement.buildIvy("(,2.0]"), "2.0.0", Range.RangeOperator.LTE);
    assertIsRange(Requirement.buildIvy("(,2.0["), "2.0.0", Range.RangeOperator.LT);
  }

  @Test public void isSatisfiedBy_with_a_loose_type() {
    Requirement req = Requirement.buildLoose("1.3.2");

    assertFalse(req.isSatisfiedBy("0.27"));
    assertTrue(req.isSatisfiedBy("1.3.2"));
    assertFalse(req.isSatisfiedBy("1.5"));
  }

  @Test public void isSatisfiedBy_with_a_complex_example() {
    Requirement req = Requirement.buildNPM("1.x || >=2.5.0 || 5.0.0 - 7.2.3");

    assertTrue(req.isSatisfiedBy("1.2.3"));
    assertTrue(req.isSatisfiedBy("2.5.2"));
    assertFalse(req.isSatisfiedBy("0.2.3"));
  }

  @Test public void isSatisfiedBy_with_a_range() {
    Range range = mock(Range.class);
    Requirement requirement = new Requirement(range, null, null, null);
    Semver version = new Semver("1.2.3");
    requirement.isSatisfiedBy(version);
    verify(range).isSatisfiedBy(version);
  }

  @Test public void isSatisfiedBy_with_subRequirements_AND_first_is_true() {
    Semver version = new Semver("1.2.3");

    Requirement req1 = mock(Requirement.class);
    when(req1.isSatisfiedBy(version)).thenReturn(true);
    Requirement req2 = mock(Requirement.class);
    Requirement requirement = new Requirement(null, req1, Requirement.RequirementOperator.AND, req2);

    requirement.isSatisfiedBy(version);

    verify(req1).isSatisfiedBy(version);
    verify(req2).isSatisfiedBy(version);
  }

  @Test public void isSatisfiedBy_with_subRequirements_AND_first_is_false() {
    Semver version = new Semver("1.2.3");

    Requirement req1 = mock(Requirement.class);
    when(req1.isSatisfiedBy(version)).thenReturn(false);
    Requirement req2 = mock(Requirement.class);
    Requirement requirement = new Requirement(null, req1, Requirement.RequirementOperator.AND, req2);

    requirement.isSatisfiedBy(version);

    verify(req1).isSatisfiedBy(version);
    verifyZeroInteractions(req2);
  }

  @Test public void isSatisfiedBy_with_subRequirements_OR_first_is_true() {
    Semver version = new Semver("1.2.3");

    Requirement req1 = mock(Requirement.class);
    when(req1.isSatisfiedBy(version)).thenReturn(true);
    Requirement req2 = mock(Requirement.class);
    Requirement requirement = new Requirement(null, req1, Requirement.RequirementOperator.OR, req2);

    requirement.isSatisfiedBy(version);

    verify(req1).isSatisfiedBy(version);
    verifyZeroInteractions(req2);
  }

  @Test public void isSatisfiedBy_with_subRequirements_OR_first_is_false() {
    Semver version = new Semver("1.2.3");

    Requirement req1 = mock(Requirement.class);
    when(req1.isSatisfiedBy(version)).thenReturn(false);
    Requirement req2 = mock(Requirement.class);
    Requirement requirement = new Requirement(null, req1, Requirement.RequirementOperator.OR, req2);

    requirement.isSatisfiedBy(version);

    verify(req1).isSatisfiedBy(version);
    verify(req2).isSatisfiedBy(version);
  }

  @Test public void npm_isSatisfiedBy_with_an_empty_string() {
    Requirement req = Requirement.buildNPM("");
    assertTrue(req.isSatisfiedBy("1.2.3"));
    assertTrue(req.isSatisfiedBy("2.5.2"));
    assertTrue(req.isSatisfiedBy("0.2.3"));
  }

  @Test public void isSatisfiedBy_with_a_star() {
    Requirement req = Requirement.buildNPM("*");
    assertTrue(req.isSatisfiedBy("1.2.3"));
    assertTrue(req.isSatisfiedBy("2.5.2"));
    assertTrue(req.isSatisfiedBy("0.2.3"));
  }

  @Test public void isSatisfiedBy_with_latest() {
    Requirement req = Requirement.buildNPM("latest");
    assertTrue(req.isSatisfiedBy("1.2.3"));
    assertTrue(req.isSatisfiedBy("2.5.2"));
    assertTrue(req.isSatisfiedBy("0.2.3"));
  }

  @Test public void tildeRequirement_cocoapods() {
    // '~> 0.1.2' Version 0.1.2 and the versions up to 0.2, not including 0.2 and higher
    tildeTest("0.1.2", "0.1.2", "0.2.0", Semver.SemverType.COCOAPODS);
    tildeTest("1.1.2", "1.1.2", "1.2.0", Semver.SemverType.COCOAPODS);

    // '~> 0.1' Version 0.1 and the versions up to 1.0, not including 1.0 and higher
    tildeTest("0.1", "0.1.0", "1.0.0", Semver.SemverType.COCOAPODS);
    tildeTest("1.1", "1.1.0", "2.0.0", Semver.SemverType.COCOAPODS);

    // '~> 0' Version 0 and higher, this is basically the same as not having it.
    Requirement req = Requirement.tildeRequirement("0", Semver.SemverType.COCOAPODS);
    assertNull(req.op);
    assertEquals(Range.RangeOperator.GTE, req.range.op);
    assertTrue(req.range.version.isEquivalentTo("0.0.0"));

    req = Requirement.tildeRequirement("1", Semver.SemverType.COCOAPODS);
    assertNull(req.op);
    assertEquals(Range.RangeOperator.GTE, req.range.op);
    assertTrue(req.range.version.isEquivalentTo("1.0.0"));
  }

  @Test public void prettyString() {
    assertEquals(">=0.0.0", Requirement.buildNPM("latest").toString());
    assertEquals(">=0.0.0", Requirement.buildNPM("*").toString());
    assertEquals(">=1.0.0 <2.0.0", Requirement.buildNPM("1.*").toString());
    assertEquals(">=1.0.0 <2.0.0", Requirement.buildNPM("1.x").toString());
    assertEquals("=1.0.0", Requirement.buildNPM("1.0.0").toString());
    assertEquals("=1.0.0", Requirement.buildNPM("=1.0.0").toString());
    assertEquals("=1.0.0", Requirement.buildNPM("v1.0.0").toString());
    assertEquals("<1.0.0", Requirement.buildNPM("<1.0.0").toString());
    assertEquals("<=1.0.0", Requirement.buildNPM("<=1.0.0").toString());
    assertEquals(">1.0.0", Requirement.buildNPM(">1.0.0").toString());
    assertEquals(">=1.0.0", Requirement.buildNPM(">=1.0.0").toString());
    assertEquals(">=1.0.0 <1.1.0", Requirement.buildNPM("~1.0.0").toString());
    assertEquals(">=1.0.0 <2.0.0", Requirement.buildNPM("^1.0.0").toString());
    assertEquals(">=1.0.0 <2.0.0 || >=2.5.0 || >=5.0.0 <=7.2.3", Requirement.buildNPM("1.x || >=2.5.0 || 5.0.0 - 7.2.3").toString());

    assertEquals(">=1.2.0 <1.3.0", Requirement.buildCocoapods("~>1.2.0").toString());

    assertEquals(">=1.0.0 <=2.0.0", Requirement.buildIvy("[1.0,2.0]").toString());
    assertEquals(">=1.0.0 <2.0.0", Requirement.buildIvy("[1.0,2.0[").toString());
    assertEquals(">1.0.0 <=2.0.0", Requirement.buildIvy("]1.0,2.0]").toString());
    assertEquals(">1.0.0 <2.0.0", Requirement.buildIvy("]1.0,2.0[").toString());
    assertEquals(">=1.0.0", Requirement.buildIvy("[1.0,)").toString());
    assertEquals(">1.0.0", Requirement.buildIvy("]1.0,)").toString());
    assertEquals("<=2.0.0", Requirement.buildIvy("(,2.0]").toString());
    assertEquals("<2.0.0", Requirement.buildIvy("(,2.0[").toString());
  }

  @Test public void testEquals() {
    Requirement requirement = Requirement.buildStrict("1.2.3");

    assertEquals(requirement, requirement);
    assertEquals(requirement, Requirement.buildStrict("1.2.3"));
    assertEquals(requirement, Requirement.buildLoose("1.2.3"));
    assertEquals(requirement, Requirement.buildNPM("=1.2.3"));
    assertEquals(requirement, Requirement.buildIvy("1.2.3"));
    assertEquals(requirement, Requirement.buildCocoapods("1.2.3"));
    assertNotEquals(requirement, null);
    assertNotEquals(requirement, "string");
    assertNotEquals(requirement, Requirement.buildStrict("1.2.4"));
    assertNotEquals(requirement, Requirement.buildNPM(">1.2.3"));
  }

  @Test public void testHashCode() {
    Requirement requirement = Requirement.buildStrict("1.2.3");

    assertEquals(requirement.hashCode(), requirement.hashCode());
    assertEquals(requirement.hashCode(), Requirement.buildStrict("1.2.3").hashCode());
    assertEquals(requirement.hashCode(), Requirement.buildLoose("1.2.3").hashCode());
    assertEquals(requirement.hashCode(), Requirement.buildNPM("=1.2.3").hashCode());
    assertEquals(requirement.hashCode(), Requirement.buildIvy("1.2.3").hashCode());
    assertEquals(requirement.hashCode(), Requirement.buildCocoapods("1.2.3").hashCode());
    assertNotEquals(requirement.hashCode(), Requirement.buildStrict("1.2.4").hashCode());
    assertNotEquals(requirement.hashCode(), Requirement.buildNPM(">1.2.3").hashCode());
  }

  private static void assertIsRange(Requirement requirement, String version, Range.RangeOperator operator) {
    assertNull(requirement.req1);
    assertNull(requirement.op);
    assertNull(requirement.req2);
    Range range = requirement.range;
    assertTrue(range.version.isEquivalentTo(version));
    assertEquals(operator, range.op);
  }

  private static void tildeTest(String requirement, String lower, String upper, Semver.SemverType type) {
    Requirement req = Requirement.tildeRequirement(requirement, type);
    rangeTest(req, lower, upper, true);
  }

  private static void caretTest(String requirement, String lower, String upper) {
    Requirement req = Requirement.caretRequirement(requirement, Semver.SemverType.NPM);
    rangeTest(req, lower, upper, true);
  }

  private static void hyphenTest(String reqLower, String reqUpper, String lower, String upper, boolean upperStrict) {
    Requirement req = Requirement.hyphenRequirement(reqLower, reqUpper, Semver.SemverType.NPM);
    rangeTest(req, lower, upper, upperStrict);
  }

  private static void rangeTest(Requirement req, String lower, String upper, boolean upperStrict) {
    rangeTest(req, lower, false, upper, upperStrict);
  }

  private static void rangeTest(Requirement req, String lower, boolean lowerStrict, String upper, boolean upperStrict) {
    assertNull(req.range);
    assertEquals(Requirement.RequirementOperator.AND, req.op);

    Requirement req1 = req.req1;
    Range.RangeOperator lowOp = lowerStrict ? Range.RangeOperator.GT : Range.RangeOperator.GTE;
    assertEquals(lowOp, req1.range.op);
    assertEquals(lower, req1.range.version.getValue());

    Requirement req2 = req.req2;
    Range.RangeOperator upOp = upperStrict ? Range.RangeOperator.LT : Range.RangeOperator.LTE;
    assertEquals(upOp, req2.range.op);
    assertEquals(upper, req2.range.version.getValue());
  }
}
