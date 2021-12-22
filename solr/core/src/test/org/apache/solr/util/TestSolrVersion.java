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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.text.ParseException;
import java.util.Locale;
import java.util.Random;

@SuppressWarnings("deprecation")
public class TestSolrVersion extends SolrTestCase {

  public void testOnOrAfter() throws Exception {
    for (Field field : SolrVersion.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == SolrVersion.class) {
        SolrVersion v = (SolrVersion) field.get(SolrVersion.class);
        assertTrue("LATEST must be always onOrAfter(" + v + ")", SolrVersion.LATEST.onOrAfter(v));
      }
    }
    assertTrue(SolrVersion.SOLR_9_0_0.onOrAfter(SolrVersion.SOLR_8_11_1));
  }

  public void testToString() {
    assertEquals("9.0.0", SolrVersion.SOLR_9_0_0.toString());
    assertEquals("8.11.1", SolrVersion.SOLR_8_11_1.toString());
  }

  public void testParseLeniently() throws Exception {
    assertEquals(SolrVersion.SOLR_9_0_0, SolrVersion.parseLeniently("9.0"));
    assertEquals(SolrVersion.SOLR_9_0_0, SolrVersion.parseLeniently("9.0.0"));
    assertEquals(SolrVersion.SOLR_9_0_0, SolrVersion.parseLeniently("SOLR_90"));
    assertEquals(SolrVersion.SOLR_9_0_0, SolrVersion.parseLeniently("SOLR_9_0"));
    assertEquals(SolrVersion.SOLR_9_0_0, SolrVersion.parseLeniently("SOLR_9_0_0"));

    assertEquals(SolrVersion.LATEST, SolrVersion.parseLeniently("LATEST"));
    assertEquals(SolrVersion.LATEST, SolrVersion.parseLeniently("latest"));
    assertEquals(SolrVersion.LATEST, SolrVersion.parseLeniently("SOLR_CURRENT"));
    assertEquals(SolrVersion.LATEST, SolrVersion.parseLeniently("solr_current"));
  }

  public void testParseLenientlyExceptions() {
    ParseException expected =
        expectThrows(
            ParseException.class,
            () -> SolrVersion.parseLeniently("SOLR"));
    assertTrue(expected.getMessage().contains("SOLR"));

    expected =
        expectThrows(
            ParseException.class,
            () -> SolrVersion.parseLeniently("SOLR_610"));
    assertTrue(expected.getMessage().contains("SOLR_610"));

    expected =
        expectThrows(
            ParseException.class,
            () -> SolrVersion.parseLeniently("SOLR61"));
    assertTrue(expected.getMessage().contains("SOLR61"));

    expected =
        expectThrows(
            ParseException.class,
            () -> SolrVersion.parseLeniently("SOLR_7.0.0"));
    assertTrue(expected.getMessage().contains("SOLR_7.0.0"));
  }

  public void testParseLenientlyOnAllConstants() throws Exception {
    boolean atLeastOne = false;
    for (Field field : SolrVersion.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == SolrVersion.class) {
        atLeastOne = true;
        SolrVersion v = (SolrVersion) field.get(SolrVersion.class);
        assertEquals(v, SolrVersion.parseLeniently(v.toString()));
        assertEquals(v, SolrVersion.parseLeniently(field.getName()));
        assertEquals(v, SolrVersion.parseLeniently(field.getName().toLowerCase(Locale.ROOT)));
      }
    }
    assertTrue(atLeastOne);
  }

  public void testParse() throws Exception {
    assertEquals(SolrVersion.SOLR_8_11_1, SolrVersion.parse("8.11.1"));
    assertEquals(SolrVersion.SOLR_9_0_0, SolrVersion.parse("9.0.0"));

    // SolrVersion does not pass judgement on the major version:
    assertEquals(1, SolrVersion.parse("1.0").major);
    assertEquals(7, SolrVersion.parse("7.0.0").major);
  }

  public void testForwardsCompatibility() throws Exception {
    assertTrue(SolrVersion.parse("9.10.20").onOrAfter(SolrVersion.SOLR_9_0_0));
  }

  public void testParseExceptions() {
    ParseException expected =
        expectThrows(
            ParseException.class,
            () -> SolrVersion.parse("SOLR_7_0_0"));
    assertTrue(expected.getMessage().contains("SOLR_7_0_0"));

    expected =
        expectThrows(
            ParseException.class,
            () -> SolrVersion.parse("7.256"));
    assertTrue(expected.getMessage().contains("7.256"));

    expected =
        expectThrows(
            ParseException.class,
            () -> SolrVersion.parse("7.-1"));
    assertTrue(expected.getMessage().contains("7.-1"));

    expected =
        expectThrows(
            ParseException.class,
            () -> SolrVersion.parse("7.1.256"));
    assertTrue(expected.getMessage().contains("7.1.256"));

    expected =
        expectThrows(
            ParseException.class,
            () -> SolrVersion.parse("7.1.-1"));
    assertTrue(expected.getMessage().contains("7.1.-1"));

    expected =
        expectThrows(
            ParseException.class,
            () -> SolrVersion.parse("7.1.1.3"));
    assertTrue(expected.getMessage().contains("7.1.1.3"));

    expected =
        expectThrows(
            ParseException.class,
            () -> SolrVersion.parse("7.1.1.-1"));
    assertTrue(expected.getMessage().contains("7.1.1.-1"));

    expected =
        expectThrows(
            ParseException.class,
            () -> SolrVersion.parse("7.1.1.1"));
    assertTrue(expected.getMessage().contains("7.1.1.1"));

    expected =
        expectThrows(
            ParseException.class,
            () -> SolrVersion.parse("7.1.1.2"));
    assertTrue(expected.getMessage().contains("7.1.1.2"));

    expected =
        expectThrows(
            ParseException.class,
            () -> SolrVersion.parse("7.0.0.0"));
    assertTrue(expected.getMessage().contains("7.0.0.0"));

    expected =
        expectThrows(
            ParseException.class,
            () -> SolrVersion.parse("7.0.0.1.42"));
    assertTrue(expected.getMessage().contains("7.0.0.1.42"));

    expected =
        expectThrows(
            ParseException.class,
            () -> SolrVersion.parse("7..0.1"));
    assertTrue(expected.getMessage().contains("7..0.1"));
  }

  public void testDeprecations() throws Exception {
    // all but the latest version should be deprecated
    boolean atLeastOne = false;
    for (Field field : SolrVersion.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == SolrVersion.class) {
        atLeastOne = true;
        SolrVersion v = (SolrVersion) field.get(SolrVersion.class);
        final boolean dep = field.isAnnotationPresent(Deprecated.class);
        if (v.equals(SolrVersion.LATEST) && !field.getName().equals("SOLR_CURRENT")) {
          assertFalse(field.getName() + " should not be deprecated", dep);
        } else {
          assertTrue(field.getName() + " should be deprecated", dep);
        }
      }
    }
    assertTrue(atLeastOne);
  }

  public void testNonFloatingPointCompliantVersionNumbers() throws ParseException {
    SolrVersion version800 = SolrVersion.parse("8.0.0");
    assertTrue(SolrVersion.parse("8.10.0").onOrAfter(version800));
    assertTrue(SolrVersion.parse("8.10.0").onOrAfter(SolrVersion.parse("8.9.255")));
    assertTrue(SolrVersion.parse("8.128.0").onOrAfter(version800));
    assertTrue(SolrVersion.parse("8.255.0").onOrAfter(version800));

    SolrVersion version400 = SolrVersion.parse("4.0.0");
    assertTrue(version800.onOrAfter(version400));
    assertTrue(SolrVersion.parse("8.128.0").onOrAfter(version400));
    assertFalse(version400.onOrAfter(version800));
  }

  public void testLatestVersionCommonBuild() {
    // common-build.xml sets 'tests.SOLR_VERSION', if not, we skip this test!
    String commonBuildVersion = System.getProperty("tests.SOLR_VERSION");
    assumeTrue(
        "Null 'tests.SOLR_VERSION' test property. You should run the tests with the official Solr build file",
        commonBuildVersion != null);
    assertEquals(
        "SolrVersion.LATEST does not match the one given in tests.SOLR_VERSION property",
        SolrVersion.LATEST.toString(),
        commonBuildVersion);
  }

  public void testEqualsHashCode() throws Exception {
    Random random = random();
    String version =
        "" + (4 + random.nextInt(1)) + "." + random.nextInt(10) + "." + random.nextInt(10);
    SolrVersion v1 = SolrVersion.parseLeniently(version);
    SolrVersion v2 = SolrVersion.parseLeniently(version);
    assertEquals(v1.hashCode(), v2.hashCode());
    assertEquals(v1, v2);
    final int iters = 10 + random.nextInt(20);
    for (int i = 0; i < iters; i++) {
      String v = "" + (4 + random.nextInt(1)) + "." + random.nextInt(10) + "." + random.nextInt(10);
      if (v.equals(version)) {
        assertEquals(SolrVersion.parseLeniently(v).hashCode(), v1.hashCode());
        assertEquals(SolrVersion.parseLeniently(v), v1);
      } else {
        assertNotEquals(SolrVersion.parseLeniently(v), v1);
      }
    }
  }
}
