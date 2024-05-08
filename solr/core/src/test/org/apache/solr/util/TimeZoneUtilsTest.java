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

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.SuppressForbidden;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeZoneUtilsTest extends SolrTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static void assertSameRules(
      final String label, final TimeZone expected, final TimeZone actual) {

    if (null == expected && null == actual) return;

    assertNotNull(label + ": expected is null", expected);
    assertNotNull(label + ": actual is null", actual);

    final boolean same = expected.hasSameRules(actual);

    assertTrue(label + ": " + expected + " [[NOT SAME RULES]] " + actual, same);
  }

  @SuppressForbidden(reason = "Using TimeZone.getDisplayName() just for warning purpose in a test")
  public void testValidIds() {

    final Set<String> idsTested = new HashSet<>();

    int skipped = 0;
    // brain dead: anything the JVM supports, should work
    for (String validId : TimeZone.getAvailableIDs()) {
      assertTrue(
          validId + " not found in list of known ids",
          TimeZoneUtils.KNOWN_TIMEZONE_IDS.contains(validId));

      final TimeZone expected = TimeZone.getTimeZone(validId);
      final TimeZone actual = TimeZoneUtils.getTimeZone(validId);

      // Hack: Why do some timezones have useDaylightTime() as true, but DST as 0?
      // It causes an exception during String.valueOf(actual/expected)
      if (expected.useDaylightTime() && expected.getDSTSavings() == 0
          || actual.useDaylightTime() && actual.getDSTSavings() == 0) {
        if (log.isWarnEnabled()) {
          log.warn(
              "Not expecting DST to be 0 for {} " + " (actual: {})",
              expected.getDisplayName(),
              actual.getDisplayName());
        }
        skipped++;
        continue;
      }
      assertSameRules(validId, expected, actual);

      idsTested.add(validId);
    }

    assertEquals(
        "TimeZone.getAvailableIDs vs TimeZoneUtils.KNOWN_TIMEZONE_IDS",
        TimeZoneUtils.KNOWN_TIMEZONE_IDS.size() - skipped,
        idsTested.size());
  }

  public void testCustom() {

    for (String input :
        new String[] {
          "GMT-00",
          "GMT+00",
          "GMT-0",
          "GMT+0",
          "GMT+08",
          "GMT+8",
          "GMT-08",
          "GMT-8",
          "GMT+0800",
          "GMT+08:00",
          "GMT-0800",
          "GMT-08:00",
          "GMT+23",
          "GMT+2300",
          "GMT-23",
          "GMT-2300"
        }) {
      assertSameRules(input, TimeZone.getTimeZone(input), TimeZoneUtils.getTimeZone(input));
    }
  }

  public void testStupidIKnowButIDontTrustTheJVM() {

    for (String input :
        new String[] {
          "GMT-00",
          "GMT+00",
          "GMT-0",
          "GMT+0",
          "GMT+08",
          "GMT+8",
          "GMT-08",
          "GMT-8",
          "GMT+0800",
          "GMT+08:00",
          "GMT-0800",
          "GMT-08:00",
          "GMT+23",
          "GMT+2300",
          "GMT-23",
          "GMT-2300"
        }) {
      assertSameRules(input, TimeZone.getTimeZone(input), TimeZone.getTimeZone(input));
    }
  }

  public void testInvalidInput() {

    final String giberish = "giberish";
    assumeFalse(
        "This test assumes that " + giberish + " is not a valid tz id",
        TimeZoneUtils.KNOWN_TIMEZONE_IDS.contains(giberish));
    assertNull(giberish, TimeZoneUtils.getTimeZone(giberish));

    for (String malformed :
        new String[] {
          "GMT+72", "GMT0800",
          "GMT+2400", "GMT+24:00",
          "GMT+11-30", "GMT+11:-30",
          "GMT+0080", "GMT+00:80"
        }) {
      assertNull(malformed, TimeZoneUtils.getTimeZone(malformed));
    }
  }

  public void testRandom() {
    final String ONE_DIGIT = "%1d";
    final String TWO_DIGIT = "%02d";

    final Random r = random();
    final int iters = atLeast(r, 50);
    for (int i = 0; i <= iters; i++) {
      int hour = TestUtil.nextInt(r, 0, 23);
      int min = TestUtil.nextInt(r, 0, 59);

      String hours = String.format(Locale.ROOT, (r.nextBoolean() ? ONE_DIGIT : TWO_DIGIT), hour);
      String mins = String.format(Locale.ROOT, TWO_DIGIT, min);
      String input =
          "GMT"
              + (r.nextBoolean() ? "+" : "-")
              + hours
              + (r.nextBoolean() ? "" : ((r.nextBoolean() ? ":" : "") + mins));
      assertSameRules(input, TimeZone.getTimeZone(input), TimeZoneUtils.getTimeZone(input));
    }
  }
}
