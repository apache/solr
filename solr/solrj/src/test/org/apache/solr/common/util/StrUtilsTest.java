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
package org.apache.solr.common.util;

import static java.util.Arrays.asList;

import java.util.List;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;

public class StrUtilsTest extends SolrTestCase {

  public void testParseBool() {
    assertTrue(StrUtils.parseBool("true"));
    assertTrue(StrUtils.parseBool("True"));
    assertTrue(StrUtils.parseBool("TRUE"));
    assertTrue(StrUtils.parseBool("true  "));
    assertTrue(StrUtils.parseBool("trueAbc"));

    assertTrue(StrUtils.parseBool("on"));
    assertTrue(StrUtils.parseBool("ON"));
    assertTrue(StrUtils.parseBool("on   "));
    assertTrue(StrUtils.parseBool("onABC"));

    assertTrue(StrUtils.parseBool("yes"));
    assertTrue(StrUtils.parseBool("YES"));
    assertTrue(StrUtils.parseBool("yes   "));
    assertTrue(StrUtils.parseBool("yesABC"));

    assertFalse(StrUtils.parseBool("false"));
    assertFalse(StrUtils.parseBool("False"));
    assertFalse(StrUtils.parseBool("FALSE"));
    assertFalse(StrUtils.parseBool("false  "));
    assertFalse(StrUtils.parseBool("falseAbc"));

    assertFalse(StrUtils.parseBool("off"));
    assertFalse(StrUtils.parseBool("OFF"));
    assertFalse(StrUtils.parseBool("off   "));
    assertFalse(StrUtils.parseBool("offAbc"));

    assertFalse(StrUtils.parseBool("no"));
    assertFalse(StrUtils.parseBool("NO"));

    assertThrows(SolrException.class, () -> StrUtils.parseBool("foo"));
    assertThrows(SolrException.class, () -> StrUtils.parseBool(""));
    assertThrows(SolrException.class, () -> StrUtils.parseBool(null));
  }

  public void testParseBoolWithDefault() {
    assertTrue(StrUtils.parseBool("true", false));
    assertTrue(StrUtils.parseBool("True", false));
    assertTrue(StrUtils.parseBool("TRUE", false));
    assertTrue(StrUtils.parseBool("true  ", false));
    assertTrue(StrUtils.parseBool("trueAbc", false));

    assertTrue(StrUtils.parseBool("on", false));
    assertTrue(StrUtils.parseBool("ON", false));
    assertTrue(StrUtils.parseBool("on   ", false));
    assertTrue(StrUtils.parseBool("onABC", false));

    assertTrue(StrUtils.parseBool("yes", false));
    assertTrue(StrUtils.parseBool("YES", false));
    assertTrue(StrUtils.parseBool("yes   ", false));
    assertTrue(StrUtils.parseBool("yesABC", false));

    assertFalse(StrUtils.parseBool("false", true));
    assertFalse(StrUtils.parseBool("False", true));
    assertFalse(StrUtils.parseBool("FALSE", true));
    assertFalse(StrUtils.parseBool("false  ", true));
    assertFalse(StrUtils.parseBool("falseAbc", true));

    assertFalse(StrUtils.parseBool("off", true));
    assertFalse(StrUtils.parseBool("OFF", true));
    assertFalse(StrUtils.parseBool("off   ", true));
    assertFalse(StrUtils.parseBool("offAbc", true));

    assertFalse(StrUtils.parseBool("no", true));
    assertFalse(StrUtils.parseBool("NO", true));
    assertFalse(StrUtils.parseBool("foo", false));
    assertFalse(StrUtils.parseBool("", false));
    assertFalse(StrUtils.parseBool(null, false));
  }

  public void testJoin() {
    assertEquals("a|b|c", StrUtils.join(asList("a", "b", "c"), '|'));
    assertEquals("a,b,c", StrUtils.join(asList("a", "b", "c"), ','));
    assertEquals("a\\,b,c", StrUtils.join(asList("a,b", "c"), ','));
    assertEquals("a,b|c", StrUtils.join(asList("a,b", "c"), '|'));

    assertEquals("a\\\\b|c", StrUtils.join(asList("a\\b", "c"), '|'));
  }

  public void testEscapeTextWithSeparator() {
    assertEquals("a", StrUtils.escapeTextWithSeparator("a", '|'));
    assertEquals("a", StrUtils.escapeTextWithSeparator("a", ','));

    assertEquals("a\\|b", StrUtils.escapeTextWithSeparator("a|b", '|'));
    assertEquals("a|b", StrUtils.escapeTextWithSeparator("a|b", ','));
    assertEquals("a,b", StrUtils.escapeTextWithSeparator("a,b", '|'));
    assertEquals("a\\,b", StrUtils.escapeTextWithSeparator("a,b", ','));
    assertEquals("a\\\\b", StrUtils.escapeTextWithSeparator("a\\b", ','));

    assertEquals("a\\\\\\,b", StrUtils.escapeTextWithSeparator("a\\,b", ','));
  }

  public void testSplitEscaping() {
    List<String> arr = StrUtils.splitSmart("\\r\\n:\\t\\f\\b", ":", true);
    assertEquals(2, arr.size());
    assertEquals("\r\n", arr.get(0));
    assertEquals("\t\f\b", arr.get(1));

    arr = StrUtils.splitSmart("\\r\\n:\\t\\f\\b", ":", false);
    assertEquals(2, arr.size());
    assertEquals("\\r\\n", arr.get(0));
    assertEquals("\\t\\f\\b", arr.get(1));

    arr = StrUtils.splitWS("\\r\\n \\t\\f\\b", true);
    assertEquals(2, arr.size());
    assertEquals("\r\n", arr.get(0));
    assertEquals("\t\f\b", arr.get(1));

    arr = StrUtils.splitWS("\\r\\n \\t\\f\\b", false);
    assertEquals(2, arr.size());
    assertEquals("\\r\\n", arr.get(0));
    assertEquals("\\t\\f\\b", arr.get(1));

    arr = StrUtils.splitSmart("\\:foo\\::\\:bar\\:", ":", true);
    assertEquals(2, arr.size());
    assertEquals(":foo:", arr.get(0));
    assertEquals(":bar:", arr.get(1));

    arr = StrUtils.splitWS("\\ foo\\  \\ bar\\ ", true);
    assertEquals(2, arr.size());
    assertEquals(" foo ", arr.get(0));
    assertEquals(" bar ", arr.get(1));

    arr = StrUtils.splitFileNames("/h/s,/h/\\,s,");
    assertEquals(2, arr.size());
    assertEquals("/h/s", arr.get(0));
    assertEquals("/h/,s", arr.get(1));

    arr = StrUtils.splitFileNames("/h/s");
    assertEquals(1, arr.size());
    assertEquals("/h/s", arr.get(0));
  }

  public void testToLower() {
    assertEquals(List.of(), StrUtils.toLower(List.of()));
    assertEquals(List.of(""), StrUtils.toLower(List.of("")));
    assertEquals(List.of("foo"), StrUtils.toLower(List.of("foo")));
    assertEquals(List.of("bar", "baz-123"), StrUtils.toLower(List.of("BAR", "Baz-123")));
  }
}
