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

package org.apache.solr.cloud.api.collections;

import static org.apache.solr.cloud.api.collections.TimeRoutedAlias.ROUTER_START;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.cloud.Aliases;
import org.junit.Test;

/** Tests {@link TimeRoutedAlias#fixStart}, which fixes a date math router.start to an instant. */
public class TimeRoutedAliasFixStartTest extends SolrTestCase {

  private static final String ALIAS = "tra";
  private static final String DATE_MATH = "NOW/DAY";
  private static final String FIXED = "2026-10-30T00:00:00Z";

  private static Aliases aliasesWithStart(String start) {
    return Aliases.EMPTY
        .cloneWithCollectionAlias(ALIAS, "tra__TRA__2026-10-30")
        .cloneWithCollectionAliasProperties(ALIAS, ROUTER_START, start);
  }

  @Test
  public void testFixesDateMathStart() {
    Aliases fixed = TimeRoutedAlias.fixStart(aliasesWithStart(DATE_MATH), ALIAS, DATE_MATH, FIXED);
    assertEquals(FIXED, fixed.getCollectionAliasProperties(ALIAS).get(ROUTER_START));
  }

  @Test
  public void testKeepsOtherAliasesAndProperties() {
    Aliases aliases =
        aliasesWithStart(DATE_MATH)
            .cloneWithCollectionAlias("other", "collection1")
            .cloneWithCollectionAliasProperties(ALIAS, "foo", "bar");
    Aliases fixed = TimeRoutedAlias.fixStart(aliases, ALIAS, DATE_MATH, FIXED);
    assertEquals("collection1", fixed.getCollectionAliasMap().get("other"));
    assertEquals("bar", fixed.getCollectionAliasProperties(ALIAS).get("foo"));
    assertEquals(FIXED, fixed.getCollectionAliasProperties(ALIAS).get(ROUTER_START));
  }

  @Test
  public void testDoesNotChangeAnAlreadyFixedStart() {
    // e.g. a node whose cached aliases were stale computed a later start
    Aliases aliases = aliasesWithStart(FIXED);
    assertSame(
        aliases, TimeRoutedAlias.fixStart(aliases, ALIAS, DATE_MATH, "2026-10-31T00:00:00Z"));
  }

  @Test
  public void testDoesNotRecreateADeletedAlias() {
    Aliases aliases = Aliases.EMPTY.cloneWithCollectionAlias("other", "collection1");
    assertSame(aliases, TimeRoutedAlias.fixStart(aliases, ALIAS, DATE_MATH, FIXED));
  }
}
