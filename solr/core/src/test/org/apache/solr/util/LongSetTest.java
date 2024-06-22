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

import static com.carrotsearch.hppc.HashContainers.MIN_HASH_ARRAY_LENGTH;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

public class LongSetTest extends SolrTestCase {

  @Test
  public void testZeroInitialCapacity() {
    final LongHashSet ls = new LongHashSet(0);
    assertEquals(0, ls.size());
    assertEquals(MIN_HASH_ARRAY_LENGTH, ls.keys.length - 1); // -1 to correct for empty slot
    assertFalse(ls.contains(0));
    assertFalse(ls.iterator().hasNext());

    final HashSet<Long> hs = new HashSet<>();
    for (long jj = 1; jj <= 10; ++jj) {
      assertTrue(ls.add(jj));
      assertFalse(ls.add(jj));
      assertEquals(jj, ls.size());
      assertTrue(hs.add(jj));
      assertFalse(hs.add(jj));
    }

    for (LongCursor c : ls) {
      hs.remove(c.value);
    }
    assertTrue(hs.isEmpty());

    assertEquals(10, ls.size());
    assertEquals(16, ls.keys.length - 1); // -1 to correct for empty slot
  }

  @Test
  public void testAddZero() {
    final LongSet ls = new LongHashSet(1);
    assertEquals(0, ls.size());
    assertFalse(ls.contains(0));
    assertFalse(ls.iterator().hasNext());

    assertTrue(ls.add(0L));
    assertTrue(ls.contains(0));
    assertFalse(ls.add(0L));
    assertTrue(ls.contains(0));

    final Iterator<LongCursor> it = ls.iterator();
    assertTrue(it.hasNext());
    assertEquals(0L, it.next().value);
    assertFalse(it.hasNext());
  }

  @Test
  public void testIterating() {
    final LongSet ls = new LongHashSet(4);
    assertTrue(ls.add(0L));
    assertTrue(ls.add(6L));
    assertTrue(ls.add(7L));
    assertTrue(ls.add(42L));

    final Iterator<LongCursor> it = ls.iterator();
    // non-zero values are returned first
    assertTrue(it.hasNext());
    assertNotEquals(0L, it.next().value);
    assertTrue(it.hasNext());
    assertNotEquals(0L, it.next().value);
    assertTrue(it.hasNext());
    assertNotEquals(0L, it.next().value);

    // and zero value (if any) is returned last
    assertTrue(it.hasNext());
    assertEquals(0L, it.next().value);
    assertFalse(it.hasNext());
  }
}
