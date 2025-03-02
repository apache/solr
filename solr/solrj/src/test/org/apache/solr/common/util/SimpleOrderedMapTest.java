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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

public class SimpleOrderedMapTest extends SolrTestCase {

  private final SimpleOrderedMap<Integer> map = new SimpleOrderedMap<>();

  @Test
  public void testPut() {
    map.put("one", 1);
    map.put("two", 2);

    assertEquals(4, map.nvPairs.size());
    assertEquals("one", map.nvPairs.get(0));
    assertEquals(1, map.nvPairs.get(1));
    assertEquals("two", map.nvPairs.get(2));
    assertEquals(2, map.nvPairs.get(3));
  }

  @Test
  public void testEquals() {
    var map2 = new SimpleOrderedMap<Integer>();
    map2.put("one", 1);
    map2.put("two", 2);

    var map3 = map2.clone();
    assertNotSame(map2, map3);
    assertEquals(map2, map3);
    map3.put("three", 3);
    assertNotEquals(map2, map3);
    map3.remove("one");
    map3.remove("three");
    map3.add("one", 1); // now it's a different order
    assertNotEquals(map2.toString(), map3.toString());
    assertEquals(map2, map3); // but still equals despite different order
    assertEquals(map2.hashCode(), map3.hashCode());

    var nl2 = new NamedList<Object>(map2);
    assertNotEquals(map2, nl2);
    assertNotEquals(nl2, map2);
  }

  public void testToString() {
    SimpleOrderedMap<Object> map = new SimpleOrderedMap<>();
    map.add("one", 1);
    map.add("two", 2);
    map.add("aNull", null);
    assertEquals(new LinkedHashMap<>(map).toString(), map.toString());
  }

  public void testPutReturnOldValue() {
    map.put("one", 1);
    int oldValue = map.put("one", 2);

    assertEquals(oldValue, oldValue);
  }

  public void testPutReturnNullForNullValue() {
    Integer oldValue = map.put("one", null);

    assertNull(oldValue);
  }

  public void testPutReplaceExistingValue() {
    map.put("one", 1);
    map.put("one", 11);

    assertEquals(2, map.nvPairs.size());
    assertEquals("one", map.nvPairs.get(0));
    assertEquals(11, map.nvPairs.get(1));
  }

  @Test
  public void testContains() {
    setupData();

    assertTrue(map.containsKey("one"));
    assertTrue(map.containsKey("two"));
    assertTrue(map.containsKey("three"));
    assertFalse(map.containsKey("four"));
  }

  @Test
  public void testContainsNullAsKey() {
    map.put(null, 1);
    assertTrue(map.containsKey(null));
  }

  @Test
  public void testContainsNullAsKeyValuePair() {
    map.put(null, null);
    assertTrue(map.containsKey(null));
    assertTrue(map.containsValue(null));
  }

  /***
   * if the map contains a entry with null as value, contains(null) should be true as it is with other maps e.g. HashMap
   */
  @Test
  public void testContainsValueWithNull() {
    setupData();
    map.add("four", null);
    assertTrue(map.containsValue(null));
  }

  @Test
  public void testContainsValue() {
    setupData();
    assertTrue(map.containsValue(1));
    assertTrue(map.containsValue(2));
    assertTrue(map.containsValue(3));
    assertFalse(map.containsValue(9));
  }

  @Test
  public void testPutAll() {
    setupData();
    map.putAll(Map.of("four", 4, "five", 5, " six", 6));
    assertEquals(12, map.nvPairs.size());

    assertEquals("one", map.nvPairs.get(0));
    assertEquals(3, map.nvPairs.get(5));

    // since putAll takes a Map (unordered), we do not know the order of the elements
    assertTrue(map.nvPairs.contains("one"));
    assertTrue(map.nvPairs.contains(1));
    assertTrue(map.nvPairs.contains("three"));
    assertTrue(map.nvPairs.contains(3));
    assertTrue(map.nvPairs.contains(1));
  }

  @Test
  public void testKeySet() {
    setupData();
    Set<String> keys = map.keySet();
    assertEquals(3, keys.size());
    assertTrue(keys.contains("one"));
    assertTrue(keys.contains("three"));
    assertFalse(keys.contains("four"));
  }

  @Test
  public void testValues() {
    setupData();

    Collection<Integer> values = map.values();
    assertEquals(3, values.size());
    assertTrue(values.contains(1));
    assertTrue(values.contains(3));
    assertFalse(values.contains(4));
  }

  @Test
  public void entrySet() {
    setupData();

    assertEquals(3, map.entrySet().size());
    assertTrue(map.nvPairs.contains("one"));
    assertTrue(map.nvPairs.contains(1));
    assertTrue(map.nvPairs.contains("three"));
    assertTrue(map.nvPairs.contains(3));
  }

  @Test
  public void remove() {
    setupData();
    Integer two = map.remove("two");

    assertEquals(Integer.valueOf(2), two);
    assertEquals(4, map.nvPairs.size());
    assertFalse(map.containsKey("two"));
  }

  private void setupData() {
    map.add("one", 1);
    map.add("two", 2);
    map.add("three", 3);
  }
}
