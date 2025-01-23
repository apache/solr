package org.apache.solr.common.util;

import java.util.Collection;
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
