package org.apache.solr.common.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class SimpleOrderedMapTest extends Assert
{
	private SimpleOrderedMap<Integer> map = new SimpleOrderedMap<>();

	@Test
	public void testPut() {
		map.put("one", 1);
		map.put("two", 2);              
		
		assertEquals(4, map.nvPairs.size());
		assertEquals("one", map.nvPairs.get(0));
		assertEquals(1, map.nvPairs.get(1));
		assertEquals("two", map.nvPairs.get(2));
		assertEquals(2, map.nvPairs.get(3));

		Object one =  map.put("one", -1);
		assertEquals(1,one);
	}

	@Test
	public void testContains()
	{
		setupData();
		assertTrue(map.containsKey("one"));
		assertTrue(map.containsKey("two"));
		assertTrue(map.containsKey("three"));
		assertFalse(map.containsKey("four"));
	}

	@Test
	public void testContainsWithNull() {
		setupData();
		assertFalse(map.containsKey(null));

	}
	

	@Test
	public void testContainsValue()
	{
		setupData();
		assertTrue(map.containsValue(1));
		assertTrue(map.containsValue(2));
		assertTrue(map.containsValue(3));
		assertFalse(map.containsValue(9));
	}
	
	@Test
	public void putAll(){
		
		map.putAll(Map.of("one", 1, "two", 2, "three", 3));
		assertEquals(6, map.nvPairs.size());
		assertTrue( map.nvPairs.contains("one"));
		assertTrue( map.nvPairs.contains(1));
		assertTrue( map.nvPairs.contains("three"));
		assertTrue( map.nvPairs.contains(3));
		/*assertTrue( map.nvPairs.contains(1));
		assertEquals("one", map.nvPairs.get(0));
		assertEquals(1, map.nvPairs.get(1));
		assertEquals("three", map.nvPairs.get(4));
		assertEquals(3, map.nvPairs.get(5));*/
		
	}
	
	@Test
	public void testKeySet(){
		setupData();
		assertEquals(3, map.keySet().size());
		assertTrue(map.keySet().contains("one"));
		assertTrue(map.keySet().contains("three"));
		assertFalse(map.keySet().contains("four"));
	}
	
	@Test
	public void testValues(){
		setupData();
		assertEquals(3, map.values().size());
		assertTrue(map.values().contains(1));
		assertTrue(map.values().contains(3));
		assertFalse(map.values().contains(4));
	}
	
	@Test
	public void entrySet(){
		setupData();
		assertEquals(3, map.entrySet().size());
		
		
		assertTrue(map.nvPairs.contains("one"));
		assertTrue(map.nvPairs.contains(1));
		assertTrue(map.nvPairs.contains("three"));
		assertTrue(map.nvPairs.contains(3));
	}
	
	
	@Test
	public void testGet()
	{
		/*setupData();

		Integer i1 = map.get((Object) "one");
		assertEquals(1, (String ) i1);
		
		assertTrue(map.get((Object)"one"));
		assertTrue(map.containsValue(2));
		assertTrue(map.containsValue(3));
		assertFalse(map.containsValue(9));*/
	}
	
	@Test
	public void remove(){
		
		
	}
	

	private void setupData()
	{
		map.add("one", 1);
		map.add("two", 2);
		map.add("three", 3);

	}


}
