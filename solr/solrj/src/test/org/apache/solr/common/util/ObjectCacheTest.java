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

import java.io.Closeable;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

/** Tests for {@link ObjectCache}. */
public class ObjectCacheTest extends SolrTestCase {

  private ObjectCache objectCache;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    objectCache = new ObjectCache();
  }

  @Override
  public void tearDown() throws Exception {
    objectCache.close();
    super.tearDown();
  }

  @Test
  public void testGetPutRemove() {
    assertNull(objectCache.get("key"));

    objectCache.put("key", "value");
    assertEquals("value", objectCache.get("key"));

    objectCache.remove("key");
    assertNull(objectCache.get("key"));
  }

  @Test
  public void testClear() {
    objectCache.put("key1", "value1");
    objectCache.put("key2", "value2");

    objectCache.clear();
    assertNull(objectCache.get("key1"));
    assertNull(objectCache.get("key2"));
  }

  @Test
  public void testGetTypeSafe() {
    objectCache.put("string", "a string");
    objectCache.put("integer", 42);

    assertEquals("a string", objectCache.get("string", String.class));
    assertEquals((Integer) 42, objectCache.get("integer", Integer.class));
  }

  @Test
  public void testComputeIfAbsentTypeSafe() {
    String returnValue = objectCache.computeIfAbsent("string", String.class, k -> "a string");
    assertEquals("a string", returnValue);
    assertEquals("a string", objectCache.get("string"));

    returnValue = objectCache.computeIfAbsent("string", String.class, k -> "another string");
    assertEquals("a string", returnValue);
    assertEquals("a string", objectCache.get("string"));
  }

  @Test
  public void testClose() throws Exception {
    assertFalse(objectCache.isClosed());
    objectCache.close();
    assertTrue(objectCache.isClosed());
  }

  @Test
  public void testCloseCloseableValues() throws Exception {
    MyCloseable object1 = new MyCloseable();
    MyCloseable object2 = new MyCloseable();
    objectCache.put("object1", object1);
    objectCache.put("object2", object2);
    objectCache.put("string", "a string");
    objectCache.close();

    assertTrue(object1.closed);
    assertTrue(object2.closed);
  }

  private static class MyCloseable implements Closeable {
    private boolean closed = false;

    @Override
    public void close() {
      closed = true;
    }
  }
}
