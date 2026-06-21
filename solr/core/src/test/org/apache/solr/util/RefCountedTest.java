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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.AlreadyClosedException;
import org.junit.Test;

/**
 * Guards the reference-counting invariants of {@link RefCounted}. A regression here (e.g. an
 * incref() that increments more than once) silently leaks every ref-counted resource — most
 * importantly {@link org.apache.solr.search.SolrIndexSearcher} — because the count never returns
 * to zero and {@code close()} is never called.
 */
public class RefCountedTest extends SolrTestCase {

  private static final class Counter extends RefCounted<Object> {
    final AtomicInteger closes = new AtomicInteger();

    Counter() {
      super(new Object());
      incref(); // by convention a freshly created holder is handed out with refcount == 1
    }

    @Override
    protected void close() {
      closes.incrementAndGet();
    }
  }

  @Test
  public void testIncrefIncrementsByExactlyOne() {
    Counter c = new Counter();
    assertEquals("fresh holder must have refcount 1", 1, c.getRefcount());
    c.incref();
    assertEquals("each incref must add exactly 1", 2, c.getRefcount());
    c.incref();
    assertEquals(3, c.getRefcount());
  }

  @Test
  public void testBalancedIncrefDecrefDoesNotDrift() {
    Counter c = new Counter();
    // Many balanced acquire/release cycles must leave the count exactly where it started and must
    // never close the resource. (The historical double-increment bug drifted +1 per cycle.)
    for (int i = 0; i < 1000; i++) {
      c.incref();
      c.decref();
      assertEquals("balanced incref/decref must not drift", 1, c.getRefcount());
    }
    assertEquals("resource must not be closed while still referenced", 0, c.closes.get());
  }

  @Test
  public void testCloseFiresExactlyOnceAtZero() {
    Counter c = new Counter(); // refcount 1
    c.incref(); // 2
    c.decref(); // 1
    assertEquals(0, c.closes.get());
    c.decref(); // 0 -> close
    assertEquals("close must fire exactly once when the count reaches zero", 1, c.closes.get());
  }

  @Test
  public void testIncrefAfterCloseThrows() {
    Counter c = new Counter(); // refcount 1
    c.decref(); // -> closed
    assertEquals(1, c.closes.get());
    boolean threw = false;
    try {
      c.incref();
    } catch (AlreadyClosedException expected) {
      threw = true;
    }
    assertTrue("incref() after close must throw AlreadyClosedException", threw);
  }
}
