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
package org.apache.solr.handler.component;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.handler.component.SearchRequestListenerManager.Delivery;
import org.junit.After;
import org.junit.Test;

public class SearchRequestListenerManagerTest extends SolrTestCase {

  private static final long DELIVERY_TIMEOUT_MS = 30_000;

  private static final long CLOSE_WAIT_CHECK_MS =
      SearchRequestListenerManager.CLOSE_TIMEOUT_MS / 10;

  private static final SearchRequestInfo INFO =
      new SearchRequestInfo(
          "/select", null, "core", null, null, 0, 0, null, null, 0, null, null, false);

  private final CountDownLatch started = new CountDownLatch(1);

  private final CountDownLatch release = new CountDownLatch(1);

  private SearchRequestListenerManager manager;

  /** Blocks the worker until {@link #release} counts down, ignoring interrupts. */
  private final SearchRequestListener blocker =
      (info, captured) -> {
        started.countDown();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DELIVERY_TIMEOUT_MS);
        while (release.getCount() > 0 && System.nanoTime() < deadline) {
          try {
            release.await(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
          } catch (InterruptedException ignored) {
          }
        }
      };

  private static class Recorder implements SearchRequestListener {
    final BlockingQueue<Object> delivered = new LinkedBlockingQueue<>();
    final BlockingQueue<Thread> threads = new LinkedBlockingQueue<>();

    @Override
    public void onRequestEnd(SearchRequestInfo info, Object captured) {
      threads.add(Thread.currentThread());
      delivered.add(captured);
    }

    Object next() throws InterruptedException {
      Object captured = delivered.poll(DELIVERY_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      assertNotNull("no event delivered", captured);
      return captured;
    }
  }

  @After
  public void closeManager() {
    release.countDown();
    if (manager != null) {
      manager.close();
    }
  }

  private void offer(Object captured, SearchRequestListener... listeners) {
    manager.offer(INFO, Arrays.stream(listeners).map(l -> new Delivery(l, captured)).toList());
  }

  private void blockWorker() throws InterruptedException {
    manager.add(blocker);
    offer("blocking", blocker);
    assertTrue(started.await(DELIVERY_TIMEOUT_MS, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testFullQueueDropsAndCounts() throws Exception {
    manager = new SearchRequestListenerManager(1);
    Recorder recorder = new Recorder();
    manager.add(recorder);
    blockWorker();
    offer("event2", recorder);
    offer("event3", recorder);
    release.countDown();
    assertEquals("event2", recorder.next());
    assertEquals(1, manager.lost());
    assertTrue(recorder.delivered.isEmpty());
  }

  @Test
  public void testOfferDoesNotBlockWhenFull() throws Exception {
    manager = new SearchRequestListenerManager(1);
    Recorder recorder = new Recorder();
    manager.add(recorder);
    blockWorker();
    offer("event2", recorder);
    ExecutorService executor =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("offer"));
    try {
      Future<?> full = executor.submit(() -> offer("event3", recorder));
      full.get(DELIVERY_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } finally {
      release.countDown();
      ExecutorUtil.shutdownAndAwaitTermination(executor);
    }
    assertEquals(1, manager.lost());
  }

  @Test
  public void testListenerErrorKeepsWorker() throws Exception {
    manager = new SearchRequestListenerManager(10);
    SearchRequestListener failing =
        (info, captured) -> {
          throw new AssertionError("listener error");
        };
    Recorder recorder = new Recorder();
    manager.add(failing);
    manager.add(recorder);
    offer("event1", failing, recorder);
    offer("event2", failing, recorder);
    assertEquals("event1", recorder.next());
    assertEquals("event2", recorder.next());
    Thread first = recorder.threads.take();
    assertSame(first, recorder.threads.take());
    assertNotSame(Thread.currentThread(), first);
  }

  @Test
  public void testStackOverflowInListenerKeepsWorker() throws Exception {
    manager = new SearchRequestListenerManager(10);
    SearchRequestListener failing =
        (info, captured) -> {
          throw new StackOverflowError("listener error");
        };
    Recorder recorder = new Recorder();
    manager.add(failing);
    manager.add(recorder);
    offer("event1", failing, recorder);
    offer("event2", failing, recorder);
    assertEquals("event1", recorder.next());
    assertEquals("event2", recorder.next());
  }

  @Test
  public void testDuplicateAddRegistersOnce() throws Exception {
    manager = new SearchRequestListenerManager(10);
    Recorder recorder = new Recorder();
    manager.add(recorder);
    manager.add(recorder);
    assertTrue(manager.remove(recorder));
    assertFalse(manager.hasListeners());
  }

  @Test
  public void testInterruptFromListenerKeepsWorker() throws Exception {
    manager = new SearchRequestListenerManager(10);
    SearchRequestListener interrupting = (info, captured) -> Thread.currentThread().interrupt();
    Recorder recorder = new Recorder();
    manager.add(interrupting);
    manager.add(recorder);
    offer("event1", interrupting, recorder);
    assertEquals("event1", recorder.next());
    offer("event2", interrupting, recorder);
    assertEquals("event2", recorder.next());
  }

  @Test
  public void testCloseDuringEventCountsIt() throws Exception {
    manager = new SearchRequestListenerManager(10);
    Recorder recorder = new Recorder();
    manager.add(blocker);
    manager.add(recorder);
    offer("event1", blocker, recorder);
    assertTrue(started.await(DELIVERY_TIMEOUT_MS, TimeUnit.MILLISECONDS));
    Thread worker = manager.worker();
    manager.close();
    release.countDown();
    worker.join(DELIVERY_TIMEOUT_MS);
    assertFalse(worker.isAlive());
    assertTrue(recorder.delivered.isEmpty());
    assertEquals(1, manager.lost());
  }

  @Test
  public void testCloseWaitsForListenerInProgress() throws Exception {
    manager = new SearchRequestListenerManager(10);
    blockWorker();
    Thread worker = manager.worker();
    ExecutorService executor =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("close"));
    try {
      Future<?> closing = executor.submit(manager::close);
      expectThrows(
          TimeoutException.class, () -> closing.get(CLOSE_WAIT_CHECK_MS, TimeUnit.MILLISECONDS));
      release.countDown();
      closing.get(DELIVERY_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } finally {
      release.countDown();
      ExecutorUtil.shutdownAndAwaitTermination(executor);
    }
    assertFalse(worker.isAlive());
  }

  @Test
  public void testRemovedListenerNotCalledForQueuedEvent() throws Exception {
    manager = new SearchRequestListenerManager(10);
    Recorder kept = new Recorder();
    Recorder removed = new Recorder();
    manager.add(kept);
    manager.add(removed);
    blockWorker();
    offer("event2", kept, removed);
    assertTrue(manager.remove(removed));
    release.countDown();
    assertEquals("event2", kept.next());
    assertTrue(removed.delivered.isEmpty());
  }

  @Test
  public void testCloseDiscardsAndRejectsAdd() throws Exception {
    manager = new SearchRequestListenerManager(10);
    Recorder recorder = new Recorder();
    manager.add(recorder);
    blockWorker();
    offer("event2", recorder);
    Thread worker = manager.worker();
    assertNotNull(worker);
    long start = System.nanoTime();
    manager.close();
    long closeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    assertTrue(
        "close took " + closeMs, closeMs < SearchRequestListenerManager.CLOSE_TIMEOUT_MS * 2);
    release.countDown();
    worker.join(DELIVERY_TIMEOUT_MS);
    assertFalse(worker.isAlive());
    assertTrue(recorder.delivered.isEmpty());
    assertEquals(1, manager.lost());
    offer("event3", recorder);
    assertEquals(2, manager.lost());
    expectThrows(IllegalStateException.class, () -> manager.add(new Recorder()));
  }
}
