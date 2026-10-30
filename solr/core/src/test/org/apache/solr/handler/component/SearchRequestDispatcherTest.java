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
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.handler.component.SearchRequestDispatcher.Delivery;
import org.junit.After;
import org.junit.Test;

public class SearchRequestDispatcherTest extends SolrTestCase {

  private static final long DELIVERY_TIMEOUT_MS = 30_000;

  private static final SearchRequestInfo INFO =
      new SearchRequestInfo(
          "/select", null, "core", null, null, 0, 0, null, null, 0, null, null, false);

  private final CountDownLatch started = new CountDownLatch(1);

  private final CountDownLatch release = new CountDownLatch(1);

  private SearchRequestDispatcher dispatcher;

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
  public void closeDispatcher() {
    release.countDown();
    if (dispatcher != null) {
      dispatcher.close();
    }
  }

  private void offer(Object captured, SearchRequestListener... listeners) {
    dispatcher.offer(
        INFO,
        Arrays.stream(listeners).map(l -> new Delivery(l, captured)).toList());
  }

  private void blockWorker() throws InterruptedException {
    dispatcher.add(blocker);
    offer("blocking", blocker);
    assertTrue(started.await(DELIVERY_TIMEOUT_MS, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testFullQueueDropsAndCounts() throws Exception {
    dispatcher = new SearchRequestDispatcher(1);
    Recorder recorder = new Recorder();
    dispatcher.add(recorder);
    blockWorker();
    offer("event2", recorder);
    offer("event3", recorder);
    release.countDown();
    assertEquals("event2", recorder.next());
    assertEquals(1, dispatcher.lost());
    assertTrue(recorder.delivered.isEmpty());
  }

  @Test
  public void testOfferDoesNotBlockWhenFull() throws Exception {
    dispatcher = new SearchRequestDispatcher(1);
    Recorder recorder = new Recorder();
    dispatcher.add(recorder);
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
    assertEquals(1, dispatcher.lost());
  }

  @Test
  public void testListenerErrorKeepsWorker() throws Exception {
    dispatcher = new SearchRequestDispatcher(10);
    SearchRequestListener failing =
        (info, captured) -> {
          throw new AssertionError("listener error");
        };
    Recorder recorder = new Recorder();
    dispatcher.add(failing);
    dispatcher.add(recorder);
    offer("event1", failing, recorder);
    offer("event2", failing, recorder);
    assertEquals("event1", recorder.next());
    assertEquals("event2", recorder.next());
    Thread first = recorder.threads.take();
    assertSame(first, recorder.threads.take());
    assertNotSame(Thread.currentThread(), first);
  }

  @Test
  public void testRemovedListenerNotCalledForQueuedEvent() throws Exception {
    dispatcher = new SearchRequestDispatcher(10);
    Recorder kept = new Recorder();
    Recorder removed = new Recorder();
    dispatcher.add(kept);
    dispatcher.add(removed);
    blockWorker();
    offer("event2", kept, removed);
    assertTrue(dispatcher.remove(removed));
    release.countDown();
    assertEquals("event2", kept.next());
    assertTrue(removed.delivered.isEmpty());
  }

  @Test
  public void testCloseDiscardsAndRejectsAdd() throws Exception {
    dispatcher = new SearchRequestDispatcher(10);
    Recorder recorder = new Recorder();
    dispatcher.add(recorder);
    blockWorker();
    offer("event2", recorder);
    Thread worker = dispatcher.worker();
    assertNotNull(worker);
    long start = System.nanoTime();
    dispatcher.close();
    long closeMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    assertTrue("close took " + closeMs, closeMs < SearchRequestDispatcher.CLOSE_TIMEOUT_MS * 2);
    release.countDown();
    worker.join(DELIVERY_TIMEOUT_MS);
    assertFalse(worker.isAlive());
    assertTrue(recorder.delivered.isEmpty());
    assertEquals(1, dispatcher.lost());
    expectThrows(IllegalStateException.class, () -> dispatcher.add(new Recorder()));
  }
}
