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

import com.carrotsearch.randomizedtesting.annotations.Timeout;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.solr.SolrTestCase;
import org.apache.solr.util.TimeOut;
import org.junit.Test;

public class ExecutorUtilTest extends SolrTestCase {

  private static final long MAX_AWAIT_TERMINATION_ARG_MS = 100;

  /**
   * The maximum amount of time we're willing to let the test wait for any type of blocking action,
   * no matter ow slow our CPU is. Any thing that exceeds this time is presumably a bug
   */
  private static final long MAX_SANE_WAIT_DURRATION_MS = 2_000;

  /** Test that if there is a non interruptable thread that awaitTermination eventually returns. */
  @Test
  // Must prevent runaway failures so limit this to short timeframe in case of failure
  @Timeout(millis = 5_000)
  public void testExecutorUtilAwaitsTerminationWhenTaskIgnoresInterupt() throws Exception {

    final Worker w = new Worker(false);
    final ExecutorService executorService =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(
            new SolrNamedThreadFactory(this.getClass().getSimpleName() + "non-interruptable"));
    try {
      final Future<Boolean> f = executorService.submit(w);

      assertTrue("Worker didn't start in a sane amount of time", w.awaitWorkerStart());
      executorService.shutdownNow();
      assertTrue(
          "Worker not interupted by shutdown in a sane amount of time",
          w.awaitWorkerInteruptedAtLeastOnce());
      assertThrows(
          RuntimeException.class,
          () ->
              ExecutorUtil.awaitTermination(
                  executorService, MAX_AWAIT_TERMINATION_ARG_MS, TimeUnit.MILLISECONDS));

      assertFalse("Worker should not finish due to shutdown or awaitTermination", f.isDone());
      assertTrue("test sanity check: WTF? how did we get here?", w.getNumberOfInterupts() > 0);

      // Worker should finish if we let it
      w.tellWorkerToFinish();
      assertTrue(f.get(MAX_SANE_WAIT_DURRATION_MS, TimeUnit.MILLISECONDS));
    } finally {
      w.tellWorkerToFinish();
      ExecutorUtil.shutdownNowAndAwaitTermination(executorService);
    }
  }

  /** Test that if there is an interruptable thread that awaitTermination forcefully returns. */
  @Test
  // Must prevent runaway failures so limit this to short timeframe in case of failure
  @Timeout(millis = 5_000)
  public void testExecutorUtilAwaitsTerminationWhenTaskRespectsInterupt() throws Exception {

    final Worker w = new Worker(true);
    final ExecutorService executorService =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(
            new SolrNamedThreadFactory(this.getClass().getSimpleName() + "interruptable"));
    try {
      final Future<Boolean> f = executorService.submit(w);

      assertTrue("Worker didn't start in a sane amount of time", w.awaitWorkerStart());
      executorService.shutdownNow();
      ExecutorUtil.awaitTermination(
          executorService, MAX_AWAIT_TERMINATION_ARG_MS, TimeUnit.MILLISECONDS);

      // Worker should finish on it's own after the interupt
      assertTrue(
          "Worker not interupted in a sane amount of time", w.awaitWorkerInteruptedAtLeastOnce());
      assertFalse(f.get(MAX_SANE_WAIT_DURRATION_MS, TimeUnit.MILLISECONDS));
      assertTrue("test sanity check: WTF? how did we get here?", w.getNumberOfInterupts() > 0);

    } finally {
      w.tellWorkerToFinish();
      ExecutorUtil.shutdownNowAndAwaitTermination(executorService);
    }
  }

  private static final class Worker implements Callable<Boolean> {
    // how we communiate out to our caller
    private final CountDownLatch taskStartedLatch = new CountDownLatch(1);
    private final CountDownLatch gotFirstInteruptLatch = new CountDownLatch(1);
    private final AtomicInteger interruptCount = new AtomicInteger(0);

    // how our caller communicates with us
    private final CountDownLatch allowedToFinishLatch = new CountDownLatch(1);
    private final boolean interruptable;

    public Worker(final boolean interruptable) {
      this.interruptable = interruptable;
    }

    /** Returns false if worker doesn't start in a sane amount of time */
    public boolean awaitWorkerStart() throws InterruptedException {
      return taskStartedLatch.await(MAX_SANE_WAIT_DURRATION_MS, TimeUnit.MILLISECONDS);
    }

    /** Returns false if worker didn't recieve interupt in a sane amount of time */
    public boolean awaitWorkerInteruptedAtLeastOnce() throws InterruptedException {
      return gotFirstInteruptLatch.await(MAX_SANE_WAIT_DURRATION_MS, TimeUnit.MILLISECONDS);
    }

    public int getNumberOfInterupts() {
      return interruptCount.get();
    }

    public void tellWorkerToFinish() {
      allowedToFinishLatch.countDown();
    }

    @Override
    public Boolean call() {
      // aboslute last resort timeout to prevent infinite while loop
      final TimeOut threadTimeout =
          new TimeOut(MAX_SANE_WAIT_DURRATION_MS, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);

      while (!threadTimeout.hasTimedOut()) {
        try {

          // this must be inside the try block, so we'll still catch the InterruptedException if our
          // caller shutsdown & awaits termination before we get a chance to start await'ing...
          taskStartedLatch.countDown();

          if (allowedToFinishLatch.await(
              threadTimeout.timeLeft(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)) {
            return true; // ran until we were told to stop
          }
        } catch (InterruptedException interruptedException) {
          interruptCount.incrementAndGet();
          gotFirstInteruptLatch.countDown();
          if (interruptable) {
            Thread.currentThread().interrupt();
            return false; // was explicitly interupted
          }
        }
      }
      throw new RuntimeException("Sane timeout elapsed before worker finished");
    }
  }

  @Test
  public void submitAllTest() throws IOException {
    AtomicLong idx = new AtomicLong();
    Callable<Long> c = () -> idx.getAndIncrement();

    List<Long> results = new ArrayList<>();
    ExecutorService service =
        ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("test"));
    try {
      List<Callable<Long>> tasks = List.of(c, c, c, c, c);
      results.addAll(ExecutorUtil.submitAllAndAwaitAggregatingExceptions(service, tasks));
    } finally {
      ExecutorUtil.shutdownNowAndAwaitTermination(service);
    }
    Collections.sort(results);
    List<Long> expected = List.of(0l, 1l, 2l, 3l, 4l);
    assertEquals(expected, results);
  }

  @Test
  public void submitAllWithExceptionsTest() {
    AtomicLong idx = new AtomicLong();
    Callable<Long> c =
        () -> {
          long id = idx.getAndIncrement();
          if (id % 2 == 0) {
            throw new Exception("TestException" + id);
          }
          return id;
        };

    ExecutorService service =
        ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("test"));
    try {
      List<Callable<Long>> tasks = List.of(c, c, c, c, c);
      IOException ex =
          expectThrows(
              IOException.class,
              () -> ExecutorUtil.submitAllAndAwaitAggregatingExceptions(service, tasks));
      List<String> results = new ArrayList<>();
      results.add(ex.getCause().getMessage());
      for (var s : ex.getSuppressed()) {
        results.add(s.getMessage());
      }
      Collections.sort(results);
      List<String> expected = List.of("TestException0", "TestException2", "TestException4");
      assertEquals(expected, results);
    } finally {
      ExecutorUtil.shutdownNowAndAwaitTermination(service);
    }
  }
}
