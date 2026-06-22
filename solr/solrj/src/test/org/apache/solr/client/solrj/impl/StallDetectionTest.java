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
package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.ParWork;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StallDetectionTest extends SolrTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void testNoStallWithEmptyQueue() throws IOException {

    long stallTimeMillis = 100;
    FakeTimeSource timeSource = new FakeTimeSource();
    StallDetection stallDetection = new StallDetection(stallTimeMillis, () -> 0, timeSource);

    // This should not throw an exception because the queue is empty
    stallDetection.stallCheck();

    // Advance time past the stall time
    timeSource.advanceMillis(stallTimeMillis + 10);

    // This should still not throw an exception because the queue is empty
    stallDetection.stallCheck();
  }

  @Test
  public void testNoStallWithProgressingQueue() throws IOException {

    long stallTimeMillis = 100;
    FakeTimeSource timeSource = new FakeTimeSource();
    StallDetection stallDetection = new StallDetection(stallTimeMillis, () -> 1, timeSource);

    // Call stallCheck once to initialize the timer
    stallDetection.stallCheck();

    // Advance time but not enough to trigger a stall
    timeSource.advanceMillis(stallTimeMillis / 2);

    // Increment the processed count to simulate progress
    stallDetection.incrementProcessedCount();

    // This should not throw an exception because progress was made
    stallDetection.stallCheck();

    // Advance time past the stall time
    timeSource.advanceMillis(stallTimeMillis + 10);

    // This still should not throw because the timer was reset during the last check
    stallDetection.stallCheck();
  }

  @Test
  public void testStallDetection() throws IOException {

    long stallTimeMillis = 5;

    FakeTimeSource timeSource = new FakeTimeSource();

    StallDetection stallDetection = new StallDetection(stallTimeMillis, () -> 1, timeSource);

    // Call stallCheck once to initialize the timer
    stallDetection.stallCheck();

    // Advance time past the stall time
    timeSource.advanceMillis(stallTimeMillis + 1);

    // This should throw an IOException
    IOException exception = LuceneTestCase.expectThrows(IOException.class, stallDetection::stallCheck);
    assertTrue(
        "Exception message should mention stall time",
        exception.getMessage().contains("Request processing has stalled for"));
    assertTrue(
        "Exception message should mention queue size",
        exception.getMessage().contains("remaining elements in the queue"));
  }

  @Test
  public void testResetStallTimer() throws IOException {

    long stallTimeMillis = 15;

    FakeTimeSource timeSource = new FakeTimeSource();

    StallDetection stallDetection = new StallDetection(stallTimeMillis, () -> 1, timeSource);

    // Call stallCheck once to initialize the timer
    stallDetection.stallCheck();

    // Advance time, but not enough to trigger a stall
    timeSource.advanceMillis(5);

    // Reset the timer
    stallDetection.resetStallTimer();

    // Call stallCheck again to reinitialize the timer
    stallDetection.stallCheck();

    // Advance time, but not enough to trigger a stall
    timeSource.advanceMillis(5);

    // This should not throw an exception because we reset the timer
    stallDetection.stallCheck();

    // Advance time past the stall time
    timeSource.advanceMillis(stallTimeMillis + 1);

    // Now it should throw an exception
    LuceneTestCase.expectThrows(IOException.class, stallDetection::stallCheck);
  }

  @Test
  public void testDynamicQueueSize() throws IOException {

    long stallTimeMillis = 15;
    AtomicInteger queueSize = new AtomicInteger(1);
    FakeTimeSource timeSource = new FakeTimeSource();
    StallDetection stallDetection = new StallDetection(stallTimeMillis, queueSize::get, timeSource);

    // Call stallCheck once to initialize the timer
    stallDetection.stallCheck();

    // Advance time but not enough to trigger a stall
    timeSource.advanceMillis(stallTimeMillis / 2);

    queueSize.set(0);

    // This should not throw an exception and should reset the timer
    stallDetection.stallCheck();

    // Advance time past the stall time
    timeSource.advanceMillis(stallTimeMillis * 2);

    queueSize.set(1);

    // This should not throw an exception because the timer was reset when queue was empty
    stallDetection.stallCheck();

    // Advance time past the stall time
    timeSource.advanceMillis(stallTimeMillis + 5);

    // Now it should throw an exception because queue is non-empty and no progress was made
    LuceneTestCase.expectThrows(IOException.class, stallDetection::stallCheck);
  }

  @Test
  public void testGetProcessedCount() {
    FakeTimeSource timeSource = new FakeTimeSource();
    StallDetection stallDetection = new StallDetection(100, () -> 1, timeSource);

    // Initially the processed count should be 0
    assertEquals(0, stallDetection.getProcessedCount());

    // Increment the processed count
    stallDetection.incrementProcessedCount();

    // Now the processed count should be 1
    assertEquals(1, stallDetection.getProcessedCount());

    // Increment multiple times
    for (int i = 0; i < 5; i++) {
      stallDetection.incrementProcessedCount();
    }

    // Now the processed count should be 6
    assertEquals(6, stallDetection.getProcessedCount());
  }

  @Test
  public void testGetStallTimeMillis() {
    long stallTimeMillis = 100;
    FakeTimeSource timeSource = new FakeTimeSource();
    StallDetection stallDetection = new StallDetection(stallTimeMillis, () -> 1, timeSource);

    // The stall time should be what we set
    assertEquals(stallTimeMillis, stallDetection.getStallTimeMillis());

    // Test with a different value
    long differentStallTime = 500;
    StallDetection anotherStallDetection =
        new StallDetection(differentStallTime, () -> 1, timeSource);
    assertEquals(differentStallTime, anotherStallDetection.getStallTimeMillis());
  }

  @Test
  public void testQueueEmptiesDuringStallCheck() throws IOException {

    long stallTimeMillis = 5;
    AtomicInteger queueSize = new AtomicInteger(1);

    FakeTimeSource timeSource = new FakeTimeSource();

    StallDetection stallDetection = new StallDetection(stallTimeMillis, queueSize::get, timeSource);

    // Call stallCheck once to initialize the timer
    stallDetection.stallCheck();

    // Advance time past the stall time
    timeSource.advanceMillis(stallTimeMillis + 1);

    // Change the queue size to 0 just before the next check
    queueSize.set(0);

    // This should not throw an exception because the queue is now empty
    stallDetection.stallCheck();

    // Change the queue size back to 1
    queueSize.set(1);

    // This should not throw an exception because the timer was reset when queue was empty
    stallDetection.stallCheck();

    // Advance time past the stall time again
    timeSource.advanceMillis(stallTimeMillis + 1);

    // Now it should throw an exception
    LuceneTestCase.expectThrows(IOException.class, stallDetection::stallCheck);
  }

  @Test
  public void testProgressResetsDuringStallCheck() throws IOException {

    long stallTimeMillis = 200;

    FakeTimeSource timeSource = new FakeTimeSource();

    final IntSupplier fixedQueueSupplier = () -> 1;
    StallDetection stallDetection =
        new StallDetection(stallTimeMillis, fixedQueueSupplier, timeSource);

    // First call - initialize timer
    stallDetection.stallCheck();

    // Advance time past the stall time
    timeSource.advanceMillis(stallTimeMillis + 50);

    // Make first progress
    stallDetection.incrementProcessedCount();

    // Second call - should not throw due to progress
    stallDetection.stallCheck();

    // Advance time past the stall time again
    timeSource.advanceMillis(stallTimeMillis + 50);

    // Third call - should NOT throw because timer was reset on second call
    try {
      stallDetection.stallCheck();
    } catch (IOException e) {
      fail("Unexpected exception thrown: " + e.getMessage());
    }
  }

  @Test
  public void testConcurrentAccess() throws Exception {
    // Models StallDetection's real production concurrency contract: MANY runner threads call
    // incrementProcessedCount() concurrently while EXACTLY ONE thread calls stallCheck()
    // periodically (see ConcurrentUpdateHttp2SolrClient, which has a single stall-checker).
    // The class is single-checker by design — lastProcessedCount/startStallTime are mutated
    // only by that one checker, so there is no shared-checker-state race. Running multiple
    // concurrent stall-checkers is NOT a supported pattern and is intentionally not exercised
    // (the previous version of this test did, and flaked: concurrent checkers consumed each
    // other's progress signal while collectively advancing the shared clock past the stall
    // threshold, producing a genuine — and correctly reported — stall).
    //
    // Invariant under test, asserted deterministically: while progress is being made, advancing
    // the clock PAST the stall threshold must never trigger a false stall. The checker advances
    // the clock only in an iteration where it has observed new progress in the SAME counter
    // stallCheck() consumes (getProcessedCount(), i.e. the LongAdder sum), and re-baselines from
    // that counter AFTER each stallCheck(). Because the baseline is always >= stallCheck()'s
    // internal lastProcessedCount and the sum is monotonic on the checker thread, the advancing
    // iteration is guaranteed to take stallCheck()'s progress-reset branch — so the stall timer is
    // never armed across an interval of advanced time and the check can never spuriously throw,
    // regardless of thread scheduling. (Gating on a SEPARATE proxy counter is incorrect: the
    // LongAdder sum lags an exact AtomicInteger under contention, so the checker would advance the
    // clock on an increment stallCheck() cannot yet see, producing a genuine — correctly reported —
    // stall. That is exactly what the previous version of this test did, and why it flaked.)
    final long stallTimeMillis = 1000;
    final FakeTimeSource timeSource = new FakeTimeSource();
    final AtomicInteger queueSize = new AtomicInteger(10); // queue stays non-empty all test
    final StallDetection stallDetection =
        new StallDetection(stallTimeMillis, queueSize::get, timeSource);

    // Initialize the timer
    stallDetection.stallCheck();

    final int incrementerCount = 8;
    final int incrementsPerThread = 1000;
    final int expectedTotal = incrementerCount * incrementsPerThread;

    // Start gate releases all incrementers plus the single checker simultaneously.
    final CyclicBarrier startGate = new CyclicBarrier(incrementerCount + 1);
    final CountDownLatch incrementersDone = new CountDownLatch(incrementerCount);
    final AtomicBoolean failed = new AtomicBoolean(false);
    final AtomicReference<Throwable> firstError = new AtomicReference<>();

    ExecutorService executor =
        ParWork.getExecutorService("StallDetectionTest", incrementerCount + 1, false);

    try {
      for (int i = 0; i < incrementerCount; i++) {
        executor.submit(
            () -> {
              try {
                startGate.await();
                for (int j = 0; j < incrementsPerThread; j++) {
                  stallDetection.incrementProcessedCount();
                  if ((j & 0x3f) == 0) {
                    Thread.yield(); // Hint to interleave with the checker
                  }
                }
              } catch (Throwable t) {
                failed.set(true);
                firstError.compareAndSet(null, t);
                log.error("Incrementer thread failed", t);
              } finally {
                incrementersDone.countDown();
              }
            });
      }

      // Exactly one stall-checker thread — the real production pattern.
      executor.submit(
          () -> {
            try {
              startGate.await();
              long baseline = stallDetection.getProcessedCount();
              // Check concurrently with the incrementers. Like the real production checker, this
              // thread yields between checks rather than busy-spinning, so it never starves the
              // incrementers. It stops once all incrementers are done (the final count assertion,
              // made after the executor terminates, sees every increment regardless).
              while (incrementersDone.getCount() > 0) {
                long now = stallDetection.getProcessedCount();
                if (now > baseline) {
                  // New progress visible in the same counter stallCheck() reads: jump the clock
                  // well past the stall threshold. The immediately following stallCheck() observes
                  // the progress and resets the timer, so the large advance cannot cause a stall.
                  synchronized (timeSource) {
                    timeSource.advanceMillis(stallTimeMillis * 2);
                  }
                }
                stallDetection.stallCheck();
                // Re-baseline from the post-check sum so the next advance is gated strictly above
                // stallCheck()'s internal lastProcessedCount.
                baseline = stallDetection.getProcessedCount();
                Thread.yield();
              }
            } catch (Throwable t) {
              failed.set(true);
              firstError.compareAndSet(null, t);
              log.error("Stall-checker thread failed", t);
            }
          });

      // The 9 worker threads (incrementers + checker) synchronize on the barrier among
      // themselves; the main thread is not a party and proceeds straight to the join wait.
      assertTrue(
          "Timed out waiting for incrementer threads",
          incrementersDone.await(30, TimeUnit.SECONDS));
    } finally {
      executor.shutdown();
      assertTrue(
          "Timed out waiting for executor shutdown", executor.awaitTermination(30, TimeUnit.SECONDS));
    }

    if (failed.get()) {
      throw new AssertionError("Concurrent operation threw an exception", firstError.get());
    }

    // All concurrent increments must be accounted for exactly once.
    assertEquals(
        "Processed count must equal the total number of concurrent increments",
        expectedTotal,
        stallDetection.getProcessedCount());
  }

  @Test
  public void testZeroStallTime() {
    // A zero stall time should be allowed but would cause quick stall detection
    FakeTimeSource timeSource = new FakeTimeSource();
    StallDetection stallDetection = new StallDetection(0, () -> 1, timeSource);
    assertEquals(0, stallDetection.getStallTimeMillis());

    try {
      // Call stallCheck to initialize timer
      stallDetection.stallCheck();

      // Any advancement of time should trigger a stall with zero stall time
      timeSource.advanceMillis(1);

      // This should throw an exception immediately
      LuceneTestCase.expectThrows(IOException.class, stallDetection::stallCheck);
    } catch (IOException e) {
      fail("First call to stallCheck should not throw: " + e.getMessage());
    }
  }

  @Test
  public void testNegativeStallTime() {
    // Negative stall time should not be allowed
    LuceneTestCase.expectThrows(IllegalArgumentException.class, () -> new StallDetection(-1, () -> 1));
  }

  @Test
  public void testVeryLargeStallTime() throws IOException {
    // Test with a large stall time value (close to Long.MAX_VALUE)
    long veryLargeStallTime = Long.MAX_VALUE / 2;
    FakeTimeSource timeSource = new FakeTimeSource();
    StallDetection stallDetection = new StallDetection(veryLargeStallTime, () -> 1, timeSource);

    // First call to initialize timer
    stallDetection.stallCheck();

    // Advance time, but not enough to trigger a stall
    timeSource.advanceMillis(Integer.MAX_VALUE);

    // This should not throw an exception
    stallDetection.stallCheck();

    // Now increment to test the reset behavior also works with large values
    stallDetection.incrementProcessedCount();
    stallDetection.stallCheck();

    // Advance time past the large stall time
    // This would cause overflow if we're not careful
    timeSource.advanceNanos(Long.MAX_VALUE - 100);

    // Now try to advance time to very close to overflow
    timeSource.advanceNanos(99);

    // This should still not throw because we reset the timer earlier
    stallDetection.stallCheck();
  }
}
