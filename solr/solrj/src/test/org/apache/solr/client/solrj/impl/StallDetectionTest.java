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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StallDetectionTest extends SolrTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // testConcurrentStallDetection config
  private static final int NUM_INCREMENTERS = 5;
  private static final int NUM_CHECKERS = 5;
  private static final long STALL_TIME_MILLIS = 200;
  private static final long CHECKER_INTERVAL_MILLIS = 10;
  private static final long TIME_ADVANCE_STEP_MILLIS = 50;

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
    IOException exception = expectThrows(IOException.class, stallDetection::stallCheck);
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
    expectThrows(IOException.class, stallDetection::stallCheck);
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
    expectThrows(IOException.class, stallDetection::stallCheck);
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
    expectThrows(IOException.class, stallDetection::stallCheck);
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
  public void testConcurrentStallDetection() throws Exception {
    // Test components
    ExecutorService executor = null;
    FakeTimeSource timeSource = new FakeTimeSource();
    BlockingQueue<Integer> testQueue = new LinkedBlockingQueue<>();

    // Add initial items to queue
    for (int i = 0; i < 10; i++) {
      testQueue.add(i);
    }

    IntSupplier queueSizeSupplier = () -> testQueue.size();
    StallDetection stallDetection =
        new StallDetection(STALL_TIME_MILLIS, queueSizeSupplier, timeSource);

    try {
      executor =
          ExecutorUtil.newMDCAwareCachedThreadPool(
              new SolrNamedThreadFactory("EnhancedStallDetectionTest"));
      // Thread synchronization
      final CyclicBarrier startBarrier = new CyclicBarrier(NUM_INCREMENTERS + NUM_CHECKERS + 1);
      final CountDownLatch phase1Complete = new CountDownLatch(1);
      final CountDownLatch phase2Complete = new CountDownLatch(1);
      final CountDownLatch allComplete = new CountDownLatch(NUM_INCREMENTERS + NUM_CHECKERS);

      // Control flags
      final AtomicBoolean keepRunning = new AtomicBoolean(true);
      final AtomicBoolean shouldStall = new AtomicBoolean(false);

      // Result tracking
      final AtomicReference<Throwable> unexpectedException = new AtomicReference<>(null);
      final AtomicReference<IOException> expectedStallException = new AtomicReference<>(null);
      final AtomicInteger prematureStallCount = new AtomicInteger(0);
      final AtomicInteger correctStallCount = new AtomicInteger(0);
      int missedStallCount = 0;

      // For checking queue transitions
      final AtomicBoolean queueWasEmpty = new AtomicBoolean(false);
      AtomicInteger queueTransitionCount = new AtomicInteger();

      List<Future<?>> futures = new ArrayList<>();

      log.info(
          "Setting up test with {} incrementers and {} checkers", NUM_INCREMENTERS, NUM_CHECKERS);

      // Submit incrementer tasks
      for (int i = 0; i < NUM_INCREMENTERS; i++) {
        final int incrementerId = i;
        futures.add(
            executor.submit(
                () -> {
                  try {
                    log.debug("Incrementer {} waiting at barrier", incrementerId);
                    startBarrier.await(10, TimeUnit.SECONDS);
                    log.debug("Incrementer {} started", incrementerId);

                    // Phase 1: Normal operation
                    while (keepRunning.get() && !shouldStall.get()) {
                      // Process items from queue
                      Integer item = testQueue.poll();
                      if (item != null) {
                        stallDetection.incrementProcessedCount();
                        // Simulate some work
                        Thread.sleep(random().nextInt(5));
                        // Add a new item to keep queue non-empty
                        if (random().nextBoolean()) {
                          testQueue.add(random().nextInt(100));
                        }
                      } else {
                        // Queue is empty, nothing to process
                        if (!queueWasEmpty.getAndSet(true)) {
                          queueTransitionCount.getAndIncrement();
                          log.debug("Queue became empty");
                        }
                        Thread.sleep(5); // Wait briefly before checking again
                      }

                      // Occasionally empty the queue completely to test transitions
                      if (random().nextInt(100) == 0) {
                        testQueue.clear();
                        log.debug("Queue cleared by incrementer {}", incrementerId);
                      }
                    }

                    // Phase 2: Stall if instructed
                    if (shouldStall.get()) {
                      log.debug("Incrementer {} stopping to induce stall", incrementerId);
                      // Stop incrementing, but don't exit yet
                      phase2Complete.await(20, TimeUnit.SECONDS);
                    }

                  } catch (Exception e) {
                    if (!(e instanceof InterruptedException)) {
                      log.error("Incrementer {} failed", incrementerId, e);
                      unexpectedException.compareAndSet(null, e);
                    }
                  } finally {
                    log.debug("Incrementer {} completing", incrementerId);
                    allComplete.countDown();
                  }
                }));
      }

      // Submit checker tasks
      for (int i = 0; i < NUM_CHECKERS; i++) {
        final int checkerId = i;
        futures.add(
            executor.submit(
                () -> {
                  try {
                    log.debug("Checker {} waiting at barrier", checkerId);
                    startBarrier.await(10, TimeUnit.SECONDS);
                    log.debug("Checker {} started", checkerId);

                    while (keepRunning.get()) {
                      try {
                        stallDetection.stallCheck();
                      } catch (IOException e) {
                        if (shouldStall.get()) {
                          if (log.isWarnEnabled()) {
                            log.warn("Checker {} detected expected stall: {}", checkerId, e);
                          }
                          expectedStallException.compareAndSet(null, e);
                          correctStallCount.incrementAndGet();
                          phase2Complete.countDown(); // Signal to complete phase 2
                          break; // Exit loop after detecting stall
                        } else {
                          if (log.isErrorEnabled()) {
                            log.error("Checker {} detected unexpected stall in phase 1", checkerId);
                          }
                          prematureStallCount.incrementAndGet();
                          unexpectedException.compareAndSet(null, e);
                        }
                      }

                      // Check less frequently than incrementers
                      Thread.sleep(CHECKER_INTERVAL_MILLIS);

                      // Occasionally reset the stall timer to test that functionality
                      if (random().nextInt(50) == 0) {
                        stallDetection.resetStallTimer();
                        log.debug("Checker {} reset stall timer", checkerId);
                      }
                    }

                  } catch (Exception e) {
                    if (!(e instanceof InterruptedException)) {
                      log.error("Checker {} failed", checkerId, e);
                      unexpectedException.compareAndSet(null, e);
                    }
                  } finally {
                    log.debug("Checker {} completing", checkerId);
                    allComplete.countDown();
                  }
                }));
      }

      log.info("Starting all threads");
      startBarrier.await(10, TimeUnit.SECONDS); // Start all threads

      // Phase 1: Verify normal operation with continuous processing
      log.info("Phase 1: Testing normal operation with continuous processing");
      long phase1StartTime = System.nanoTime();

      // Monitor and advance time during phase 1
      for (int step = 0; step < 10; step++) {
        // Let threads run for a while
        Thread.sleep(TIME_ADVANCE_STEP_MILLIS);

        // Advance fake time
        timeSource.advanceMillis(TIME_ADVANCE_STEP_MILLIS);

        // Check for premature stall detection
        if (prematureStallCount.get() > 0) {
          fail("Stall detected prematurely during Phase 1");
        }

        // Check for unexpected exceptions
        if (unexpectedException.get() != null) {
          fail("Unexpected exception in Phase 1: " + unexpectedException.get());
        }
        if (log.isDebugEnabled()) {
          log.debug("Phase 1 step {} complete, queue size: {}", step, testQueue.size());
        }
      }
      if (log.isInfoEnabled()) {
        log.info(
            "Phase 1 complete after {} ms. No stalls detected.",
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - phase1StartTime));
      }
      phase1Complete.countDown();

      // Phase 2: Induce stall and verify detection
      log.info("Phase 2: Inducing stall");
      shouldStall.set(true); // Signal incrementers to stop processing

      // Wait for incrementers to notice the signal
      Thread.sleep(100);

      // Add items to the queue to ensure it's not empty during stall
      testQueue.clear();
      for (int i = 0; i < 5; i++) {
        testQueue.add(i);
      }

      // Advance time until stall should be detected
      long startTime = System.nanoTime();
      long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(STALL_TIME_MILLIS * 3 + 1000);

      boolean stallDetected = false;
      while (System.nanoTime() - startTime < timeoutNanos) {
        // Advance time in smaller steps to ensure stall detection
        timeSource.advanceMillis(TIME_ADVANCE_STEP_MILLIS);
        Thread.sleep(20); // Let checkers run with new time

        if (expectedStallException.get() != null) {
          stallDetected = true;
          break;
        }

        // Check for unexpected exceptions
        if (unexpectedException.get() != null) {
          fail("Unexpected exception in Phase 2: " + unexpectedException.get());
        }
      }

      // If stall wasn't detected, mark as missed and allow test to complete
      if (!stallDetected) {
        missedStallCount++;
        if (log.isErrorEnabled()) {
          log.error("Stall was not detected within timeout");
        }
        phase2Complete.countDown();
      }

      // Complete test
      log.info("Completing test, waiting for all threads to finish");
      keepRunning.set(false);

      // Wait for all futures to complete
      for (Future<?> future : futures) {
        try {
          future.get(15, TimeUnit.SECONDS);
        } catch (Exception e) {
          log.warn("Exception during future cleanup get", e);
        }
      }

      // Also wait for all threads to complete via the latch
      assertTrue("Threads did not complete in time", allComplete.await(10, TimeUnit.SECONDS));

      // Verify results
      if (unexpectedException.get() != null) {
        fail("Unexpected exception occurred: " + unexpectedException.get());
      }

      assertEquals("Premature stalls detected", 0, prematureStallCount.get());
      assertEquals("Missed stalls", 0, missedStallCount);
      assertTrue("No stall detected in phase 2", correctStallCount.get() > 0);

      IOException stallException = expectedStallException.get();
      assertNotNull("Stall exception was null", stallException);
      String msg = stallException.getMessage();
      log.info("Stall exception message: {}", msg);

      assertTrue(
          "Incorrect stall exception message",
          stallException.getMessage().contains("Request processing has stalled"));

      log.info("Queue transitions observed: {}", queueTransitionCount);
      log.info("Test completed successfully");

    } finally {
      if (executor != null) {
        ExecutorUtil.shutdownAndAwaitTermination(executor);
      }
    }
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
      expectThrows(IOException.class, stallDetection::stallCheck);
    } catch (IOException e) {
      fail("First call to stallCheck should not throw: " + e.getMessage());
    }
  }

  @Test
  public void testNegativeStallTime() {
    // Negative stall time should not be allowed
    expectThrows(IllegalArgumentException.class, () -> new StallDetection(-1, () -> 1));
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
