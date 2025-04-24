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
import java.util.function.IntSupplier;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
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
  public void testConcurrentAccess() throws Exception {

    final long stallTimeMillis = 1000;
    final FakeTimeSource timeSource = new FakeTimeSource();
    final AtomicInteger queueSize = new AtomicInteger(10);
    final StallDetection stallDetection =
        new StallDetection(stallTimeMillis, queueSize::get, timeSource);

    // Initialize the timer
    stallDetection.stallCheck();

    final int threadCount = 10;

    final CyclicBarrier barrier = new CyclicBarrier(threadCount);
    final CountDownLatch allDone = new CountDownLatch(threadCount);
    final AtomicBoolean exceptionThrown = new AtomicBoolean(false);

    ExecutorService executor =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            threadCount, new SolrNamedThreadFactory("StallDetectionTest"));

    try {
      for (int i = 0; i < threadCount; i++) {
        final int threadId = i;
        executor.submit(
            () -> {
              try {
                // Wait for all threads to be ready
                barrier.await();

                // Half the threads increment the progress counter
                // The other half call stallCheck
                if (threadId % 2 == 0) {
                  // Increment progress
                  for (int j = 0; j < 100; j++) {
                    stallDetection.incrementProcessedCount();
                    Thread.yield(); // Hint to allow other threads to run
                  }
                } else {
                  // Check for stalls
                  for (int j = 0; j < 100; j++) {
                    try {
                      stallDetection.stallCheck();
                      // Advance time a bit between checks, but not enough to trigger a stall
                      synchronized (timeSource) {
                        timeSource.advanceMillis(5);
                      }
                      Thread.yield(); // Hint to allow other threads to run
                    } catch (IOException e) {
                      exceptionThrown.set(true);
                      fail("Unexpected IOException: " + e.getMessage());
                    }
                  }
                }
              } catch (Exception e) {
                exceptionThrown.set(true);
                log.error("Exception in thread {}", threadId, e);
              } finally {
                allDone.countDown();
              }
            });
      }

      // Wait for all threads to complete
      assertTrue("Timed out waiting for threads to complete", allDone.await(30, TimeUnit.SECONDS));

      // Verify no exceptions were thrown
      assertFalse("Exception was thrown during concurrent operation", exceptionThrown.get());

      // Verify the final processed count is correct
      assertEquals(
          "Processed count should equal number of incrementing threads × 100",
          (threadCount / 2) * 100,
          stallDetection.getProcessedCount());
    } finally {
      executor.shutdownNow();
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
