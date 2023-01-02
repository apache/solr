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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.SolrTestCase;
import org.apache.solr.util.TimeOut;
import org.junit.Test;

public class ExecutorUtilTest extends SolrTestCase {
  @Test
  // Must prevent runaway failures so limit this to short timeframe in case of failure
  @Timeout(millis = 3000)
  public void testExecutorUtilAwaitsTerminationEnds() throws Exception {
    final long awaitTerminationTimeout = 100;
    final long threadTimeoutDuration = 3 * awaitTerminationTimeout;
    final TimeUnit testTimeUnit = TimeUnit.MILLISECONDS;

    // check that if there is a non interruptable thread that awaitTermination eventually returns.

    ExecutorService executorService =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(
            new SolrNamedThreadFactory(this.getClass().getSimpleName() + "non-interruptable"));
    final AtomicInteger interruptCount = new AtomicInteger();
    Future<Boolean> nonInterruptableFuture =
        executorService.submit(
            () -> getTestThread(threadTimeoutDuration, testTimeUnit, interruptCount, false));
    executorService.shutdownNow();
    assertThrows(
        RuntimeException.class,
        () ->
            ExecutorUtil.awaitTermination(executorService, awaitTerminationTimeout, testTimeUnit));

    // Thread should not have finished in await termination.
    assertFalse(nonInterruptableFuture.isDone());
    assertTrue(interruptCount.get() > 0);

    // Thread should have finished by now.
    Thread.sleep(TimeUnit.MILLISECONDS.convert(threadTimeoutDuration, testTimeUnit));
    assertTrue(nonInterruptableFuture.isDone());
    assertTrue(nonInterruptableFuture.get());

    // check that if there is an interruptable thread that awaitTermination forcefully returns.

    ExecutorService executorService2 =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(
            new SolrNamedThreadFactory(this.getClass().getSimpleName() + "interruptable"));
    interruptCount.set(0);
    Future<Boolean> interruptableFuture =
        executorService2.submit(
            () -> getTestThread(threadTimeoutDuration, testTimeUnit, interruptCount, true));
    executorService2.shutdownNow();
    ExecutorUtil.awaitTermination(executorService2, awaitTerminationTimeout, testTimeUnit);

    // Thread should have been interrupted.
    assertTrue(interruptableFuture.isDone());
    assertTrue(interruptCount.get() > 0);
    assertFalse(interruptableFuture.get());
  }

  private boolean getTestThread(
      long threadTimeoutDuration,
      TimeUnit testTimeUnit,
      AtomicInteger interruptCount,
      boolean interruptable) {
    TimeOut threadTimeout = new TimeOut(threadTimeoutDuration, testTimeUnit, TimeSource.NANO_TIME);
    while (!threadTimeout.hasTimedOut()) {
      try {
        threadTimeout.sleep(TimeUnit.MILLISECONDS.convert(threadTimeoutDuration, testTimeUnit));
      } catch (InterruptedException interruptedException) {
        interruptCount.incrementAndGet();
        if (interruptable) {
          Thread.currentThread().interrupt();
          return false; // didn't run full time
        }
      }
    }
    return true; // ran full time
  }
}
