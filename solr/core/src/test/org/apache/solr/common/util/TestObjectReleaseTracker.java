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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.stringContainsInOrder;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.metrics.SolrMetricsContext;
import org.junit.Test;

public class TestObjectReleaseTracker extends SolrTestCaseJ4 {

  @Test
  public void testReleaseObject() {
    Object obj = new Object();
    ObjectReleaseTracker.track(obj);
    ObjectReleaseTracker.release(obj);
    assertNull(ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty());

    Object obj1 = new Object();
    ObjectReleaseTracker.track(obj1);
    Object obj2 = new Object();
    ObjectReleaseTracker.track(obj2);
    Object obj3 = new Object();
    ObjectReleaseTracker.track(obj3);

    ObjectReleaseTracker.release(obj1);
    ObjectReleaseTracker.release(obj2);
    ObjectReleaseTracker.release(obj3);
    assertNull(ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty());
  }

  @Test
  public void testUnreleased() {
    Object obj1 = new Object();
    Object obj2 = new Object();
    Object obj3 = new Object();

    ObjectReleaseTracker.track(obj1);
    ObjectReleaseTracker.track(obj2);
    ObjectReleaseTracker.track(obj3);

    ObjectReleaseTracker.release(obj1);
    ObjectReleaseTracker.release(obj2);
    // ObjectReleaseTracker.release(obj3);

    assertNotNull(ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty());
    assertNull(ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty());
  }

  @Test
  public void testReleaseDifferentObject() {
    ObjectReleaseTracker.track(new Object());
    ObjectReleaseTracker.release(new Object());
    assertNotNull(ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty());
    assertNull(ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty());
  }

  @Test
  public void testAnonymousClasses() {
    ObjectReleaseTracker.track(new Object() {});
    String message = ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty();
    assertThat(message, containsString("[Object]"));
  }

  @Test
  public void testTryCloseAutoCloseable() {
    AtomicInteger closeCount = new AtomicInteger();
    AutoCloseable closeable =
        new AutoCloseable() {
          @Override
          public void close() {
            closeCount.incrementAndGet();
            ObjectReleaseTracker.release(this);
          }
        };

    try {
      ObjectReleaseTracker.track(closeable);
      assertFalse(closeable instanceof Closeable);

      ObjectReleaseTracker.tryClose();

      assertEquals(1, closeCount.get());
      assertFalse(ObjectReleaseTracker.OBJECTS.containsKey(closeable));
    } finally {
      ObjectReleaseTracker.clear();
    }
  }

  @Test
  public void testTryCloseContinuesAfterAutoCloseableFailure() {
    AtomicInteger failedCloseCount = new AtomicInteger();
    AutoCloseable failingCloseable =
        new AutoCloseable() {
          @Override
          public void close() {
            failedCloseCount.incrementAndGet();
            ObjectReleaseTracker.release(this);
            throw new IllegalStateException("expected close failure");
          }
        };
    AtomicInteger successfulCloseCount = new AtomicInteger();
    AutoCloseable successfulCloseable =
        new AutoCloseable() {
          @Override
          public void close() {
            successfulCloseCount.incrementAndGet();
            ObjectReleaseTracker.release(this);
          }
        };

    try {
      ObjectReleaseTracker.track(failingCloseable);
      ObjectReleaseTracker.track(successfulCloseable);

      ObjectReleaseTracker.tryClose();

      assertEquals(1, failedCloseCount.get());
      assertEquals(1, successfulCloseCount.get());
    } finally {
      ObjectReleaseTracker.clear();
    }
  }

  @Test
  public void testTryCloseSolrMetricsContextClosesRegisteredResources() {
    AtomicInteger closeCount = new AtomicInteger();
    SolrMetricsContext metricsContext = new SolrMetricsContext(null, "test");

    try {
      metricsContext.registerCloseable(closeCount::incrementAndGet);

      ObjectReleaseTracker.tryClose();

      assertEquals(1, closeCount.get());
      assertFalse(ObjectReleaseTracker.OBJECTS.containsKey(metricsContext));
    } finally {
      ObjectReleaseTracker.clear();
    }
  }

  @Test
  public void testTryCloseNestedAutoCloseables() {
    AtomicBoolean childClosed = new AtomicBoolean();
    AtomicInteger childCloseCount = new AtomicInteger();
    AtomicReference<AutoCloseable> childRef = new AtomicReference<>();
    AutoCloseable child =
        () -> {
          if (childClosed.compareAndSet(false, true)) {
            childCloseCount.incrementAndGet();
            ObjectReleaseTracker.release(childRef.get());
          }
        };
    childRef.set(child);

    AtomicInteger parentCloseCount = new AtomicInteger();
    AtomicReference<AutoCloseable> parentRef = new AtomicReference<>();
    AutoCloseable parent =
        () -> {
          parentCloseCount.incrementAndGet();
          child.close();
          ObjectReleaseTracker.release(parentRef.get());
        };
    parentRef.set(parent);

    try {
      ObjectReleaseTracker.track(parent);
      ObjectReleaseTracker.track(child);

      ObjectReleaseTracker.tryClose();

      assertEquals(1, parentCloseCount.get());
      assertTrue(childClosed.get());
      assertEquals(1, childCloseCount.get());
      assertFalse(ObjectReleaseTracker.OBJECTS.containsKey(parent));
      assertFalse(ObjectReleaseTracker.OBJECTS.containsKey(child));
    } finally {
      ObjectReleaseTracker.clear();
    }
  }

  @Test
  public void testTryCloseUsesExecutorServiceShutdownPath() {
    TrackingExecutorService executor = new TrackingExecutorService();

    try {
      ObjectReleaseTracker.track(executor);

      ObjectReleaseTracker.tryClose();

      assertTrue(executor.isShutdown());
      assertTrue(executor.isTerminated());
      assertEquals(0, executor.closeCount);
    } finally {
      ObjectReleaseTracker.clear();
    }
  }

  @Test
  public void testAsyncTracking() throws InterruptedException, ExecutionException {
    ExecutorService es =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("TestExec"));
    Object trackable = new Object();

    Future<?> result =
        es.submit(
            () -> {
              ObjectReleaseTracker.track(trackable);
            });

    result.get(); // make sure that track has been called
    String message = ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty();
    assertThat(
        message,
        stringContainsInOrder(
            ObjectReleaseTracker.ObjectTrackerException.class.getName(),
            "Exception: Submitter stack trace",
            getClassName() + "." + getTestName()));

    // Test the grandparent submitter case
    AtomicReference<Future<?>> indirectResult = new AtomicReference<>();
    result =
        es.submit(
            () ->
                indirectResult.set(
                    es.submit(
                        () -> {
                          ObjectReleaseTracker.track(trackable);
                        })));

    result.get();
    indirectResult.get().get();
    message = ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty();
    assertThat(
        message,
        stringContainsInOrder(
            ObjectReleaseTracker.ObjectTrackerException.class.getName(),
            "Exception: Submitter stack trace",
            "Exception: Submitter stack trace",
            getClassName() + "." + getTestName()));

    // Now test great-grandparent, which we don't explicitly account for, but should have been
    // recursively set
    AtomicReference<Future<?>> indirectIndirect = new AtomicReference<>();
    result =
        es.submit(
            () ->
                indirectResult.set(
                    es.submit(
                        () ->
                            indirectIndirect.set(
                                es.submit(
                                    () -> {
                                      ObjectReleaseTracker.track(trackable);
                                    })))));

    result.get();
    indirectResult.get().get();
    indirectIndirect.get().get();
    message = ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty();
    assertThat(
        message,
        stringContainsInOrder(
            ObjectReleaseTracker.ObjectTrackerException.class.getName(),
            "Exception: Submitter stack trace",
            "Exception: Submitter stack trace",
            "Exception: Submitter stack trace",
            getClassName() + "." + getTestName()));

    es.shutdown();
    assertTrue(es.awaitTermination(1, TimeUnit.SECONDS));
  }

  private static class TrackingExecutorService extends AbstractExecutorService
      implements AutoCloseable {
    private boolean shutdown;
    private boolean terminated;
    private int closeCount;

    @Override
    public void shutdown() {
      shutdown = true;
      terminated = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
      shutdown = true;
      terminated = true;
      return List.of();
    }

    @Override
    public boolean isShutdown() {
      return shutdown;
    }

    @Override
    public boolean isTerminated() {
      return terminated;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return terminated;
    }

    @Override
    public void execute(Runnable command) {}

    @Override
    public void close() {
      closeCount++;
    }
  }
}
