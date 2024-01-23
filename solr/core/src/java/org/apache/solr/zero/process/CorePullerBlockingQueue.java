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
package org.apache.solr.zero.process;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.zero.util.DeduplicatingList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper class for a {@link DeduplicatingList} so it appears to be a {@link BlockingQueue} of
 * {@link Runnable} and can be used by the {@link ZeroStoreManager#asyncCorePullExecutor} {@link
 * ThreadPoolExecutor}.
 *
 * <p>This is a bare-bones implementation. Only methods {@link #take()}, {@link #isEmpty()} and
 * {@link #drainTo(Collection)} are implemented. This class is only meant to serve the single very
 * specific use case mentioned above.
 */
public class CorePullerBlockingQueue implements BlockingQueue<Runnable> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final DeduplicatingList<String, CorePuller> pullTaskQueue;
  private final ZeroStoreManager storeManager;

  public CorePullerBlockingQueue(
      DeduplicatingList<String, CorePuller> pullTaskQueue, ZeroStoreManager storeManager) {
    this.pullTaskQueue = pullTaskQueue;
    this.storeManager = storeManager;
  }

  /**
   * Wrapper of {@link CorePuller} instantiated each time an object is {@link #take}-en from the
   * {@link CorePullerBlockingQueue} so that the executor that consumes that queue ({@link
   * ZeroStoreManager#asyncCorePullExecutor}) get a {@link Runnable} as expected.
   */
  static class RunnableAsyncCorePull implements Runnable {
    private final CorePuller corePuller;
    private final boolean isleaderPulling;
    private final Instant enqueueTime = Instant.now();

    public RunnableAsyncCorePull(CorePuller corePuller, boolean isleaderPulling) {
      this.corePuller = corePuller;
      this.isleaderPulling = isleaderPulling;
    }

    @Override
    public void run() {
      MDCLoggingContext.setCore(corePuller.getCore());
      try {
        CorePullStatus pullStatus = corePuller.lockAndPullCore(isleaderPulling, 0);
        // Tests might count down on a latch to observe the end of the pull
        corePuller.notifyPullEnd(pullStatus);
        if (pullStatus.isSuccess()) {
          if (log.isInfoEnabled()) {
            log.info(
                "Async Zero store pull finished successfully {} ms after being enqueued",
                enqueueTime.until(Instant.now(), ChronoUnit.MILLIS));
          }
        } else {
          // Just a log, no retries, the pull has failed. Future pulls might succeed
          log.error(
              "Async Zero store pull failed with status {} {} ms after being enqueued",
              pullStatus,
              enqueueTime.until(Instant.now(), ChronoUnit.MILLIS));
        }
      } finally {
        MDCLoggingContext.clear();
      }
    }
  }

  @Override
  public Runnable take() throws InterruptedException {
    CorePuller cp = pullTaskQueue.removeFirst();
    return new RunnableAsyncCorePull(cp, storeManager.isLeader(cp.getCore()));
  }

  @Override
  public boolean isEmpty() {
    return pullTaskQueue.size() == 0;
  }

  @Override
  public int drainTo(Collection<? super Runnable> c) {
    int count = 0;
    while (!isEmpty()) {
      try {
        c.add(take());
        count++;
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
      }
    }
    return count;
  }

  /*
   * CorePullerBlockingQueue methods below this point throw UnsupportedOperationException
   */

  @Override
  public boolean add(Runnable runnable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean offer(Runnable runnable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Runnable remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Runnable poll() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Runnable element() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Runnable peek() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void put(Runnable runnable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean offer(Runnable runnable, long timeout, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Runnable poll(long timeout, TimeUnit unit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int remainingCapacity() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(Collection<? extends Runnable> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean contains(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<Runnable> iterator() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int drainTo(Collection<? super Runnable> c, int maxElements) {
    throw new UnsupportedOperationException();
  }
}
