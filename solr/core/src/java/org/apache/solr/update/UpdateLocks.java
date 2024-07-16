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

package org.apache.solr.update;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.util.IOFunction;

/**
 * Locks associated with updates in connection with the {@link UpdateLog}.
 *
 * @lucene.internal
 */
public class UpdateLocks {

  private final long docLockTimeoutMs;

  private final ReadWriteLock blockUpdatesLock = new ReentrantReadWriteLock(true);

  private final ConcurrentHashMap<BytesRef, LockAndCondition> idToLock = new ConcurrentHashMap<>();

  public UpdateLocks(long docLockTimeoutMs) {
    this.docLockTimeoutMs = docLockTimeoutMs;
  }

  public <R> R runWithLock(BytesRef id, IOFunction<Condition, R> function) throws IOException {
    final var startTimeNanos = System.nanoTime();

    lockForUpdate();
    try {

      LockAndCondition lock;
      while (true) {
        lock = idToLock.computeIfAbsent(id, (k) -> new LockAndCondition());
        synchronized (lock) {
          if (lock.refCount >= 0) { // always true, notwithstanding a race
            lock.refCount++;
            break;
          }
        }
        // race condition -- existing lock was removed; try again
        Thread.yield();
      }
      // try-finally ensuring we decrement the refCount
      try {
        return runWithLockInternal(id, function, lock, startTimeNanos);
      } finally {
        assert idToLock.get(id) == lock : "lock shouldn't have changed";
        synchronized (lock) {
          assert lock.refCount > 0; // because we incremented it
          if (lock.refCount > 1) {
            lock.refCount--;
          } else { // == 1  (most typical)
            lock.refCount = -1; // unused
            idToLock.remove(id);
          }
        }
      }

    } finally {
      unlockForUpdate();
    }
  }

  private <R> R runWithLockInternal(
      BytesRef id, IOFunction<Condition, R> function, LockAndCondition lock, long startTimeNanos)
      throws IOException {
    // Acquire the lock
    try {
      if (docLockTimeoutMs == 0) {
        lock.lock.lockInterruptibly();
      } else {
        long remainingNs =
            TimeUnit.MILLISECONDS.toNanos(docLockTimeoutMs) - (System.nanoTime() - startTimeNanos);
        boolean timedOut = !lock.lock.tryLock(remainingNs, TimeUnit.NANOSECONDS);
        if (timedOut) {
          throw new SolrException(
              ErrorCode.SERVER_ERROR,
              "Unable to lock doc " + id + " in " + docLockTimeoutMs + " ms");
        }
      }
    } catch (InterruptedException e) {
      // don't set interrupt status; we're ending the request
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to lock doc " + id, e);
    }
    // try-finally ensuring we unlock
    try {
      // We have the lock; do stuff with it
      return function.apply(lock.condition);
    } finally {
      // Release the lock
      lock.lock.unlock();
    }
  }

  private static class LockAndCondition {
    final Lock lock;
    final Condition condition;
    int refCount; // use synchronized to read/write

    LockAndCondition() {
      lock = new ReentrantLock(true); // fair
      condition = lock.newCondition();
      refCount = 0;
    }
  }

  public void lockForUpdate() {
    blockUpdatesLock.readLock().lock();
  }

  public void unlockForUpdate() {
    blockUpdatesLock.readLock().unlock();
  }

  public void blockUpdates() {
    blockUpdatesLock.writeLock().lock();
  }

  public void unblockUpdates() {
    blockUpdatesLock.writeLock().unlock();
  }
}
