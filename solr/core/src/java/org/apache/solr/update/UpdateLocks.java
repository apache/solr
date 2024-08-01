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

import com.carrotsearch.hppc.IntObjectHashMap;
import java.io.IOException;
import java.util.ArrayDeque;
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

  // SolrCloud's first approach was a fixed size array of locks (lock striping) using an ID's hash.
  //   Sized too small, there was too much lock sharing; sized too big, there was memory waste.
  // Here we have a Map keyed by hash and a pool of locks to re-use.  Synchronization is needed 2x.
  //   Note:  ConcurrentHashMap was also explored but HPPC came out on top, probably because
  //          we can use a hashcode directly as the key, and it's GC friendly (zero-allocation).

  /** Maps a ID hashcode to a lock.  Synchronize to manipulate. */
  private final IntObjectHashMap<LockAndCondition> hashToLock =
      new IntObjectHashMap<>(32) {
        @Override
        protected int hashKey(int key) {
          return key; // our keys are themselves hash-codes
        }
      };

  /** A pool of locks to avoid creating & GC'ing them too much.  Must synchronize on hashToLock. */
  private final ArrayDeque<LockAndCondition> lockPool = new ArrayDeque<>(16);

  public UpdateLocks(long docLockTimeoutMs) {
    this.docLockTimeoutMs = docLockTimeoutMs;
  }

  /**
   * Acquires a lock for the given doc ID, executes the function, and releases the lock.  The
   * provided {@link Condition} can be used for wait/notify control.  The per-doc locking is needed
   * indirectly due to SolrCloud's internal out-of-order versioned update design.
   */
  public <R> R runWithLock(BytesRef id, IOFunction<Condition, R> function) throws IOException {
    final var startTimeNanos = System.nanoTime();

    lockForUpdate();
    try {
      // note: if we didn't need a Condition, then we could have reused
      //   OrderedExecutor.SparseStripedLock over here, which is also a mechanism invented for
      //   per-doc locking.

      // hashToLock isn't concurrent, but we synchronize on it briefly twice to do cheap work

      final int hash = id.hashCode();
      final LockAndCondition lock;
      // get or insert lock, increment refcount
      synchronized (hashToLock) {
        final int idx = hashToLock.indexOf(hash);
        if (hashToLock.indexExists(idx)) {
          lock = hashToLock.indexGet(idx);
          assert lock.refCount >= 1;
          lock.refCount++;
        } else {
          lock = borrowLock();
          hashToLock.indexInsert(idx, hash, lock);
        }
      }

      // try-finally ensuring we decrement the refCount
      try {
        return runWithLockInternal(id, function, lock, startTimeNanos);
      } finally {
        // decrement refcount, remove lock if unreferenced
        synchronized (hashToLock) {
          assert lock.refCount > 0; // because we incremented it
          if (--lock.refCount == 0) { // typical
            hashToLock.remove(hash);
            returnLock(lock);
          }
        }
      }

    } finally {
      unlockForUpdate();
    }
  }

  private LockAndCondition borrowLock() {
    assert Thread.holdsLock(hashToLock);
    if (lockPool.isEmpty()) {
      return new LockAndCondition();
    } else {
      return lockPool.removeLast();
    }
  }

  private void returnLock(LockAndCondition lock) {
    assert Thread.holdsLock(hashToLock);
    if (lockPool.size() < 16) {
      lockPool.add(lock);
      lock.refCount = 1; // ready for next use
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
    int refCount; // only access when synchronized on hashToLock

    LockAndCondition() {
      lock = new ReentrantLock(true); // fair
      condition = lock.newCondition();
      refCount = 1;
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
