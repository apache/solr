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

import com.google.common.annotations.VisibleForTesting;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.solr.zero.exception.ZeroLockException;
import org.apache.solr.zero.metadata.ZeroMetadataVersion;
import org.apache.solr.zero.metadata.ZeroStoreShardMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Locking logic (and locks) for read and write access to the Zero store */
public class ZeroAccessLocks {
  /**
   * Should only be created once at initialization time, subsequent updates should reuse same lock
   * instance. see {@link
   * org.apache.solr.zero.metadata.MetadataCacheManager.MetadataCacheEntry#updatedOf(ZeroMetadataVersion,
   * ZeroStoreShardMetadata, boolean)} and other overload.
   *
   * <p>We don't need fair ordering policy for this lock, which normally has lower throughput. We
   * rely on cacheLikelyUpToDate and isLeader at query time so that queries may not contend on this
   * lock and let the steady state indexing do its job without contention.
   *
   * <p>On indexing side we rely on read lock and we can have multiple readers just fine. Write lock
   * is only needed in a fail over scenario(leader changed) where we need to pull from Zero store
   * but that is only needed to be done by one thread. Therefore, we acquire write lock with a
   * timeout and check for that condition after the timeout. Therefore, no concern of starvation
   * there either.
   */
  private final ReentrantReadWriteLock indexingAndPullLock;

  /**
   * Should only be created once at initialization time, subsequent updates should reuse same lock
   * instance. see {@link
   * org.apache.solr.zero.metadata.MetadataCacheManager.MetadataCacheEntry#updatedOf(ZeroMetadataVersion,
   * ZeroStoreShardMetadata, boolean)} and other overload.
   *
   * <p>Use a fair ordering policy for this lock, to ensure that the longest waiting thread gets to
   * acquire the lock first. This will help prevent threads that may starve waiting to acquire the
   * lock to push, even though another thread may have already completed the work on their behalf.
   * See {@link CorePusher#endToEndPushCoreToZeroStore()} for details.
   */
  private final ReentrantLock pushLock;

  private final CloseableLock closableIndexingLock;
  private final CloseableLock closablePullLock;
  private final CloseableLock closablePushLock;

  public ZeroAccessLocks() {
    this(new ReentrantReadWriteLock(), new ReentrantLock(true));
  }

  @VisibleForTesting
  public ZeroAccessLocks(ReentrantReadWriteLock indexingAndPullLock, ReentrantLock pushLock) {
    this.indexingAndPullLock = indexingAndPullLock;
    this.pushLock = pushLock;

    closableIndexingLock = new CloseableLock(indexingAndPullLock.readLock());
    closablePullLock = new CloseableLock(indexingAndPullLock.writeLock());
    closablePushLock = new CloseableLock(pushLock);
  }

  /**
   * This lock must be acquired before starting indexing (or any other update) on a core. It
   * disallows pulls from occurring during indexing, because otherwise the subsequent Zero store
   * update could end up overwriting a previous update and leading to data loss. This lock does
   * allow multiple indexing batches to proceed concurrently on a core. This lock must be held
   * during the whole indexing duration, including when the core is pushed to the Zero store at the
   * end of indexing (pushing requires first calling {@link #acquirePushLock(long)})
   */
  public NoThrowAutoCloseable acquireIndexingLock(long timeoutSecond)
      throws ZeroLockException, InterruptedException {
    return closableIndexingLock.tryLockAsResource(timeoutSecond, TimeUnit.SECONDS);
  }

  /**
   * This lock protects pulls from each other (only one pull at any given time for efficiency
   * reasons given only one can succeed), and prevents indexing from happening concurrently with
   * pulls (this could lead to data loss). This lock must be held for the whole duration of a core
   * pull from the Zero store.
   */
  public NoThrowAutoCloseable acquirePullLock(long timeoutSecond)
      throws ZeroLockException, InterruptedException {
    return closablePullLock.tryLockAsResource(timeoutSecond, TimeUnit.SECONDS);
  }

  /**
   * This lock is required for pushing the core to Zero store. Before acquiring this lock, {@link
   * #acquireIndexingLock(long)} must have been called (both are needed for pushing). Although
   * multiple indexing batches can index in parallel (multiple threads can concurrently call {@link
   * #acquireIndexingLock(long)}, only one push can be done in parallel to the Zero store, and only
   * one thread at a time will succeed calling this method (for efficiency reasons, because if
   * multiple pushes happen in parallel only one will succeed).
   */
  public NoThrowAutoCloseable acquirePushLock(long timeoutSecond)
      throws ZeroLockException, InterruptedException {
    return closablePushLock.tryLockAsResource(timeoutSecond, TimeUnit.SECONDS);
  }

  /**
   * To update the cached core metadata, need either a pull write lock or push lock along with pull
   * read lock
   */
  public boolean canUpdateCoreMetadata() {
    return indexingAndPullLock.getWriteHoldCount() > 0
        || (pushLock.getHoldCount() > 0 && indexingAndPullLock.getReadHoldCount() > 0);
  }

  public int getIndexingHoldCount() {
    // The indexing lock is the read component. See closableIndexingLock assignment
    return indexingAndPullLock.getReadHoldCount();
  }

  public int getPullHoldCount() {
    // The pull lock is the write component. See closablePullLock assignment
    return indexingAndPullLock.getWriteHoldCount();
  }

  @Override
  public String toString() {
    return "pullLockHoldCount="
        + getPullHoldCount()
        + " indexingLockHoldCount="
        + getIndexingHoldCount()
        + " pushLockHoldCount="
        + pushLock.getHoldCount();
  }

  public interface NoThrowAutoCloseable extends AutoCloseable {
    @Override
    void close();
  }

  public static class CloseableLock {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final ZeroAccessLocks.NoThrowAutoCloseable unlocker = CloseableLock.this::unlock;

    private final Lock lock;

    public CloseableLock(Lock lock) {
      assert (lock != null);
      this.lock = lock;
    }

    public ZeroAccessLocks.NoThrowAutoCloseable tryLockAsResource(long timeout, TimeUnit unit)
        throws InterruptedException, ZeroLockException {
      Instant startTime = Instant.now();
      boolean success = lock.tryLock(timeout, unit);
      if (!success) {
        if (log.isWarnEnabled()) {
          log.warn(
              "Acquiring lock timed out after: {} ",
              startTime.until(Instant.now(), ChronoUnit.MILLIS));
        }
        throw new ZeroLockException("Couldn't acquire lock within %d ms ", timeout);
      } else {
        if (log.isDebugEnabled()) {
          log.debug("Lock acquired in {} ms", startTime.until(Instant.now(), ChronoUnit.MILLIS));
        }
        return unlocker;
      }
    }

    public void unlock() {
      lock.unlock();
      log.info("Lock released successfully");
    }
  }
}
