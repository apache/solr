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

package org.apache.solr.servlet;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import net.jcip.annotations.ThreadSafe;
import org.apache.solr.core.RateLimiterConfig;

/**
 * Handles rate limiting for a specific request type.
 *
 * <p>The control flow is as follows: Handle request -- Check if slot is available -- If available,
 * acquire slot and proceed -- else reject the same.
 */
@ThreadSafe
public class RequestRateLimiter {

  // Total slots that are available for this request rate limiter.
  private final Semaphore totalSlotsPool;

  // Competitive slots pool that are available for this rate limiter as well as borrowing by other
  // request rate limiters. By competitive, the meaning is that there is no prioritization for the
  // acquisition of these slots -- First Come First Serve, irrespective of whether the request is of
  // this request rate limiter or other.
  private final Semaphore borrowableSlotsPool;

  private final AtomicInteger nativeReservations;

  private final RateLimiterConfig rateLimiterConfig;
  public static final SlotReservation UNLIMITED =
      () -> {
        // no-op
      };

  public RequestRateLimiter(RateLimiterConfig rateLimiterConfig) {
    this.rateLimiterConfig = rateLimiterConfig;
    totalSlotsPool = new Semaphore(rateLimiterConfig.allowedRequests);
    int guaranteedSlots = rateLimiterConfig.guaranteedSlotsThreshold;
    if (!rateLimiterConfig.isSlotBorrowingEnabled
        || guaranteedSlots >= rateLimiterConfig.allowedRequests) {
      // slot borrowing is disabled, either explicitly or implicitly
      borrowableSlotsPool = null;
      nativeReservations = null;
    } else if (guaranteedSlots <= 0) {
      // all slots are guaranteed
      borrowableSlotsPool = totalSlotsPool;
      nativeReservations = null;
    } else {
      borrowableSlotsPool = new Semaphore(rateLimiterConfig.allowedRequests - guaranteedSlots);
      nativeReservations = new AtomicInteger();
    }
  }

  @VisibleForTesting
  boolean isEmpty() {
    if (totalSlotsPool.availablePermits() != rateLimiterConfig.allowedRequests) {
      return false;
    }
    if (nativeReservations == null) {
      return true;
    }
    if (nativeReservations.get() != 0) {
      return false;
    }
    assert borrowableSlotsPool != null; // implied by `nativeReservations != null`
    return borrowableSlotsPool.availablePermits()
        == rateLimiterConfig.allowedRequests - rateLimiterConfig.guaranteedSlotsThreshold;
  }

  /**
   * Handles an incoming request. returns a metadata object representing the metadata for the
   * acquired slot, if acquired. If a slot is not acquired, returns a null metadata object.
   */
  public SlotReservation handleRequest() throws InterruptedException {

    if (!rateLimiterConfig.isEnabled) {
      return UNLIMITED;
    }

    if (totalSlotsPool.tryAcquire(
        rateLimiterConfig.waitForSlotAcquisition, TimeUnit.MILLISECONDS)) {
      if (nativeReservations == null) {
        assert borrowableSlotsPool == null || totalSlotsPool == borrowableSlotsPool;
        // simple case: all slots guaranteed; or none, do not double-acquire
        return new SingleSemaphoreReservation(totalSlotsPool);
      }
      assert borrowableSlotsPool != null; // implied by `nativeReservations != null`
      if (nativeReservations.incrementAndGet() <= rateLimiterConfig.guaranteedSlotsThreshold
          || borrowableSlotsPool.tryAcquire()) {
        // we either fungibly occupy a guaranteed slot, so don't have to acquire
        // a borrowable slot; or we acquire a borrowable slot
        return new NativeBorrowableReservation(
            totalSlotsPool,
            borrowableSlotsPool,
            nativeReservations,
            rateLimiterConfig.guaranteedSlotsThreshold);
      } else {
        // this should never happen, but if it does we should not leak permits/accounting
        nativeReservations.decrementAndGet();
        totalSlotsPool.release();
        throw new IllegalStateException(
            "if we have a top-level slot, there should be an available borrowable slot");
      }
    }

    return null;
  }

  /**
   * Whether to allow another request type to borrow a slot from this request rate limiter.
   * Typically works fine if there is a relatively lesser load on this request rate limiter's type
   * compared to the others (think of skew).
   *
   * @return returns a metadata object for the acquired slot, if acquired. If the slot was not
   *     acquired, returns a metadata object with a null pool.
   * @lucene.experimental -- Can cause slots to be blocked if a request borrows a slot and is itself
   *     long lived.
   */
  public SlotReservation allowSlotBorrowing() throws InterruptedException {
    if (borrowableSlotsPool == null) {
      return null;
    }
    if (totalSlotsPool.tryAcquire(
        rateLimiterConfig.waitForSlotAcquisition, TimeUnit.MILLISECONDS)) {
      if (totalSlotsPool == borrowableSlotsPool) {
        // simple case: there are no guaranteed slots; do not double-acquire
        return new SingleSemaphoreReservation(borrowableSlotsPool);
      } else if (borrowableSlotsPool.tryAcquire()) {
        return new BorrowedReservation(totalSlotsPool, borrowableSlotsPool);
      } else {
        // this can happen, e.g., if all of the borrowable slots are occupied
        // by non-native requests, but there are open guaranteed slots. In that
        // case, top-level acquire would succeed, but borrowed acquire would fail.
        totalSlotsPool.release();
      }
    }

    return null;
  }

  public RateLimiterConfig getRateLimiterConfig() {
    return rateLimiterConfig;
  }

  public interface SlotReservation extends Closeable {}

  // Represents the metadata for a slot
  static class SingleSemaphoreReservation implements SlotReservation {
    private final Semaphore usedPool;

    public SingleSemaphoreReservation(Semaphore usedPool) {
      assert usedPool != null;
      this.usedPool = usedPool;
    }

    @Override
    public void close() {
      usedPool.release();
    }
  }

  static class NativeBorrowableReservation implements SlotReservation {
    private final Semaphore totalPool;
    private final Semaphore borrowablePool;
    private final AtomicInteger nativeReservations;
    private final int guaranteedSlots;

    public NativeBorrowableReservation(
        Semaphore totalPool,
        Semaphore borrowablePool,
        AtomicInteger nativeReservations,
        int guaranteedSlots) {
      this.totalPool = totalPool;
      this.borrowablePool = borrowablePool;
      this.nativeReservations = nativeReservations;
      this.guaranteedSlots = guaranteedSlots;
    }

    @Override
    public void close() {
      if (nativeReservations.getAndDecrement() > guaranteedSlots) {
        // we should consider ourselves as having come from the borrowable pool
        borrowablePool.release();
      }
      totalPool.release(); // release this last
    }
  }

  static class BorrowedReservation implements SlotReservation {
    private final Semaphore totalPool;
    private final Semaphore borrowablePool;

    public BorrowedReservation(Semaphore totalPool, Semaphore borrowablePool) {
      this.totalPool = totalPool;
      this.borrowablePool = borrowablePool;
    }

    @Override
    public void close() {
      borrowablePool.release();
      totalPool.release();
    }
  }
}
