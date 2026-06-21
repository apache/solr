package org.apache.solr.client.solrj.impl;

import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.Phaser;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

public class AsyncTracker implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final long CLOSE_TIMEOUT = TimeUnit.SECONDS.convert(1, TimeUnit.HOURS);

  private final Semaphore available;
  private final boolean wait;

  private volatile boolean closed = false;

  private final ReentrantLock waitForCompleteLock = new ReentrantLock(false);

  // wait for async requests
  private final Phaser phaser;
  // maximum outstanding requests left

  public static class ThePhaser extends Phaser {

    ThePhaser(int start) {
      super(start);
    }

    @Override
    protected boolean onAdvance(int phase, int parties) {
      return false;
    }
  }

  public AsyncTracker(int maxOutstandingAsyncRequests) {
    this(maxOutstandingAsyncRequests, true, 0);
  }

  public AsyncTracker(int maxOutstandingAsyncRequests, boolean wait, int start) {
    phaser = new ThePhaser(start);
    this.wait = wait;
    if (maxOutstandingAsyncRequests > 0) {
      available = new Semaphore(maxOutstandingAsyncRequests, false);
    } else {
      available = null;
    }
  }

  public void waitForComplete(long timeout, TimeUnit timeUnit) throws TimeoutException {
    //waitForCompleteLock.lock();
    try {
      final int registeredParties = phaser.getRegisteredParties();
      int phase = phaser.getPhase();
      if (phaser.getUnarrivedParties() == 0) return;
      if (log.isTraceEnabled()) {
        final int unarrivedParties = phaser.getUnarrivedParties();
        final int arrivedParties = phaser.getArrivedParties();
        log.trace("Before wait for outstanding requests registered: {} arrived: {}, {} {}", registeredParties, arrivedParties,
            unarrivedParties, phaser);
      }
      try {
        phaser.awaitAdvanceInterruptibly(phase, timeout, timeUnit);
      } catch (IllegalStateException e) {
        log.error("Unexpected, perhaps came after close; ?", e);
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }

      if (log.isTraceEnabled()) log.trace("After wait for outstanding requests {}", phaser);
    } finally {
      //waitForCompleteLock.unlock();
    }
  }

  public void close() {
    try {
      if (wait && available != null) {
        while (true) {
          final boolean hasQueuedThreads = available.hasQueuedThreads();
          if (!hasQueuedThreads) break;
          available.release(available.getQueueLength());
        }
      }
      phaser.forceTermination();
    } catch (Exception e) {
      log.error("Exception closing Http2SolrClient asyncTracker", e);
    } finally {
      closed = true;
    }
  }

  public boolean register() {
    if (log.isDebugEnabled()) {
      log.debug("Registered new party {}", phaser);
    }

    phaser.register();

    if (available != null) {
      if (!wait) {
        boolean success;
        success = available.tryAcquire();
        return success;
      } else {
        try {
          available.acquire();
        } catch (InterruptedException e) {
          throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
        }
      }
    }
    return true;
  }

  public void arrive() {
    arrive(true);
  }

  public void arrive(boolean releaseAvailable) {

    if (available != null && releaseAvailable) available.release();

    try {
      phaser.arriveAndDeregister();
    } catch (IllegalStateException e) {
      // if (closed) {
      log.warn("Came after close", e);
      //  } else {
      //   throw e;
      // }
    }

    if (log.isDebugEnabled()) log.debug("Request complete {}", phaser);
  }

  public int getUnArrived() {
    return phaser.getUnarrivedParties();
  }
}
