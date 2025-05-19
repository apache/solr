package org.apache.solr.servlet;

import com.codahale.metrics.Timer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.servlet.http.HttpServletRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.RateLimiterConfig;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;

/**
 * PriorityBasedRateLimiter allocates the slot based on their request priority Currently, it has two
 * priorities FOREGROUND and BACKGROUND Requests. Client can pass the {@link
 * org.apache.solr.common.params.CommonParams} SOLR_REQUEST_TYPE_PARAM request header to indicate
 * the foreground and background request. Foreground requests has high priority than background
 * requests
 */
public class PriorityBasedRateLimiter extends RequestRateLimiter implements SolrMetricProducer {
  public static final String SOLR_REQUEST_PRIORITY_PARAM = "Solr-Request-Priority";
  private final AtomicInteger activeRequests = new AtomicInteger();
  private final Semaphore numRequestsAllowed;

  private final int totalAllowedRequests;

  private final LinkedBlockingQueue<CountDownLatch> waitingList = new LinkedBlockingQueue<>();

  private final long waitTimeoutInNanos;

  private SolrMetricsContext solrMetricsContext;

  private final Timer foregroundRequestDelay;
  private final Timer backgroundRequestDelay;

  public PriorityBasedRateLimiter(
      RateLimiterConfig rateLimiterConfig, SolrMetricsContext solrMetricsContext) {
    super(rateLimiterConfig);
    this.numRequestsAllowed = new Semaphore(rateLimiterConfig.allowedRequests, true);
    this.totalAllowedRequests = rateLimiterConfig.allowedRequests;
    this.waitTimeoutInNanos = rateLimiterConfig.waitForSlotAcquisition * 1000000l;
    this.initializeMetrics(solrMetricsContext, null);
    this.foregroundRequestDelay =
        solrMetricsContext.timer("foregroundRequestDelay", "PriorityBasedRateLimiter");
    this.backgroundRequestDelay =
        solrMetricsContext.timer("backgroundRequestDelay", "PriorityBasedRateLimiter");
  }

  @Override
  public SlotReservation handleRequest(HttpServletRequest request) {
    if (!rateLimiterConfig.isEnabled) {
      return UNLIMITED;
    }
    RequestPriorities requestPriority = getRequestPriority(request);
    if (requestPriority == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Request priority header is not defined or not set properly");
    }
    Timer.Context timer =
        requestPriority == RequestPriorities.FOREGROUND
            ? this.foregroundRequestDelay.time()
            : this.backgroundRequestDelay.time();
    try {
      if (!acquire(requestPriority)) {
        return null;
      }
    } catch (InterruptedException ie) {
      return null;
    } finally {
      timer.stop();
    }
    return () -> PriorityBasedRateLimiter.this.release();
  }

  private boolean acquire(RequestPriorities priority) throws InterruptedException {
    if (priority.equals(RequestPriorities.FOREGROUND)) {
      return nextInQueue(this.waitTimeoutInNanos);
    } else if (priority.equals(RequestPriorities.BACKGROUND)) {
      if (this.activeRequests.get() < this.totalAllowedRequests) {
        return nextInQueue(this.waitTimeoutInNanos);
      } else {
        CountDownLatch wait = new CountDownLatch(1);
        this.waitingList.put(wait);
        long startTime = System.nanoTime();
        if (wait.await(this.waitTimeoutInNanos, TimeUnit.NANOSECONDS)) {
          return nextInQueue(this.waitTimeoutInNanos - (System.nanoTime() - startTime));
        } else {
          // remove from the queue; this/other requests already waited long enough; thus best effort
          this.waitingList.poll();
          return false;
        }
      }
    }
    return true;
  }

  private boolean nextInQueue(long waitTimeoutInNanos) throws InterruptedException {
    this.activeRequests.addAndGet(1);
    boolean acquired =
        this.numRequestsAllowed.tryAcquire(1, waitTimeoutInNanos, TimeUnit.NANOSECONDS);
    if (!acquired) {
      this.activeRequests.addAndGet(-1);
      return false;
    }
    return true;
  }

  private void exitFromQueue() {
    this.numRequestsAllowed.release(1);
    this.activeRequests.addAndGet(-1);
  }

  private void release() {
    this.exitFromQueue();
    if (this.activeRequests.get() < this.totalAllowedRequests) {
      // next priority
      CountDownLatch waiter = this.waitingList.poll();
      if (waiter != null) {
        waiter.countDown();
      }
    }
  }

  @Override
  public SlotReservation allowSlotBorrowing() throws InterruptedException {
    // if we reach here that means slot is not available
    return null;
  }

  public int getRequestsAllowed() {
    return this.activeRequests.get();
  }

  private RequestPriorities getRequestPriority(HttpServletRequest request) {
    String requestPriority = request.getHeader(SOLR_REQUEST_PRIORITY_PARAM);
    try {
      return RequestPriorities.valueOf(requestPriority);
    } catch (IllegalArgumentException iae) {
    }
    return null;
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    this.solrMetricsContext = parentContext.getChildContext(this);
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return this.solrMetricsContext;
  }

  public enum RequestPriorities {
    // this has high priority
    FOREGROUND,
    // this has low priority
    BACKGROUND
  }
}
