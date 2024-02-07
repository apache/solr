package org.apache.solr.search;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.LockFreeExponentiallyDecayingReservoir;
import com.google.common.annotations.VisibleForTesting;
import org.apache.lucene.index.QueryTimeout;
import org.apache.solr.common.params.CommonParams;
                                                                                                                                                   import org.apache.solr.request.SolrQueryRequest;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;

public class MemQueryLimit implements QueryTimeout {
  private static final double MEBI = 1024.0 * 1024.0;
  private static final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
  private static Method GET_BYTES_METHOD;
  private static boolean available;
  // this keeps the maxDelta recorded during previous execution
  private static ThreadLocal<Long> previousMaxDelta = ThreadLocal.withInitial(() -> Long.valueOf(0L));
  private static Histogram memHistogram = new Histogram(LockFreeExponentiallyDecayingReservoir.builder().build());

  static {
    boolean testAvailable;
    try {
      Class<?> sunThreadBeanClz = Class.forName("com.sun.management.ThreadMXBean");
      if (sunThreadBeanClz.isAssignableFrom(threadBean.getClass())) {
        Method m = sunThreadBeanClz.getMethod("isThreadAllocatedMemorySupported");
        Boolean supported = (Boolean) m.invoke(threadBean);
        if (supported) {
          m = sunThreadBeanClz.getMethod("setThreadAllocatedMemoryEnabled", boolean.class);
          m.invoke(threadBean, Boolean.TRUE);
          testAvailable = true;
          GET_BYTES_METHOD = sunThreadBeanClz.getMethod("getCurrentThreadAllocatedBytes");
        } else {
          testAvailable = false;
        }
      } else {
        testAvailable = false;
      }
    } catch (Exception e) {
      testAvailable = false;
    }
    available = testAvailable;
  }

  private long limitAtBytes;
  private long initialBytes;

  public MemQueryLimit(SolrQueryRequest req) {
    if (!available) {
      throw new IllegalArgumentException("Per-thread memory allocation monitoring not available in this JVM.");
    }
    float reqMemLimit = req.getParams().getFloat(CommonParams.MEM_ALLOWED, -1.0f);
    if (reqMemLimit <= 0.0f) {
      throw new IllegalArgumentException(
          "Check for limit with hasMemLimit(req) before creating a MemQueryLimit");
    }
    init(reqMemLimit);
  }

  private void init(float reqMemLimit) {
    long limitBytes = Math.round(reqMemLimit * MEBI);
    try {
      initialBytes = (Long) GET_BYTES_METHOD.invoke(threadBean);
      limitAtBytes = initialBytes + limitBytes;
      // record the last max delta left here by the previous query, and reset
      if (previousMaxDelta.get() > 0L) {
        memHistogram.update(previousMaxDelta.get());
      }
      previousMaxDelta.set(0L);
    } catch (Exception e) {
      available = false;
      throw new IllegalArgumentException("Unexpected error checking thread allocation, disabling!", e);
    }
  }

  @VisibleForTesting
  MemQueryLimit(float memLimit) {
    init(memLimit);
  }

  @VisibleForTesting
  static boolean isAvailable() {
    return available;
  }

  static boolean hasMemLimit(SolrQueryRequest req) {
    return req.getParams().getFloat(CommonParams.MEM_ALLOWED, -1.0f) > 0.0f;
  }

  @Override
  public boolean shouldExit() {
    try {
      long currentAllocatedBytes = (Long) GET_BYTES_METHOD.invoke(threadBean);
      Long lastDelta = previousMaxDelta.get();
      if (currentAllocatedBytes - initialBytes > lastDelta) {
        previousMaxDelta.set(currentAllocatedBytes - initialBytes);
      }
      return limitAtBytes - currentAllocatedBytes < 0L;
    } catch (Exception e) {
      available = false;
      throw new IllegalArgumentException("Unexpected error checking thread allocation, disabling!", e);
    }
  }
}
