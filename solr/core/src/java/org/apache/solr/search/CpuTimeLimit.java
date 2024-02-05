package org.apache.solr.search;

import com.google.common.annotations.VisibleForTesting;
import org.apache.lucene.index.QueryTimeout;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.TimeUnit;

public class CpuTimeLimit implements QueryTimeout {
  private static final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

  private final long limitAt;

  public CpuTimeLimit(SolrQueryRequest req) {
    long reqCpuLimit = req.getParams().getLong(CommonParams.CPU_ALLOWED, -1L);

    if (reqCpuLimit <= 0L) {
      throw new IllegalArgumentException(
          "Check for limit with hasLimit(req) before creating a CpuTimeLimit");
    }
    long currentTime = threadMXBean.getCurrentThreadCpuTime();
    limitAt = currentTime + TimeUnit.NANOSECONDS.convert(reqCpuLimit, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  CpuTimeLimit(long limitMs) {
    limitAt = threadMXBean.getCurrentThreadCpuTime() + TimeUnit.NANOSECONDS.convert(limitMs, TimeUnit.MILLISECONDS);
  }

  static boolean hasCpuLimit(SolrQueryRequest req) {
    return req.getParams().getLong(CommonParams.CPU_ALLOWED, -1L) > 0L;
  }

  /** Return true if a max limit value is set and the current usage has exceeded the limit. */
  @Override
  public boolean shouldExit() {
    return limitAt - threadMXBean.getCurrentThreadCpuTime() < 0L;
  }
}
