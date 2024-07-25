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
package org.apache.solr.search;

import static org.apache.solr.util.ThreadCpuTimer.readNSAndReset;

import com.google.common.annotations.VisibleForTesting;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import net.jcip.annotations.NotThreadSafe;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.ThreadCpuTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enforces a CPU-time based timeout on a given SolrQueryRequest, as specified by the {@code
 * cpuAllowed} query parameter.
 *
 * <p>Since this class uses {@link ThreadCpuTimer} it is irrevocably lock-hostile and can never be
 * exposed to multiple threads, even if guarded by synchronization. Normally this is attached to
 * objects ultimately held by a ThreadLocal in {@link SolrRequestInfo} to provide safe usage on the
 * assumption that such objects are not shared to other threads.
 *
 * @see ThreadCpuTimer
 */
@NotThreadSafe
public class CpuAllowedLimit implements QueryLimit {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final long requestedTimeoutNs;
  private volatile long timedOutAt = 0L;
  AtomicLong accumulatedTime = new AtomicLong(0);
  public static final String TIMING_CONTEXT = CpuAllowedLimit.class.getName();

  /**
   * Create an object to represent a CPU time limit for the current request. NOTE: this
   * implementation will attempt to obtain an existing thread CPU time monitor, created when {@link
   * QueryLimits#QueryLimits(SolrQueryRequest, SolrQueryResponse)} is called.
   *
   * @param req solr request with a {@code cpuAllowed} parameter
   */
  public CpuAllowedLimit(SolrQueryRequest req) {
    if (!ThreadCpuTimer.isSupported()) {
      throw new IllegalArgumentException("Thread CPU time monitoring is not available.");
    }
    long reqCpuLimit = req.getParams().getLong(CommonParams.CPU_ALLOWED, -1L);

    if (reqCpuLimit <= 0L) {
      throw new IllegalArgumentException(
          "Check for limit with hasCpuLimit(req) before creating a CpuAllowedLimit");
    }
    // calculate the time when the limit is reached, e.g. account for the time already spent
    requestedTimeoutNs = TimeUnit.NANOSECONDS.convert(reqCpuLimit, TimeUnit.MILLISECONDS);

    // here we rely on the current thread never creating a second CpuAllowedLimit within the same
    // request, and also rely on it always creating a new CpuAllowedLimit object for each
    // request that requires it.
    ThreadCpuTimer.beginContext(TIMING_CONTEXT);
  }

  @VisibleForTesting
  CpuAllowedLimit(long limitMs) {
    requestedTimeoutNs = TimeUnit.NANOSECONDS.convert(limitMs, TimeUnit.MILLISECONDS);
  }

  /** Return true if the current request has a parameter with a valid value of the limit. */
  static boolean hasCpuLimit(SolrQueryRequest req) {
    return req.getParams().getLong(CommonParams.CPU_ALLOWED, -1L) > 0L;
  }

  /** Return true if usage has exceeded the limit. */
  @Override
  public boolean shouldExit() {
    if (timedOutAt > 0L) {
      return true;
    }
    // if unsupported, use zero, and thus never exit, expect jvm and/or cpu branch
    // prediction to short circuit things if unsupported.
    Long delta = readNSAndReset(TIMING_CONTEXT).orElse(0L);
    try {
      if (accumulatedTime.addAndGet(delta) > requestedTimeoutNs) {
        timedOutAt = accumulatedTime.get();
        return true;
      }
      return false;
    } finally {
      // uncomment for debugging. Our suspicious log checker will never be happy here... (nor should
      // it be)

      //      java.text.DecimalFormatSymbols symbols = new DecimalFormatSymbols(Locale.US);
      //      DecimalFormat formatter = new DecimalFormat("#,###", symbols);
      //
      //      if (log.isInfoEnabled()) {
      //        log.info("++++++++++++ SHOULD_EXIT {} accumulated:{} vs {} ++++ ON:{}",
      // formatter.format(delta),
      // formatter.format(accumulatedTime.get()),formatter.format(requestedTimeoutNs)
      // ,Thread.currentThread().getName());
      //      }
    }
  }

  @Override
  public Object currentValue() {
    return timedOutAt > 0 ? timedOutAt : accumulatedTime.get();
  }
}
