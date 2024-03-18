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

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.NotThreadSafe;
import org.apache.lucene.index.QueryTimeout;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.util.ThreadCpuTimer;

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
public class CpuAllowedLimit implements QueryTimeout {
  private final ThreadCpuTimer threadCpuTimer;
  private final long requestedTimeoutNs;

  /**
   * Create an object to represent a CPU time limit for the current request. NOTE: this
   * implementation will attempt to obtain an existing thread CPU time monitor, created when {@link
   * SolrRequestInfo#getThreadCpuTimer()} is initialized.
   *
   * @param req solr request with a {@code cpuAllowed} parameter
   */
  public CpuAllowedLimit(SolrQueryRequest req) {
    if (!ThreadCpuTimer.isSupported()) {
      throw new IllegalArgumentException("Thread CPU time monitoring is not available.");
    }
    SolrRequestInfo solrRequestInfo = SolrRequestInfo.getRequestInfo();
    // get existing timer if available to ensure sub-queries can't reset/exceed the intended time
    // constraint.
    threadCpuTimer =
        solrRequestInfo != null ? solrRequestInfo.getThreadCpuTimer() : new ThreadCpuTimer();
    long reqCpuLimit = req.getParams().getLong(CommonParams.CPU_ALLOWED, -1L);

    if (reqCpuLimit <= 0L) {
      throw new IllegalArgumentException(
          "Check for limit with hasCpuLimit(req) before creating a CpuAllowedLimit");
    }
    // calculate the time when the limit is reached, e.g. account for the time already spent
    requestedTimeoutNs = TimeUnit.NANOSECONDS.convert(reqCpuLimit, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  CpuAllowedLimit(long limitMs) {
    this.threadCpuTimer = new ThreadCpuTimer();
    requestedTimeoutNs = TimeUnit.NANOSECONDS.convert(limitMs, TimeUnit.MILLISECONDS);
  }

  /** Return true if the current request has a parameter with a valid value of the limit. */
  static boolean hasCpuLimit(SolrQueryRequest req) {
    return req.getParams().getLong(CommonParams.CPU_ALLOWED, -1L) > 0L;
  }

  /** Return true usage has exceeded the limit. */
  @Override
  public boolean shouldExit() {
    return threadCpuTimer.getElapsedCpuNs() > requestedTimeoutNs;
  }
}
