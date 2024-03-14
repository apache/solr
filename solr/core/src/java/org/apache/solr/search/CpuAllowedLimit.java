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
import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.index.QueryTimeout;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.util.ThreadCpuTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enforces a CPU-time based timeout on a given SolrQueryRequest, as specified by the {@code
 * cpuAllowed} query parameter.
 */
public class CpuAllowedLimit implements QueryTimeout {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final long
      timeOutAtThreadTotalCpuNs; // this is a value based on total cpu since the inception of the
  // thread
  private final ThreadCpuTimer threadCpuTimer;

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
    threadCpuTimer =
        solrRequestInfo != null ? solrRequestInfo.getThreadCpuTimer() : new ThreadCpuTimer();
    long reqCpuLimit = req.getParams().getLong(CommonParams.CPU_ALLOWED, -1L);

    if (reqCpuLimit <= 0L) {
      throw new IllegalArgumentException(
          "Check for limit with hasCpuLimit(req) before creating a CpuAllowedLimit");
    }
    // calculate when the time when the limit is reached, e.g. account for the time already spent
    timeOutAtThreadTotalCpuNs =
        threadCpuTimer.getStartCpuTimeNs() // how much the thread already used since it began
            + TimeUnit.NANOSECONDS.convert(reqCpuLimit, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  CpuAllowedLimit(long limitMs) {
    this.threadCpuTimer = new ThreadCpuTimer();
    timeOutAtThreadTotalCpuNs =
        threadCpuTimer.getThreadTotalCpuNs()
            + TimeUnit.NANOSECONDS.convert(limitMs, TimeUnit.MILLISECONDS);
  }

  /** Return true if the current request has a parameter with a valid value of the limit. */
  static boolean hasCpuLimit(SolrQueryRequest req) {
    return req.getParams().getLong(CommonParams.CPU_ALLOWED, -1L) > 0L;
  }

  /** Return true if a max limit value is set and the current usage has exceeded the limit. */
  @Override
  public boolean shouldExit() {
    return timeOutAtThreadTotalCpuNs - threadCpuTimer.getThreadTotalCpuNs() < 0L;
  }
}
