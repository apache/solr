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
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.index.QueryTimeout;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CpuTimeQueryLimit implements QueryTimeout {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
  private static final boolean available;

  static {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    boolean testAvailable = false;
    try {
      if (threadBean.isThreadCpuTimeSupported()) {
        threadBean.setThreadCpuTimeEnabled(true);
        testAvailable = true;
      }
    } catch (UnsupportedOperationException e) {
      log.info("Thread CPU time monitoring is not available.");
      testAvailable = false;
    }
    available = testAvailable;
  }

  private final long limitAt;

  public CpuTimeQueryLimit(SolrQueryRequest req) {
    if (!available) {
      throw new IllegalArgumentException("Thread CPU time monitoring is not available.");
    }
    long reqCpuLimit = req.getParams().getLong(CommonParams.CPU_ALLOWED, -1L);

    if (reqCpuLimit <= 0L) {
      throw new IllegalArgumentException(
          "Check for limit with hasLimit(req) before creating a CpuTimeLimit");
    }
    long currentTime = THREAD_MX_BEAN.getCurrentThreadCpuTime();
    limitAt = currentTime + TimeUnit.NANOSECONDS.convert(reqCpuLimit, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  static boolean isAvailable() {
    return available;
  }

  @VisibleForTesting
  CpuTimeQueryLimit(long limitMs) {
    limitAt =
        THREAD_MX_BEAN.getCurrentThreadCpuTime()
            + TimeUnit.NANOSECONDS.convert(limitMs, TimeUnit.MILLISECONDS);
  }

  static boolean hasCpuLimit(SolrQueryRequest req) {
    return req.getParams().getLong(CommonParams.CPU_ALLOWED, -1L) > 0L;
  }

  /** Return true if a max limit value is set and the current usage has exceeded the limit. */
  @Override
  public boolean shouldExit() {
    return limitAt - THREAD_MX_BEAN.getCurrentThreadCpuTime() < 0L;
  }
}
