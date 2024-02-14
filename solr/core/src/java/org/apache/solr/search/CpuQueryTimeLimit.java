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
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.index.QueryTimeout;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.ThreadCpuTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CpuQueryTimeLimit implements QueryTimeout {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final long limitAtNs;
  private final ThreadCpuTime threadCpuTime;

  public CpuQueryTimeLimit(SolrQueryRequest req, ThreadCpuTime threadCpuTime) {
    if (!ThreadCpuTime.isSupported()) {
      throw new IllegalArgumentException("Thread CPU time monitoring is not available.");
    }
    Objects.requireNonNull(threadCpuTime, "Thread CPU time monitor must not be null.");
    this.threadCpuTime = threadCpuTime;
    long reqCpuLimit = req.getParams().getLong(CommonParams.CPU_ALLOWED, -1L);

    if (reqCpuLimit <= 0L) {
      throw new IllegalArgumentException(
          "Check for limit with hasCpuLimit(req) before creating a CpuQueryTimeLimit");
    }
    // calculate when the time limit is reached, account for the time already spent
    limitAtNs =
        threadCpuTime.getStartCpuTimeNs()
            + TimeUnit.NANOSECONDS.convert(reqCpuLimit, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  CpuQueryTimeLimit(long limitMs) {
    this.threadCpuTime = new ThreadCpuTime();
    limitAtNs =
        threadCpuTime.getCurrentCpuTimeNs()
            + TimeUnit.NANOSECONDS.convert(limitMs, TimeUnit.MILLISECONDS);
  }

  static boolean hasCpuLimit(SolrQueryRequest req) {
    return req.getParams().getLong(CommonParams.CPU_ALLOWED, -1L) > 0L;
  }

  /** Return true if a max limit value is set and the current usage has exceeded the limit. */
  @Override
  public boolean shouldExit() {
    return limitAtNs - threadCpuTime.getCurrentCpuTimeNs() < 0L;
  }
}
