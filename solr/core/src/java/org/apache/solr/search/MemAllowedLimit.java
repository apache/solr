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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.LockFreeExponentiallyDecayingReservoir;
import com.google.common.annotations.VisibleForTesting;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import org.apache.lucene.index.QueryTimeout;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.request.SolrQueryRequest;

public class MemAllowedLimit implements QueryTimeout {
  private static final double MEBI = 1024.0 * 1024.0;
  private static final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
  private static Method GET_BYTES_METHOD;
  private static boolean supported;
  // this keeps the maxDelta that was recorded during previous execution
  private static ThreadLocal<Long> previousMaxDelta =
      ThreadLocal.withInitial(() -> Long.valueOf(0L));

  static {
    boolean testSupported;
    try {
      Class<?> sunThreadBeanClz = Class.forName("com.sun.management.ThreadMXBean");
      if (sunThreadBeanClz.isAssignableFrom(threadBean.getClass())) {
        Method m = sunThreadBeanClz.getMethod("isThreadAllocatedMemorySupported");
        Boolean supported = (Boolean) m.invoke(threadBean);
        if (supported) {
          m = sunThreadBeanClz.getMethod("setThreadAllocatedMemoryEnabled", boolean.class);
          m.invoke(threadBean, Boolean.TRUE);
          testSupported = true;
          GET_BYTES_METHOD = sunThreadBeanClz.getMethod("getCurrentThreadAllocatedBytes");
        } else {
          testSupported = false;
        }
      } else {
        testSupported = false;
      }
    } catch (Exception e) {
      testSupported = false;
    }
    supported = testSupported;
  }

  static final int MIN_COUNT = 100;

  private long limitAtBytes;
  private float limitRatio;
  private long initialBytes;
  private Histogram memHistogram;

  public MemAllowedLimit(SolrQueryRequest req) {
    if (!supported) {
      throw new IllegalArgumentException(
          "Per-thread memory allocation monitoring not available in this JVM.");
    }
    float reqMemLimit = req.getParams().getFloat(CommonParams.MEM_ALLOWED, -1.0f);
    float reqMemRatio = req.getParams().getFloat(CommonParams.MEM_ALLOWED_RATIO, -1.0f);
    if (reqMemLimit <= 0.0f && reqMemRatio <= 1.0f) {
      throw new IllegalArgumentException(
          "Check for limit with hasMemLimit(req) before creating a MemAllowedLimit!");
    }
    if (reqMemRatio >= 0.0f && reqMemRatio < 1.0f) {
      throw new IllegalArgumentException(
          "Parameter " + CommonParams.MEM_ALLOWED_RATIO + " must be greater than 1.0!");
    }
    memHistogram =
        req.getCore()
            .getCoreMetricManager()
            .getSolrMetricsContext()
            .histogram(CommonParams.MEM_ALLOWED, SolrInfoBean.Category.QUERY.toString());

    init(reqMemLimit, reqMemRatio);
  }

  private void init(float reqMemLimit, float reqMemRatio) {
    this.limitRatio = reqMemRatio;
    try {
      initialBytes = (Long) GET_BYTES_METHOD.invoke(threadBean);
      if (reqMemLimit > 0) {
        long limitBytes = Math.round(reqMemLimit * MEBI);
        limitAtBytes = initialBytes + limitBytes;
      } else {
        limitAtBytes = -1;
      }
      // record the last max delta left here by the previous query, and reset
      if (previousMaxDelta.get() > 0L) {
        memHistogram.update(previousMaxDelta.get());
      }
      previousMaxDelta.set(0L);
    } catch (Exception e) {
      supported = false;
      throw new IllegalArgumentException(
          "Unexpected error checking thread allocation, disabling!", e);
    }
  }

  @VisibleForTesting
  MemAllowedLimit(float memLimit, float memLimitRatio, Histogram memHistogram) {
    this.memHistogram = memHistogram;
    init(memLimit, memLimitRatio);
  }

  static Histogram createHistogram() {
    return new Histogram(LockFreeExponentiallyDecayingReservoir.builder().build());
  }

  @VisibleForTesting
  static boolean isSupported() {
    return supported;
  }

  static boolean hasMemLimit(SolrQueryRequest req) {
    return req.getParams().getFloat(CommonParams.MEM_ALLOWED, -1.0f) > 0.0f
        || req.getParams().getFloat(CommonParams.MEM_ALLOWED_RATIO, -1.0f) > 1.0f;
  }

  @Override
  public boolean shouldExit() {
    try {
      long currentAllocatedBytes = (Long) GET_BYTES_METHOD.invoke(threadBean);
      Long lastDelta = previousMaxDelta.get();
      if (currentAllocatedBytes - initialBytes > lastDelta) {
        previousMaxDelta.set(currentAllocatedBytes - initialBytes);
      }
      if (limitRatio > 0.0f && memHistogram.getCount() > MIN_COUNT) {
        long maxDynamicDelta =
            Math.round(memHistogram.getSnapshot().get99thPercentile() * (double) limitRatio);
        if (initialBytes + maxDynamicDelta < currentAllocatedBytes) {
          return true;
        }
        // don't exit yet - check the absolute limit, too
      }
      if (limitAtBytes > 0) {
        return limitAtBytes - currentAllocatedBytes < 0L;
      } else {
        return false;
      }
    } catch (Exception e) {
      supported = false;
      throw new IllegalArgumentException(
          "Unexpected error checking thread allocation, disabling!", e);
    }
  }
}
