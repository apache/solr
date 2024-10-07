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
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enforces a memory-based limit on a given SolrQueryRequest, as specified by the {@code memAllowed}
 * query parameter.
 *
 * <p>This class tracks per-thread memory allocations during a request using its own ThreadLocal. It
 * records the current thread allocation when the instance was created (typically at the start of
 * SolrQueryRequest processing) as a starting point, and then on every call to {@link #shouldExit()}
 * it accumulates the amount of reported allocated memory since the previous call, and compares the
 * accumulated amount to the configured threshold, expressed in mebi-bytes.
 *
 * <p>NOTE: this class accesses {@code
 * com.sun.management.ThreadMXBean#getCurrentThreadAllocatedBytes} using reflection. On JVM-s where
 * this implementation is not available an exception will be thrown when attempting to use the
 * {@code memAllowed} parameter.
 */
public class MemAllowedLimit implements QueryLimit {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final double MEBI = 1024.0 * 1024.0;
  private static final ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
  private static final Method GET_BYTES_METHOD;
  private static final boolean supported;

  static {
    boolean testSupported;
    Method getBytesMethod = null;
    try {
      Class<?> sunThreadBeanClz = Class.forName("com.sun.management.ThreadMXBean");
      if (sunThreadBeanClz.isAssignableFrom(threadBean.getClass())) {
        Method m = sunThreadBeanClz.getMethod("isThreadAllocatedMemorySupported");
        Boolean supported = (Boolean) m.invoke(threadBean);
        if (supported) {
          m = sunThreadBeanClz.getMethod("setThreadAllocatedMemoryEnabled", boolean.class);
          m.invoke(threadBean, Boolean.TRUE);
          testSupported = true;
          getBytesMethod = sunThreadBeanClz.getMethod("getCurrentThreadAllocatedBytes");
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
    GET_BYTES_METHOD = getBytesMethod;
  }

  private static final ThreadLocal<AtomicLong> threadLocalMem =
      ThreadLocal.withInitial(() -> new AtomicLong(-1L));

  private long limitBytes;
  private final AtomicLong accumulatedMem = new AtomicLong();
  private long exitedAt = 0;

  public MemAllowedLimit(SolrQueryRequest req) {
    if (!supported) {
      throw new IllegalArgumentException(
          "Per-thread memory allocation monitoring not available in this JVM.");
    }
    float reqMemLimit = req.getParams().getFloat(CommonParams.MEM_ALLOWED, -1.0f);
    if (reqMemLimit <= 0.0f) {
      throw new IllegalArgumentException(
          "Check for limit with hasMemLimit(req) before creating a MemAllowedLimit!");
    }
    limitBytes = Math.round(reqMemLimit * MEBI);
    // init the thread-local
    init();
  }

  @VisibleForTesting
  MemAllowedLimit(float memLimit) {
    if (!supported) {
      throw new IllegalArgumentException(
          "Per-thread memory allocation monitoring not available in this JVM.");
    }
    limitBytes = Math.round(memLimit * MEBI);
    // init the thread-local
    init();
  }

  private final void init() {
    long currentAllocatedBytes;
    try {
      currentAllocatedBytes = (Long) GET_BYTES_METHOD.invoke(threadBean);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unexpected error checking thread allocation!", e);
    }
    AtomicLong threadMem = threadLocalMem.get();
    threadMem.compareAndSet(-1L, currentAllocatedBytes);
  }

  private long getCurrentAllocatedBytes() {
    try {
      return (Long) GET_BYTES_METHOD.invoke(threadBean);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unexpected error checking thread allocation!", e);
    }
  }

  @VisibleForTesting
  static boolean isSupported() {
    return supported;
  }

  static boolean hasMemLimit(SolrQueryRequest req) {
    return req.getParams().getFloat(CommonParams.MEM_ALLOWED, -1.0f) > 0.0f;
  }

  @Override
  public boolean shouldExit() {
    if (exitedAt > 0L) {
      return true;
    }

    try {
      long currentAllocatedBytes = getCurrentAllocatedBytes();
      AtomicLong threadMem = threadLocalMem.get();
      long lastAllocatedBytes = threadMem.get();
      accumulatedMem.addAndGet(currentAllocatedBytes - lastAllocatedBytes);
      threadMem.set(currentAllocatedBytes);
      if (log.isDebugEnabled()) {
        log.debug(
            "mem limit thread {} remaining delta {}",
            Thread.currentThread().getName(),
            (limitBytes - accumulatedMem.get()));
      }
      if (limitBytes < accumulatedMem.get()) {
        exitedAt = accumulatedMem.get();
        return true;
      }
      return false;
    } catch (Exception e) {
      throw new IllegalArgumentException("Unexpected error checking thread allocation!", e);
    }
  }

  @Override
  public Object currentValue() {
    return exitedAt > 0 ? exitedAt : accumulatedMem.get();
  }
}
