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
package org.apache.solr.util;

import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows tracking information about the current thread using the JVM's built-in management bean
 * {@link java.lang.management.ThreadMXBean}. Methods on this class are safe for use on any thread,
 * but will return different values for different threads by design.
 */
public class ThreadCpuTimer {
  private static final long UNSUPPORTED = -1;
  public static final String CPU_TIME = "cpuTime";
  public static final String LOCAL_CPU_TIME = "localCpuTime";
  public static final String ENABLE_CPU_TIME = "solr.log.cputime";

  private static ThreadMXBean THREAD_MX_BEAN;
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static {
    try {
      ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
      if (!threadBean.isThreadCpuTimeEnabled()) {
        threadBean.setThreadCpuTimeEnabled(true);
      }
      THREAD_MX_BEAN = threadBean;
    } catch (UnsupportedOperationException | SecurityException e) {
      THREAD_MX_BEAN = null;
      log.info("Thread CPU time monitoring is not available.");
    }
  }

  private static final ThreadLocal<Map<String, AtomicLong>> threadLocalTimer =
      ThreadLocal.withInitial(ConcurrentHashMap::new);

  /* no instances shall be created. */
  private ThreadCpuTimer() {}

  public static void beginContext(String context) {
    readNSAndReset(context);
  }

  public static boolean isSupported() {
    return THREAD_MX_BEAN != null;
  }

  /**
   * Get the number of nanoseconds since the last time <strong>this thread</strong> took a reading
   * for the supplied context.
   *
   * @param context An arbitrary name that code can supply to avoid clashing with other usages.
   * @return An optional long which may be empty if
   *     java.lang.management.ManagementFactory#getThreadMXBean() is unsupported or otherwise
   *     unavailable.
   */
  public static Optional<Long> readNSAndReset(String context) {
    // simulate heavy query and/or heavy CPU load in tests
    TestInjection.injectCpuUseInSearcherCpuLimitCheck();
    if (THREAD_MX_BEAN == null) {
      return Optional.empty();
    } else {
      AtomicLong threadCpuTime =
          threadLocalTimer
              .get()
              .computeIfAbsent(
                  context, (ctx) -> new AtomicLong(THREAD_MX_BEAN.getCurrentThreadCpuTime()));
      long currentThreadCpuTime = THREAD_MX_BEAN.getCurrentThreadCpuTime();
      long result = currentThreadCpuTime - threadCpuTime.get();
      threadCpuTime.set(currentThreadCpuTime);
      return Optional.of(result);
    }
  }

  /**
   * Discard any accumulated time for a given context since the last invocation.
   *
   * @param context the context to reset
   */
  public static void reset(String context) {
    if (THREAD_MX_BEAN != null) {
      threadLocalTimer
          .get()
          .computeIfAbsent(
              context, (ctx) -> new AtomicLong(THREAD_MX_BEAN.getCurrentThreadCpuTime()))
          .set(THREAD_MX_BEAN.getCurrentThreadCpuTime());
    }
  }

  public static Optional<Long> readMSandReset(String context) {
    return readNSAndReset(context)
        .map((cpuTimeNs) -> TimeUnit.MILLISECONDS.convert(cpuTimeNs, TimeUnit.NANOSECONDS));
  }

  /**
   * Cleanup method. This should be called at the very end of a request thread when it's absolutely
   * sure no code will attempt a new reading.
   */
  public static void reset() {
    threadLocalTimer.get().clear();
  }

  @Override
  public String toString() {
    return THREAD_MX_BEAN == null
        ? "UNSUPPORTED"
        : "Timing contexts:" + threadLocalTimer.get().toString();
  }
}
