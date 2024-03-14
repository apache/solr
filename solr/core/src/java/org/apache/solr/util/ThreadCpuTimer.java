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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows tracking information about the current thread using the JVM's built-in management bean
 * {@link java.lang.management.ThreadMXBean}.
 *
 * <p>Calling code should create an instance of this class when starting the operation, and then can
 * get the {@link #getCpuTimeMs()} at any time thereafter.
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

  private final long startCpuTimeNanos;

  /**
   * Create an instance to track the current thread's usage of CPU. The usage information can later
   * be retrieved by any thread by calling {@link #getCpuTimeMs()}.
   */
  public ThreadCpuTimer() {
    if (THREAD_MX_BEAN != null) {
      this.startCpuTimeNanos = getThreadTotalCpuNs();
    } else {
      this.startCpuTimeNanos = UNSUPPORTED;
    }
  }

  public static boolean isSupported() {
    return THREAD_MX_BEAN != null;
  }

  /**
   * Return the initial value of CPU time for this thread when this instance was first created.
   * NOTE: absolute value returned by this method has no meaning by itself, it should only be used
   * when comparing elapsed time between this value and {@link #getElapsedTimerCpuNs()}.
   *
   * @return current value, or {@link #UNSUPPORTED} if not supported.
   */
  public long getStartCpuTimeNs() {
    return startCpuTimeNanos;
  }

  /**
   * Return current value of CPU time for this thread.
   *
   * @return current value, or {@link #UNSUPPORTED} if not supported.
   */
  public long getElapsedTimerCpuNs() {
    if (THREAD_MX_BEAN != null) {
      return this.startCpuTimeNanos != UNSUPPORTED
          ? getThreadTotalCpuNs() - this.startCpuTimeNanos
          : UNSUPPORTED;
    } else {
      return UNSUPPORTED;
    }
  }

  public long getThreadTotalCpuNs() {
    return THREAD_MX_BEAN.getCurrentThreadCpuTime();
  }

  /**
   * Get the CPU usage information for the thread that created this {@link ThreadCpuTimer}. The
   * information will track the thread's cpu since the creation of this {@link ThreadCpuTimer}
   * instance, if the VM's cpu tracking is disabled, returned value will be {@link #UNSUPPORTED}.
   */
  public Optional<Long> getCpuTimeMs() {
    long cpuTimeNs = getElapsedTimerCpuNs();
    return cpuTimeNs != UNSUPPORTED
        ? Optional.of(TimeUnit.MILLISECONDS.convert(cpuTimeNs, TimeUnit.NANOSECONDS))
        : Optional.of(UNSUPPORTED);
  }

  @Override
  public String toString() {
    return getCpuTimeMs().toString();
  }
}
