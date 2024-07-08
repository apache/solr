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
import net.jcip.annotations.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows tracking information about the current thread using the JVM's built-in management bean
 * {@link java.lang.management.ThreadMXBean}.
 *
 * <p>Calling code should create an instance of this class when starting the operation, and then can
 * get the {@link #getElapsedCpuMs()} at any time thereafter.
 *
 * <p>This class is irrevocably not thread safe. Never allow instances of this class to be exposed
 * to more than one thread. Acquiring an external lock will not be sufficient. This class can be
 * considered "lock-hostile" due to its caching of timing information for a specific thread.
 */
@NotThreadSafe
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
   * Create an instance to track the current thread's usage of CPU. Usage information can later be
   * retrieved by calling {@link #getElapsedCpuMs()}. Timing starts immediately upon construction.
   */
  public ThreadCpuTimer() {
    this.startCpuTimeNanos = getThreadTotalCpuNs();
  }

  public static boolean isSupported() {
    return THREAD_MX_BEAN != null;
  }

  /**
   * Return CPU time consumed by this thread since the construction of this timer object.
   *
   * @return current value, or {@link #UNSUPPORTED} if not supported.
   */
  public long getElapsedCpuNs() {
    return this.startCpuTimeNanos != UNSUPPORTED
        ? getThreadTotalCpuNs() - this.startCpuTimeNanos
        : UNSUPPORTED;
  }

  /**
   * Get the cpu time for the current thread since {@link Thread#start()} without throwing an
   * exception.
   *
   * @see ThreadMXBean#getCurrentThreadCpuTime() for important details
   * @return the number of nanoseconds of cpu consumed by this thread since {@code Thread.start()}.
   */
  private long getThreadTotalCpuNs() {
    if (THREAD_MX_BEAN != null) {
      return THREAD_MX_BEAN.getCurrentThreadCpuTime();
    } else {
      return UNSUPPORTED;
    }
  }

  /**
   * Get the CPU usage information for the current thread since it created this {@link
   * ThreadCpuTimer}. The result is undefined if called by any other thread.
   *
   * @return the thread's cpu since the creation of this {@link ThreadCpuTimer} instance. If the
   *     VM's cpu tracking is disabled, returned value will be {@link #UNSUPPORTED}.
   */
  public Optional<Long> getElapsedCpuMs() {
    long cpuTimeNs = getElapsedCpuNs();
    return cpuTimeNs != UNSUPPORTED
        ? Optional.of(TimeUnit.MILLISECONDS.convert(cpuTimeNs, TimeUnit.NANOSECONDS))
        : Optional.empty();
  }

  @Override
  public String toString() {
    return getElapsedCpuMs().map(String::valueOf).orElse("UNSUPPORTED");
  }
}
