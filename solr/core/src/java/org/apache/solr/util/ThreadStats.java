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
public class ThreadStats {
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
    } catch (UnsupportedOperationException e) {
      THREAD_MX_BEAN = null;
      log.info("Operation Cpu Time is not supported.");
    }
  }

  private final long threadId;
  private final long startCpuTimeNanos;

  /**
   * Create an instance to track the current thread's usage of cpu and memory. The usage information
   * can later be retrieved by any thread by calling {@link #getCpuTimeMs()}.
   */
  public ThreadStats() {
    this.threadId = Thread.currentThread().getId();
    if (THREAD_MX_BEAN != null) {
      this.startCpuTimeNanos = THREAD_MX_BEAN.getThreadCpuTime(threadId);
    } else {
      this.startCpuTimeNanos = UNSUPPORTED;
    }
  }

  /**
   * Get the cpu usage information for the thread that created this {@link ThreadStats}. The
   * information will track the thread's cpu since the creation of this {@link ThreadStats}
   * instance, if the VM's cpu tracking is disabled, returned value counr be {@link #UNSUPPORTED}
   */
  public Optional<Long> getCpuTimeMs() {
    if (THREAD_MX_BEAN != null) {
      long cpuTimeMs =
          this.startCpuTimeNanos != UNSUPPORTED
              ? TimeUnit.NANOSECONDS.toMillis(
                  THREAD_MX_BEAN.getThreadCpuTime(threadId) - this.startCpuTimeNanos)
              : UNSUPPORTED;
      return Optional.of(cpuTimeMs);
    } else {
      return Optional.empty();
    }
  }

  @Override
  public String toString() {
    return getCpuTimeMs().toString();
  }
}
