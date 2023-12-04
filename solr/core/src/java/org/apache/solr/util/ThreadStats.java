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

import com.sun.management.ThreadMXBean;

import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
/**
 * Allows tracking information about the current thread using the JVM's built-in management bean
 * {@link java.lang.management.ThreadMXBean}.
 * 
 * Calling code should create an instance of this class when starting the operation, and then
 * can get the {@link Usage} at any time thereafter.
 *
 */
public class ThreadStats {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final long UNSUPPORTED = -1;
  public static final String SHARDS_CPU_TIME = "cpuTime";
  public static final String LOCAL_CPU_TIME = "localCpuTime";
  public static final String ENABLE_CPU_TIME = "solr.enableCpuTime";

  private static final ThreadMXBean THREAD_MX_BEAN;

  static {
    java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
    if (bean instanceof ThreadMXBean) {
      ThreadMXBean threadBean = (ThreadMXBean) bean;
      if (threadBean.isThreadAllocatedMemorySupported()) {
        if (!threadBean.isThreadAllocatedMemoryEnabled()) {
          threadBean.setThreadAllocatedMemoryEnabled(true);
        }
        if (!threadBean.isThreadCpuTimeEnabled()) {
          threadBean.setThreadCpuTimeEnabled(true);
        }
        THREAD_MX_BEAN = threadBean;
      }
      else {
        log.info("Thread allocated memory is not supported.");
        THREAD_MX_BEAN = null;
      }
    }
    else {
      if (log.isInfoEnabled()) {
        log.info("Registered thread management bean is an unrecognized type: {}", bean.getClass());
      }
      THREAD_MX_BEAN = null;
    }
  }

  private final long threadId;
  private final long startAllocatedBytes;
  private final long startCpuTimeNanos;

  /**
   * Create an instance to track the current thread's usage of cpu and memory.  The usage information
   * can later be retrieved by any thread by calling {@link #getUsage()}.
   */
  public ThreadStats() {
    this.threadId = Thread.currentThread().getId();
    if (THREAD_MX_BEAN != null) {
      this.startAllocatedBytes = THREAD_MX_BEAN.getThreadAllocatedBytes(threadId);
      this.startCpuTimeNanos = THREAD_MX_BEAN.getThreadCpuTime(threadId);
    } else {
      this.startAllocatedBytes = UNSUPPORTED;
      this.startCpuTimeNanos = UNSUPPORTED;
    }
  }
  
  /**
   * Get the usage information for the thread that created this {@link ThreadStats}.  The information will
   * track the thread's activity since the creation of this {@link ThreadStats} instance, though individual
   * metrics may be {@link #UNSUPPORTED} if the VM's tracking of them is disabled.
   * 
   * @return The usage information or an empty Optional if thread monitoring is unavailable in this VM.
   */
  public Optional<Usage> getUsage() {
    if (THREAD_MX_BEAN != null) {
      long allocatedBytes = this.startAllocatedBytes != UNSUPPORTED ?
          THREAD_MX_BEAN.getThreadAllocatedBytes(threadId) - this.startAllocatedBytes : UNSUPPORTED;
      long cpuTimeMs = this.startCpuTimeNanos != UNSUPPORTED ?
          TimeUnit.NANOSECONDS.toMillis(THREAD_MX_BEAN.getThreadCpuTime(threadId) - this.startCpuTimeNanos) : UNSUPPORTED;
      return Optional.of(new Usage(allocatedBytes, cpuTimeMs));
    } else {
      return Optional.empty();
    }
  }

  @Override
  public String toString() {
    return getUsage().toString();
  }

  /**
   * Usage information for a thread including the amount of time spent on the CPU and the amount of
   * memory allocated in the heap.
   */
  public static class Usage {
    public final long allocatedBytes;
    public final long cpuTimeMs;

    protected Usage(long allocatedBytes, long cpuTime) {
      this.allocatedBytes = allocatedBytes;
      this.cpuTimeMs = cpuTime;
    }

    @Override
    public String toString() {
      return "ThreadStats allocatedBytes : " + allocatedBytes + " cpuTimeMs : " + cpuTimeMs;
    }
  }
}
