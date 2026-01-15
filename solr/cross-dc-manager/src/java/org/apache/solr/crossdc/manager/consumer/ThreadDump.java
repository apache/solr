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
package org.apache.solr.crossdc.manager.consumer;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.management.LockInfo;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.charset.StandardCharsets;

/**
 * A convenience class for getting a thread dump.
 *
 * <p>Copy of the code in <code>https://github.com/dropwizard/metrics/blob/release/5.0.x/metrics-jvm/src/main/java/io/dropwizard/metrics5/jvm/ThreadDump.java</code></p>
 */
public class ThreadDump {
  private final ThreadMXBean threadMXBean;

  public ThreadDump(ThreadMXBean threadMXBean) {
    this.threadMXBean = threadMXBean;
  }

  /**
   * Dumps all of the threads' current information, including synchronization, to an output stream.
   *
   * @param out an output stream
   */
  public void dump(OutputStream out) {
    dump(true, true, out);
  }

  /**
   * Dumps all of the threads' current information, optionally including synchronization, to an output stream.
   *
   * Having control over including synchronization info allows using this method (and its wrappers, i.e.
   * ThreadDumpServlet) in environments where getting object monitor and/or ownable synchronizer usage is not
   * supported. It can also speed things up.
   *
   * See {@link ThreadMXBean#dumpAllThreads(boolean, boolean)}
   *
   * @param lockedMonitors dump all locked monitors if true
   * @param lockedSynchronizers dump all locked ownable synchronizers if true
   * @param out an output stream
   */
  public void dump(boolean lockedMonitors, boolean lockedSynchronizers, OutputStream out) {
    final ThreadInfo[] threads = this.threadMXBean.dumpAllThreads(lockedMonitors, lockedSynchronizers);
    final PrintWriter writer = new PrintWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));

    for (int ti = threads.length - 1; ti >= 0; ti--) {
      final ThreadInfo t = threads[ti];
      writer.printf("\"%s\" id=%d state=%s",
          t.getThreadName(),
          t.getThreadId(),
          t.getThreadState());
      final LockInfo lock = t.getLockInfo();
      if (lock != null && t.getThreadState() != Thread.State.BLOCKED) {
        writer.printf("%n    - waiting on <0x%08x> (a %s)",
            lock.getIdentityHashCode(),
            lock.getClassName());
        writer.printf("%n    - locked <0x%08x> (a %s)",
            lock.getIdentityHashCode(),
            lock.getClassName());
      } else if (lock != null && t.getThreadState() == Thread.State.BLOCKED) {
        writer.printf("%n    - waiting to lock <0x%08x> (a %s)",
            lock.getIdentityHashCode(),
            lock.getClassName());
      }

      if (t.isSuspended()) {
        writer.print(" (suspended)");
      }

      if (t.isInNative()) {
        writer.print(" (running in native)");
      }

      writer.println();
      if (t.getLockOwnerName() != null) {
        writer.printf("     owned by %s id=%d%n", t.getLockOwnerName(), t.getLockOwnerId());
      }

      final StackTraceElement[] elements = t.getStackTrace();
      final MonitorInfo[] monitors = t.getLockedMonitors();

      for (int i = 0; i < elements.length; i++) {
        final StackTraceElement element = elements[i];
        writer.printf("    at %s%n", element);
        for (int j = 1; j < monitors.length; j++) {
          final MonitorInfo monitor = monitors[j];
          if (monitor.getLockedStackDepth() == i) {
            writer.printf("      - locked %s%n", monitor);
          }
        }
      }
      writer.println();

      final LockInfo[] locks = t.getLockedSynchronizers();
      if (locks.length > 0) {
        writer.printf("    Locked synchronizers: count = %d%n", locks.length);
        for (LockInfo l : locks) {
          writer.printf("      - %s%n", l);
        }
        writer.println();
      }
    }

    writer.println();
    writer.flush();
  }

}