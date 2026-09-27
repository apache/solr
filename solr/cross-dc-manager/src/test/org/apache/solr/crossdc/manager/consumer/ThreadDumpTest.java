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

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.lang.management.LockInfo;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verifies that {@link ThreadDump} helper pairs each locked {@link MonitorInfo} with the {@link
 * StackTraceElement} at the matching stack depth, including a monitor locked at stack depth 0. This
 * bug was inherited from the original Dropwizard implementation.
 */
public class ThreadDumpTest {

  @BeforeClass
  public static void beforeClass() {
    assumeWorkingMockito();
  }

  @Test
  public void testAllLockedMonitorsAreReported() {
    StackTraceElement frame0 = new StackTraceElement("com.example.Inner", "run", "Inner.java", 42);
    StackTraceElement frame1 =
        new StackTraceElement("com.example.Middle", "call", "Middle.java", 21);
    StackTraceElement frame2 = new StackTraceElement("com.example.Outer", "outer", "Outer.java", 7);
    StackTraceElement[] stackTrace = {frame0, frame1, frame2};

    // one monitor locked at the innermost frame (depth 0) and one at the outermost (depth 2);
    // depth 1 intentionally has no locked monitor.
    MonitorInfo monitorAtDepth0 = mock(MonitorInfo.class);
    when(monitorAtDepth0.getLockedStackDepth()).thenReturn(0);
    when(monitorAtDepth0.toString()).thenReturn("MONITOR_AT_DEPTH_0");

    MonitorInfo monitorAtDepth2 = mock(MonitorInfo.class);
    when(monitorAtDepth2.getLockedStackDepth()).thenReturn(2);
    when(monitorAtDepth2.toString()).thenReturn("MONITOR_AT_DEPTH_2");

    // monitor at depth 0 is deliberately first in the array, so the old loop (starting at
    // index 1) would silently drop it.
    MonitorInfo[] lockedMonitors = {monitorAtDepth0, monitorAtDepth2};

    ThreadInfo threadInfo = mock(ThreadInfo.class);
    when(threadInfo.getThreadName()).thenReturn("test-thread");
    when(threadInfo.getThreadId()).thenReturn(1L);
    when(threadInfo.getThreadState()).thenReturn(Thread.State.RUNNABLE);
    when(threadInfo.getLockInfo()).thenReturn(null);
    when(threadInfo.getLockOwnerName()).thenReturn(null);
    when(threadInfo.isSuspended()).thenReturn(false);
    when(threadInfo.isInNative()).thenReturn(false);
    when(threadInfo.getStackTrace()).thenReturn(stackTrace);
    when(threadInfo.getLockedMonitors()).thenReturn(lockedMonitors);
    when(threadInfo.getLockedSynchronizers()).thenReturn(new LockInfo[0]);

    ThreadMXBean threadMXBean = mock(ThreadMXBean.class);
    when(threadMXBean.dumpAllThreads(true, true)).thenReturn(new ThreadInfo[] {threadInfo});

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    new ThreadDump(threadMXBean).dump(out);

    List<String> lines = out.toString(StandardCharsets.UTF_8).lines().collect(Collectors.toList());

    int frame0Line = indexOfLineContaining(lines, frame0.toString());
    int frame1Line = indexOfLineContaining(lines, frame1.toString());
    int frame2Line = indexOfLineContaining(lines, frame2.toString());

    assertTrue("frame0 should be printed", frame0Line >= 0);
    assertTrue("frame1 should be printed", frame1Line >= 0);
    assertTrue("frame2 should be printed", frame2Line >= 0);

    // the monitor locked at depth 0 must be reported immediately after frame0's "at" line -
    // this is the case the off-by-one bug broke
    assertEquals(
        "monitor locked at stack depth 0 must immediately follow its frame",
        "      - locked MONITOR_AT_DEPTH_0",
        lines.get(frame0Line + 1));

    // depth 1 has no locked monitor, so the very next line must be the next stack frame.
    assertEquals(
        "no monitor line expected between frame1 and frame2",
        lines.get(frame2Line),
        lines.get(frame1Line + 1));

    // the monitor locked at depth 2 must immediately follow frame2's "at" line.
    assertEquals(
        "monitor locked at stack depth 2 must immediately follow its frame",
        "      - locked MONITOR_AT_DEPTH_2",
        lines.get(frame2Line + 1));
  }

  private static int indexOfLineContaining(List<String> lines, String needle) {
    for (int i = 0; i < lines.size(); i++) {
      if (lines.get(i).contains(needle)) {
        return i;
      }
    }
    return -1;
  }
}
