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
package org.apache.solr.handler.admin.api;

import static org.apache.solr.security.PermissionNameProvider.Name.METRICS_READ_PERM;

import jakarta.inject.Inject;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.NodeThreadsApi;
import org.apache.solr.client.api.model.NodeThreadsResponse;
import org.apache.solr.client.api.model.NodeThreadsResponse.LockOwner;
import org.apache.solr.client.api.model.NodeThreadsResponse.LockWaiting;
import org.apache.solr.client.api.model.NodeThreadsResponse.SystemInfo;
import org.apache.solr.client.api.model.NodeThreadsResponse.ThreadCount;
import org.apache.solr.client.api.model.NodeThreadsResponse.ThreadEntry;
import org.apache.solr.jersey.PermissionName;

/** Implementation of {@link NodeThreadsApi}. */
public class NodeThreadsAPI extends JerseyResource implements NodeThreadsApi {

  @Inject
  public NodeThreadsAPI() {}

  @Override
  @PermissionName(METRICS_READ_PERM)
  public NodeThreadsResponse getThreadDump() {
    final var response = instantiateJerseyResponse(NodeThreadsResponse.class);
    final var tmbean = ManagementFactory.getThreadMXBean();
    response.system = new SystemInfo();
    final var counts = new ThreadCount();
    counts.current = tmbean.getThreadCount();
    counts.peak = tmbean.getPeakThreadCount();
    counts.daemon = tmbean.getDaemonThreadCount();
    response.system.threadCount = counts;
    final long[] deadlockedThreads = tmbean.findDeadlockedThreads();
    if (deadlockedThreads != null) {
      response.system.deadlocks =
          toThreadEntries(tmbean.getThreadInfo(deadlockedThreads, Integer.MAX_VALUE), tmbean);
    }
    response.system.threadDump = toThreadEntries(tmbean.dumpAllThreads(true, true), tmbean);
    return response;
  }

  private static List<ThreadEntry> toThreadEntries(ThreadInfo[] threads, ThreadMXBean tmbean) {
    final List<ThreadEntry> entries = new ArrayList<>(threads.length);
    for (var thread : threads) {
      if (thread != null) {
        final var entry = new ThreadEntry();
        entry.thread = getThreadInfo(thread, tmbean);
        entries.add(entry);
      }
    }
    return entries;
  }

  private static NodeThreadsResponse.ThreadInfo getThreadInfo(ThreadInfo ti, ThreadMXBean tmbean) {
    final var info = new NodeThreadsResponse.ThreadInfo();
    final long tid = ti.getThreadId();
    info.id = tid;
    info.name = ti.getThreadName();
    info.state = ti.getThreadState().toString();
    info.lock = ti.getLockName();
    final LockInfo lockInfo = ti.getLockInfo();
    if (lockInfo != null) {
      info.lockWaiting = new LockWaiting();
      info.lockWaiting.name = lockInfo.toString();
      if (ti.getLockOwnerId() != -1 || ti.getLockOwnerName() != null) {
        info.lockWaiting.owner = new LockOwner();
        info.lockWaiting.owner.name = ti.getLockOwnerName();
        info.lockWaiting.owner.id = ti.getLockOwnerId();
      }
    }
    info.synchronizersLocked = toLockNames(ti.getLockedSynchronizers());
    info.monitorsLocked = toLockNames(ti.getLockedMonitors());
    if (ti.isSuspended()) {
      info.suspended = true;
    }
    if (ti.isInNative()) {
      info.nativeThread = true;
    }
    if (tmbean.isThreadCpuTimeSupported()) {
      info.cpuTime = formatNanos(tmbean.getThreadCpuTime(tid));
      info.userTime = formatNanos(tmbean.getThreadUserTime(tid));
    }
    info.stackTrace = new ArrayList<>(ti.getStackTrace().length);
    for (StackTraceElement ste : ti.getStackTrace()) {
      info.stackTrace.add(ste.toString());
    }
    return info;
  }

  private static List<String> toLockNames(LockInfo[] locks) {
    if (locks.length == 0) {
      return null;
    }
    final List<String> names = new ArrayList<>(locks.length);
    for (LockInfo lock : locks) {
      names.add(lock.toString());
    }
    return names;
  }

  private static String formatNanos(long ns) {
    return String.format(Locale.ROOT, "%.4fms", ns / (double) 1000000);
  }
}
