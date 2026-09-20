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
package org.apache.solr.handler.admin;

import java.io.IOException;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import org.apache.solr.api.Api;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.model.NodeThreadsResponse;
import org.apache.solr.client.api.model.NodeThreadsResponse.LockOwner;
import org.apache.solr.client.api.model.NodeThreadsResponse.LockWaiting;
import org.apache.solr.client.api.model.NodeThreadsResponse.SystemInfo;
import org.apache.solr.client.api.model.NodeThreadsResponse.ThreadCount;
import org.apache.solr.client.api.model.NodeThreadsResponse.ThreadEntry;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.api.NodeThreadsAPI;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;

/**
 * @since solr 1.2
 */
public class ThreadDumpHandler extends RequestHandlerBase {

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, getThreadDump());
    rsp.setHttpCaching(false);
  }

  public static NodeThreadsResponse getThreadDump() {
    NodeThreadsResponse response = new NodeThreadsResponse();
    response.system = new SystemInfo();

    ThreadMXBean tmbean = ManagementFactory.getThreadMXBean();

    response.system.threadCount = new ThreadCount();
    response.system.threadCount.current = tmbean.getThreadCount();
    response.system.threadCount.peak = tmbean.getPeakThreadCount();
    response.system.threadCount.daemon = tmbean.getDaemonThreadCount();

    ThreadInfo[] tinfos;
    long[] tids = tmbean.findDeadlockedThreads();
    if (tids != null) {
      tinfos = tmbean.getThreadInfo(tids, Integer.MAX_VALUE);
      response.system.deadlocks = new ArrayList<>();
      for (ThreadInfo ti : tinfos) {
        if (ti != null) {
          response.system.deadlocks.add(getThreadInfo(ti, tmbean));
        }
      }
    }

    tinfos = tmbean.dumpAllThreads(true, true);
    response.system.threadDump = new ArrayList<>();
    for (ThreadInfo ti : tinfos) {
      if (ti != null) {
        response.system.threadDump.add(getThreadInfo(ti, tmbean));
      }
    }
    return response;
  }

  // --------------------------------------------------------------------------------
  // --------------------------------------------------------------------------------

  private static ThreadEntry getThreadInfo(ThreadInfo ti, ThreadMXBean tmbean) {
    ThreadEntry entry = new ThreadEntry();
    NodeThreadsResponse.ThreadInfo info = new NodeThreadsResponse.ThreadInfo();
    entry.thread = info;
    long tid = ti.getThreadId();

    info.id = tid;
    info.name = ti.getThreadName();
    info.state = ti.getThreadState().toString();

    if (ti.getLockName() != null) {
      // TODO: this is redundent with lock-waiting below .. deprecate & remove
      // TODO: (but first needs UI change)
      info.lock = ti.getLockName();
    }
    {
      final LockInfo lockInfo = ti.getLockInfo();
      if (null != lockInfo) {
        LockWaiting lock = new LockWaiting();
        info.lockWaiting = lock;
        lock.name = lockInfo.toString();
        if (-1 == ti.getLockOwnerId() && null == ti.getLockOwnerName()) {
          lock.owner = null;
        } else {
          LockOwner owner = new LockOwner();
          lock.owner = owner;
          owner.name = ti.getLockOwnerName();
          owner.id = ti.getLockOwnerId();
        }
      }
    }
    {
      final LockInfo[] synchronizers = ti.getLockedSynchronizers();
      if (0 < synchronizers.length) {
        final List<String> locks = new ArrayList<>(synchronizers.length);
        info.synchronizersLocked = locks;
        for (LockInfo sync : synchronizers) {
          locks.add(sync.toString());
        }
      }
    }
    {
      final LockInfo[] monitors = ti.getLockedMonitors();
      if (0 < monitors.length) {
        final List<String> locks = new ArrayList<>(monitors.length);
        info.monitorsLocked = locks;
        for (LockInfo monitor : monitors) {
          locks.add(monitor.toString());
        }
      }
    }

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

    // Add the stack trace
    int i = 0;
    String[] trace = new String[ti.getStackTrace().length];
    for (StackTraceElement ste : ti.getStackTrace()) {
      trace[i++] = ste.toString();
    }
    info.stackTrace = Arrays.asList(trace);
    return entry;
  }

  private static String formatNanos(long ns) {
    return String.format(Locale.ROOT, "%.4fms", ns / (double) 1000000);
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Thread Dump";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  @Override
  public Collection<Api> getApis() {
    return List.of();
  }

  @Override
  public Collection<Class<? extends JerseyResource>> getJerseyResources() {
    return List.of(NodeThreadsAPI.class);
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return Name.METRICS_READ_PERM;
  }
}
