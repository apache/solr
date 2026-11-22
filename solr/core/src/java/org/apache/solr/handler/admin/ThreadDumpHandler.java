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

import static org.apache.solr.common.params.CommonParams.ID;
import static org.apache.solr.common.params.CommonParams.NAME;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.solr.api.Api;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.model.NodeThreadsResponse.ThreadEntry;
import org.apache.solr.client.api.model.NodeThreadsResponse.ThreadInfo;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.api.NodeThreadsAPI;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;

/**
 * @since solr 1.2
 */
public class ThreadDumpHandler extends RequestHandlerBase {

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
    final var response = new NodeThreadsAPI().getThreadDump();
    final var system = new SimpleOrderedMap<Object>();
    final var counts = new SimpleOrderedMap<Object>();
    counts.add("current", Math.toIntExact(response.system.threadCount.current));
    counts.add("peak", Math.toIntExact(response.system.threadCount.peak));
    counts.add("daemon", Math.toIntExact(response.system.threadCount.daemon));
    system.add("threadCount", counts);
    if (response.system.deadlocks != null) {
      system.add("deadlocks", toNamedList(response.system.deadlocks));
    }
    system.add("threadDump", toNamedList(response.system.threadDump));
    rsp.add("system", system);
    rsp.setHttpCaching(false);
  }

  private static NamedList<SimpleOrderedMap<Object>> toNamedList(List<ThreadEntry> threads) {
    final var result = new NamedList<SimpleOrderedMap<Object>>();
    for (var entry : threads) {
      result.add("thread", toNamedList(entry.thread));
    }
    return result;
  }

  private static SimpleOrderedMap<Object> toNamedList(ThreadInfo thread) {
    final var info = new SimpleOrderedMap<Object>();
    info.add(ID, thread.id);
    info.add(NAME, thread.name);
    info.add("state", thread.state);
    if (thread.lock != null) {
      info.add("lock", thread.lock);
    }
    if (thread.lockWaiting != null) {
      final var lock = new SimpleOrderedMap<Object>();
      lock.add(NAME, thread.lockWaiting.name);
      SimpleOrderedMap<Object> owner = null;
      if (thread.lockWaiting.owner != null) {
        owner = new SimpleOrderedMap<>();
        owner.add(NAME, thread.lockWaiting.owner.name);
        owner.add(ID, thread.lockWaiting.owner.id);
      }
      lock.add("owner", owner);
      info.add("lock-waiting", lock);
    }
    if (thread.synchronizersLocked != null) {
      info.add("synchronizers-locked", thread.synchronizersLocked);
    }
    if (thread.monitorsLocked != null) {
      info.add("monitors-locked", thread.monitorsLocked);
    }
    if (thread.suspended != null) {
      info.add("suspended", thread.suspended);
    }
    if (thread.nativeThread != null) {
      info.add("native", thread.nativeThread);
    }
    if (thread.cpuTime != null) {
      info.add("cpuTime", thread.cpuTime);
    }
    if (thread.userTime != null) {
      info.add("userTime", thread.userTime);
    }
    info.add("stackTrace", thread.stackTrace.toArray(String[]::new));
    return info;
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
