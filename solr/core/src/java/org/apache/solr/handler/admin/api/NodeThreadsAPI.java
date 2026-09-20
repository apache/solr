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
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.NodeThreadsApi;
import org.apache.solr.client.api.model.NodeThreadsResponse;
import org.apache.solr.client.api.model.NodeThreadsResponse.SystemInfo;
import org.apache.solr.client.api.model.NodeThreadsResponse.ThreadCount;
import org.apache.solr.client.api.model.NodeThreadsResponse.ThreadEntry;
import org.apache.solr.client.api.model.NodeThreadsResponse.ThreadInfo;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.admin.ThreadDumpHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJacksonMapper;

/** Implementation of {@link NodeThreadsApi}. */
public class NodeThreadsAPI extends JerseyResource implements NodeThreadsApi {

  @Inject
  public NodeThreadsAPI() {}

  @Override
  @PermissionName(METRICS_READ_PERM)
  public NodeThreadsResponse getThreadDump() {
    final var response = instantiateJerseyResponse(NodeThreadsResponse.class);
    final var system = ThreadDumpHandler.getThreadDump();
    response.system = new SystemInfo();
    response.system.threadCount =
        SolrJacksonMapper.getObjectMapper()
            .convertValue(((NamedList<?>) system.get("threadCount")).asMap(1), ThreadCount.class);
    response.system.threadDump = toThreadEntries((NamedList<?>) system.get("threadDump"));
    if (system.get("deadlocks") instanceof NamedList<?> deadlocks) {
      response.system.deadlocks = toThreadEntries(deadlocks);
    }
    return response;
  }

  private static List<ThreadEntry> toThreadEntries(NamedList<?> threads) {
    final List<ThreadEntry> entries = new ArrayList<>(threads.size());
    for (var thread : threads) {
      final var entry = new ThreadEntry();
      entry.thread =
          SolrJacksonMapper.getObjectMapper()
              .convertValue(((NamedList<?>) thread.getValue()).asMap(3), ThreadInfo.class);
      entries.add(entry);
    }
    return entries;
  }
}
