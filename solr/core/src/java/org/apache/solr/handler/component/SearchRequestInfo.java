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
package org.apache.solr.handler.component;

import java.util.List;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.ThreadCpuTimer;

/**
 * A completed search request, as passed to {@link SearchRequestListener}. Listeners must not retain
 * or modify {@code req}, {@code rsp} or {@code rb}; copy what they need.
 *
 * @param req the request
 * @param rsp the response
 * @param rb null if the handler failed before creating it
 * @param handlerPath the path the request log uses
 * @param elapsedMs time since the request started
 * @param cpuTimeMs null unless {@code solr.log.cputime=true}; includes shard CPU when distributed
 * @param hits null if not available
 * @param shardCount 0 for a non-distributed request
 * @param failure the exception set on the response, if any
 * @lucene.experimental
 */
public record SearchRequestInfo(
    SolrQueryRequest req,
    SolrQueryResponse rsp,
    ResponseBuilder rb,
    String handlerPath,
    long elapsedMs,
    Long cpuTimeMs,
    Long hits,
    int shardCount,
    Throwable failure) {

  static SearchRequestInfo from(SolrQueryRequest req, SolrQueryResponse rsp) {
    SolrRequestInfo requestInfo = SolrRequestInfo.getRequestInfo();
    ResponseBuilder rb = requestInfo == null ? null : requestInfo.getResponseBuilder();
    if (rb != null && rb.req != req) {
      rb = null;
    }
    boolean distributed = rb != null && rb.isDistributed() && rb.shards != null;
    return new SearchRequestInfo(
        req,
        rsp,
        rb,
        rsp.getToLog().get("path") instanceof String path ? path : null,
        (long) req.getRequestTimer().getTime(),
        cpuTimeMs(rsp, distributed),
        rsp.getToLog().get("hits") instanceof Number hits ? hits.longValue() : null,
        distributed ? rb.shards.length : 0,
        rsp.getException());
  }

  private static Long cpuTimeMs(SolrQueryResponse rsp, boolean distributed) {
    if (rsp.getResponseHeader() == null) {
      return null;
    }
    List<Object> cpuTimes = rsp.getResponseHeader().getAll(ThreadCpuTimer.CPU_TIME);
    if (cpuTimes.isEmpty() || !(cpuTimes.getLast() instanceof Long cpuTime)) {
      return null;
    }
    if (distributed && rsp.getToLog().get(ThreadCpuTimer.LOCAL_CPU_TIME) instanceof Long local) {
      return cpuTime + local;
    }
    return cpuTime;
  }
}
