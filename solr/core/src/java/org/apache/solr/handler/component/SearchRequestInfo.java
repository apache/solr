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

import java.security.Principal;
import java.util.List;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.ThreadCpuTimer;

/**
 * A completed search request, as passed to {@link SearchRequestListener}. It holds only values
 * copied from the request, so it can be kept and read on any thread.
 *
 * @param handlerPath the path the request log uses
 * @param collection null in standalone mode
 * @param core the name of the core that coordinated the search
 * @param user the authenticated user, or null
 * @param traceId the trace id, or null
 * @param startTimeMs when the request started, in epoch milliseconds
 * @param qTimeMs the QTime in the response header
 * @param cpuTimeMs null unless {@code solr.log.cputime=true}; includes the CPU shards reported when
 *     distributed
 * @param hits null if not available
 * @param shardCount number of shards targeted; 0 for a non-distributed request
 * @param failureType the class name of the exception set on the response, or null
 * @param failureMessage its message, cut to {@link #MAX_FAILURE_MESSAGE_CHARS}, or null
 * @param partialResults whether the response header marks the results as partial, for example after
 *     a timeout
 * @lucene.experimental
 */
public record SearchRequestInfo(
    String handlerPath,
    String collection,
    String core,
    String user,
    String traceId,
    long startTimeMs,
    long qTimeMs,
    Long cpuTimeMs,
    Long hits,
    int shardCount,
    String failureType,
    String failureMessage,
    boolean partialResults) {

  public static final int MAX_FAILURE_MESSAGE_CHARS = 256;

  static SearchRequestInfo from(
      SolrQueryRequest req, SolrQueryResponse rsp, ResponseBuilder rb, long qTimeMs) {
    boolean distributed = rb.isDistributed() && rb.shards != null;
    Principal user = req.getUserPrincipal();
    Exception failure = rsp.getException();
    return new SearchRequestInfo(
        rsp.getToLog().get("path") instanceof String path ? path : null,
        req.getCore().getCoreDescriptor().getCollectionName(),
        req.getCore().getName(),
        user == null ? null : user.getName(),
        MDCLoggingContext.getTraceId(),
        req.getStartTime(),
        qTimeMs,
        cpuTimeMs(rsp),
        rsp.getToLog().get("hits") instanceof Number hits ? hits.longValue() : null,
        distributed ? rb.shards.length : 0,
        failure == null ? null : failure.getClass().getName(),
        failure == null ? null : truncate(failure.getMessage()),
        SolrQueryResponse.isPartialResults(rsp.getResponseHeader()));
  }

  private static String truncate(String message) {
    return message == null || message.length() <= MAX_FAILURE_MESSAGE_CHARS
        ? message
        : message.substring(0, MAX_FAILURE_MESSAGE_CHARS);
  }

  private static Long cpuTimeMs(SolrQueryResponse rsp) {
    if (!(rsp.getToLog().get(ThreadCpuTimer.LOCAL_CPU_TIME) instanceof Long local)) {
      return null;
    }
    List<Object> shardCpuTimes = rsp.getToLog().getAll(ThreadCpuTimer.CPU_TIME);
    if (!shardCpuTimes.isEmpty() && shardCpuTimes.getLast() instanceof Long shardCpuTime) {
      return shardCpuTime + local;
    }
    return local;
  }
}
