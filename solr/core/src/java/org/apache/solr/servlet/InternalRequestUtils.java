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

package org.apache.solr.servlet;

import jakarta.servlet.http.HttpServletRequest;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.params.CommonParams;

/**
 * Shared detection for internal intra-cluster server-to-server requests. Filter-tier admission
 * control (e.g. {@link SolrQoSFilter}, {@link RateLimitManager}) consults this so sub-shard
 * requests are unconditionally admitted — refusing them would risk distributed deadlock, since the
 * parent request is already blocking on the sub-shard.
 *
 * <p>Mirrors {@code RequestHandlerBase.isInternalShardRequest} at the HTTP layer so the filter-tier
 * definition stays consistent with the handler-tier one.
 */
final class InternalRequestUtils {

  private InternalRequestUtils() {}

  /**
   * Whether {@code req} is an internal intra-cluster shard / admin request. Identified by any of:
   *
   * <ul>
   *   <li>{@code Solr-Request-Context: SERVER} header — set by SolrJ on server-to-server traffic.
   *   <li>{@code isShard=true} query parameter — set by {@code ShardHandler} on sub-shard fan-out.
   *   <li>{@code distrib.from=…} query parameter — set by {@code DistributedUpdateProcessor} when
   *       forwarding update requests between replicas.
   * </ul>
   */
  static boolean isInternalServerRequest(HttpServletRequest req) {
    String ctx = req.getHeader(CommonParams.SOLR_REQUEST_CONTEXT_PARAM);
    if (ctx != null && SolrRequest.SolrClientContext.SERVER.toString().equals(ctx)) {
      return true;
    }
    String qs = req.getQueryString();
    if (qs == null) {
      return false;
    }
    return qs.contains("isShard=true") || qs.contains("distrib.from=");
  }
}
