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

import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * Notified once per search a client sends to a search handler, on the node that coordinated it.
 * Shard sub-requests (any request with {@code isShard=true}), real-time get, export and searches
 * Solr runs internally (ping, warming, {@code [subquery]}) are not reported. A search a streaming
 * expression sends to a replica is reported on that node. Searches through a container-level
 * handler in {@code EmbeddedSolrServer} and requests that fail the {@code echoParams} check are not
 * reported either.
 *
 * <p>{@link #capture} runs on the request thread; {@link #onRequestEnd} runs later on a single
 * delivery thread per {@link org.apache.solr.core.CoreContainer}, shared by all listeners through
 * one bounded queue. When the queue is full the search is dropped for every listener rather than
 * delaying the request, and counted in the {@code solr.search.listener.lost} metric. Size the queue
 * with {@code solr.search.listener.queue.size} (default 1000). One slow listener causes drops for
 * all of them. Queued searches are discarded at shutdown and counted the same way.
 *
 * <p>The delivery thread has no request MDC, so log lines from {@link #onRequestEnd} carry no trace
 * id; include {@link SearchRequestInfo#traceId()} where needed.
 *
 * @see org.apache.solr.core.CoreContainer#addSearchRequestListener(SearchRequestListener)
 * @lucene.experimental
 */
public interface SearchRequestListener {
  /**
   * Called on the request thread once the response header has its status and QTime, before the
   * response is written. Returns what {@link #onRequestEnd} needs, or null to skip this search.
   *
   * <p>Must be cheap and must not block, since it adds to the request's latency. Must not return or
   * hold on to {@code req}, {@code rsp}, {@code rb}, the searcher or a {@code DocList}: copy the
   * values instead. An {@link Exception} is logged and skips this listener for this search; an
   * {@link Error} fails the request.
   *
   * @return {@code info} by default
   */
  default Object capture(
      SearchRequestInfo info, SolrQueryRequest req, SolrQueryResponse rsp, ResponseBuilder rb) {
    return info;
  }

  /**
   * Called on the delivery thread with what {@link #capture} returned. Not called if the search was
   * dropped, or if this listener was removed while the search was queued; a listener removed and
   * added again still gets searches captured before it was re-added. Any {@link Throwable} is
   * logged.
   */
  void onRequestEnd(SearchRequestInfo info, Object captured);
}
