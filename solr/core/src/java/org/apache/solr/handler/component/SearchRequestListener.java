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

/**
 * Notified once per search a client sends to a search handler, on the node that coordinated it.
 * Shard sub-requests, real-time get, export and searches Solr runs internally (ping, warming,
 * {@code [subquery]}) are not reported. A search a streaming expression sends to a replica is
 * reported on that node.
 *
 * @see org.apache.solr.core.CoreContainer#getSearchRequestListeners()
 * @lucene.experimental
 */
public interface SearchRequestListener {
  /**
   * Called on the request thread after the search, before the response header gets status and QTime
   * and before the response is written. An {@link Exception} is logged and the next listener still
   * runs; an {@link Error} propagates, failing the request and skipping later listeners.
   */
  void onRequestEnd(SearchRequestInfo info);
}
