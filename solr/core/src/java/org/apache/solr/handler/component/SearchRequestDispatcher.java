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

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Holds the {@link SearchRequestListener}s of a node and delivers each reported search to them. */
public class SearchRequestDispatcher {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String MARKER = SearchRequestDispatcher.class.getName();

  private record Marker(ResponseBuilder rb) {}

  private final List<SearchRequestListener> listeners = new CopyOnWriteArrayList<>();

  public void add(SearchRequestListener listener) {
    listeners.add(listener);
  }

  public boolean remove(SearchRequestListener listener) {
    return listeners.remove(listener);
  }

  public List<SearchRequestListener> listeners() {
    return Collections.unmodifiableList(listeners);
  }

  static void mark(SolrQueryRequest req, ResponseBuilder rb) {
    req.getContext().put(MARKER, new Marker(rb));
  }

  /** Drops the report a {@link SearchHandler} requested for {@code req}, if any. */
  public static void unmark(SolrQueryRequest req) {
    req.getContext().remove(MARKER);
  }

  /**
   * Reports {@code req} to the listeners if a {@link SearchHandler} marked it. Called once the
   * response header has its status and QTime.
   */
  public void afterResponse(SolrQueryRequest req, SolrQueryResponse rsp, long qTimeMs) {
    if (!(req.getContext().remove(MARKER) instanceof Marker marker)) {
      return;
    }
    SearchRequestInfo info = SearchRequestInfo.from(req, rsp, marker.rb(), qTimeMs);
    for (SearchRequestListener listener : listeners) {
      Object captured;
      try {
        captured = listener.capture(info, req, rsp, marker.rb());
      } catch (Exception e) {
        log.warn("SearchRequestListener {} capture failed", listener.getClass().getName(), e);
        continue;
      }
      if (captured == null) {
        continue;
      }
      try {
        listener.onRequestEnd(info, captured);
      } catch (Exception e) {
        log.warn("SearchRequestListener {} failed", listener.getClass().getName(), e);
      }
    }
  }
}
