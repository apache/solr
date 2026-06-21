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

package org.apache.solr.util.tracing;

import java.util.Iterator;
import java.util.Map;

import io.opentracing.propagation.TextMap;
import org.eclipse.jetty.client.api.Request;

/**
 * An OpenTracing Carrier for injecting Span context onto a Jetty client {@link Request} (used by the
 * inter-node proxy / forward path in {@code SolrRequestForwarder}). Without it a proxied request
 * (e.g. a non-leader node forwarding an update to the shard leader) starts a fresh trace on the
 * target node instead of continuing the proxying node's trace.
 */
public class JettyRequestCarrier implements TextMap {

  private final Request request;

  public JettyRequestCarrier(Request request) {
    this.request = request;
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    throw new UnsupportedOperationException("carrier is write-only");
  }

  @Override
  public void put(String key, String value) {
    // Replace rather than append: copyRequestHeaders may have already carried trace-context headers
    // from the original client request; the injected (proxying-node) span supersedes them.
    request.headers(httpFields -> {
      httpFields.remove(key);
      httpFields.add(key, value);
    });
  }
}
