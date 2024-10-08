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

import io.opentracing.propagation.TextMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.http.HttpRequest;

/** An OpenTracing Carrier for injecting Span context through an Apache HttpRequest */
public class SolrApacheHttpRequestCarrier implements TextMap {

  private final HttpRequest httpRequest;

  public SolrApacheHttpRequestCarrier(HttpRequest httpRequest) {
    this.httpRequest = httpRequest;
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    throw new UnsupportedOperationException("carrier is write-only");
  }

  @Override
  public void put(String key, String value) {
    httpRequest.setHeader(key, value);
  }
}
