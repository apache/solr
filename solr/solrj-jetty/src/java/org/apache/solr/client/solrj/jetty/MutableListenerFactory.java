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
package org.apache.solr.client.solrj.jetty;

/**
 * A {@link HttpListenerFactory} whose delegate can be repointed after construction, for the case
 * where the actual listener isn't known yet when a {@link HttpJettySolrClient} is built (or may
 * need to change later) but the client's identity is already relied on elsewhere, so it can't
 * simply be rebuilt and swapped out. Register one instance via {@link
 * HttpJettySolrClient.Builder#addListenerFactory} at construction time, then call {@link
 * #setDelegate} on that same instance whenever the real listener becomes available -- the client
 * itself stays immutable.
 *
 * <p>Repointing the delegate is a single volatile write, safe to call from a different thread than
 * the ones invoking {@link #get()} concurrently, and replaces rather than accumulates: calling
 * {@link #setDelegate} again (e.g. on a security.json reload) does not leave the previous listener
 * still registered.
 */
public final class MutableListenerFactory implements HttpListenerFactory {
  private static final RequestResponseListener NO_OP = new RequestResponseListener() {};

  private volatile HttpListenerFactory delegate = () -> NO_OP;

  public void setDelegate(HttpListenerFactory delegate) {
    this.delegate = delegate;
  }

  @Override
  public RequestResponseListener get() {
    return delegate.get();
  }
}
