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
package org.apache.solr.client.solrj.impl;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.Configurable;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.impl.HttpSolrClient.Builder;
import org.apache.solr.client.solrj.request.RequestWriter;

/**
 * @deprecated Please look into using Solr's new Http2 clients
 */
@Deprecated(since = "9.0")
public abstract class SolrClientBuilder<B extends SolrClientBuilder<B>> {

  protected int timeToLiveSeconds = 60;
  protected HttpClient httpClient;
  protected ResponseParser responseParser;
  protected RequestWriter requestWriter;
  protected boolean useMultiPartPost;
  protected int connectionTimeoutMillis = 15000; // 15 seconds
  private boolean connectionTimeoutMillisUpdate = false;
  protected int socketTimeoutMillis = 120000; // 120 seconds
  private boolean socketTimeoutMillisUpdate = false;
  protected boolean followRedirects = false;
  protected Set<String> urlParamNames;

  /** The solution for the unchecked cast warning. */
  public abstract B getThis();

  /** Provides a {@link HttpClient} for the builder to use when creating clients. */
  public B withHttpClient(HttpClient httpClient) {
    this.httpClient = httpClient;

    if (this.httpClient instanceof Configurable) {
      RequestConfig conf = ((Configurable) httpClient).getConfig();
      // only update values that were not already manually changed
      if (!connectionTimeoutMillisUpdate && conf.getConnectTimeout() > 0) {
        this.connectionTimeoutMillis = conf.getConnectTimeout();
      }
      if (!socketTimeoutMillisUpdate && conf.getSocketTimeout() > 0) {
        this.socketTimeoutMillis = conf.getSocketTimeout();
      }
    }
    return getThis();
  }

  /** Provides a {@link ResponseParser} for created clients to use when handling requests. */
  public B withResponseParser(ResponseParser responseParser) {
    this.responseParser = responseParser;
    return getThis();
  }

  /** Provides a {@link RequestWriter} for created clients to use when handing requests. */
  public B withRequestWriter(RequestWriter requestWriter) {
    this.requestWriter = requestWriter;
    return getThis();
  }

  /** Enables or disables splitting POST requests into pieces. */
  public B allowMultiPartPost(Boolean useMultiPartPost) {
    this.useMultiPartPost = useMultiPartPost;
    return getThis();
  }

  /**
   * Provides a set of keys which the created client will send as a part of the query string.
   *
   * @param queryParams set of param keys to only send via the query string Note that the param will
   *     be sent as a query string if the key is part of this Set or the SolrRequest's query params.
   */
  public B withTheseParamNamesInTheUrl(Set<String> queryParams) {
    this.urlParamNames = queryParams;
    return getThis();
  }

  public B withFollowRedirects(boolean followRedirects) {
    this.followRedirects = followRedirects;
    return getThis();
  }

  /**
   * Tells {@link Builder} that created clients should obey the following timeout when connecting to
   * Solr servers.
   *
   * <p>For valid values see {@link org.apache.http.client.config.RequestConfig#getConnectTimeout()}
   *
   * @deprecated Please use {@link #withConnectionTimeout(int, TimeUnit)}
   */
  @Deprecated(since = "9.2")
  public B withConnectionTimeout(int connectionTimeoutMillis) {
    withConnectionTimeout(connectionTimeoutMillis, TimeUnit.MILLISECONDS);
    return getThis();
  }

  /**
   * Tells {@link Builder} that created clients should obey the following timeout when connecting to
   * Solr servers.
   *
   * <p>For valid values see {@link org.apache.http.client.config.RequestConfig#getConnectTimeout()}
   */
  public B withConnectionTimeout(int connectionTimeout, TimeUnit unit) {
    if (connectionTimeout < 0) {
      throw new IllegalArgumentException("connectionTimeout must be a non-negative integer.");
    }
    this.connectionTimeoutMillis =
        Math.toIntExact(TimeUnit.MILLISECONDS.convert(connectionTimeout, unit));
    connectionTimeoutMillisUpdate = true;
    return getThis();
  }

  public int getConnectionTimeoutMillis() {
    return this.connectionTimeoutMillis;
  }

  /**
   * Tells {@link Builder} that created clients should set the following read timeout on all
   * sockets.
   *
   * <p>For valid values see {@link org.apache.http.client.config.RequestConfig#getSocketTimeout()}
   *
   * <p>* @deprecated Please use {@link #withSocketTimeout(int, TimeUnit)}
   */
  @Deprecated(since = "9.2")
  public B withSocketTimeout(int socketTimeoutMillis) {
    withSocketTimeout(socketTimeoutMillis, TimeUnit.MILLISECONDS);
    return getThis();
  }

  /**
   * Tells {@link Builder} that created clients should set the following read timeout on all
   * sockets.
   *
   * <p>For valid values see {@link org.apache.http.client.config.RequestConfig#getSocketTimeout()}
   */
  public B withSocketTimeout(int socketTimeout, TimeUnit unit) {
    if (socketTimeout < 0) {
      throw new IllegalArgumentException("socketTimeout must be a non-negative integer.");
    }
    this.socketTimeoutMillis = Math.toIntExact(TimeUnit.MILLISECONDS.convert(socketTimeout, unit));
    socketTimeoutMillisUpdate = true;
    return getThis();
  }

  public int getSocketTimeoutMillis() {
    return this.socketTimeoutMillis;
  }
}
