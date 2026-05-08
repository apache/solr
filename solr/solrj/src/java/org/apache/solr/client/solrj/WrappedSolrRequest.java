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
package org.apache.solr.client.solrj;

import java.io.IOException;
import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.ResponseParser;
import org.apache.solr.client.solrj.response.StreamingResponseCallback;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;

/** A {@link SolrRequest} that wrappeds all method calls to a wrapped instance. */
public class WrappedSolrRequest<T> extends SolrRequest<T> {

  protected final SolrRequest<T> wrapped;

  public WrappedSolrRequest(SolrRequest<T> wrapped) {
    super(wrapped.getMethod(), wrapped.getPath(), wrapped.getRequestType());
    this.wrapped = wrapped;
  }

  // -- Abstract methods --

  @Override
  public SolrParams getParams() {
    return wrapped.getParams();
  }

  @Override
  protected T createResponse(NamedList<Object> namedList) {
    return wrapped.createResponse(namedList);
  }

  // -- Overrides for all non-final methods with private state in SolrRequest --

  @Override
  public METHOD getMethod() {
    return wrapped.getMethod();
  }

  @Override
  public void setMethod(METHOD method) {
    wrapped.setMethod(method);
  }

  @Override
  public String getPath() {
    return wrapped.getPath();
  }

  @Override
  public void setPath(String path) {
    wrapped.setPath(path);
  }

  @Override
  public ResponseParser getResponseParser() {
    return wrapped.getResponseParser();
  }

  @Override
  public void setResponseParser(ResponseParser responseParser) {
    wrapped.setResponseParser(responseParser);
  }

  @Override
  public StreamingResponseCallback getStreamingResponseCallback() {
    return wrapped.getStreamingResponseCallback();
  }

  @Override
  public void setStreamingResponseCallback(StreamingResponseCallback callback) {
    wrapped.setStreamingResponseCallback(callback);
  }

  @Override
  public Set<String> getQueryParams() {
    return wrapped.getQueryParams();
  }

  @Override
  public void setQueryParams(Set<String> queryParams) {
    wrapped.setQueryParams(queryParams);
  }

  @Override
  public SolrRequestType getRequestType() {
    return wrapped.getRequestType();
  }

  @Override
  public void setRequestType(SolrRequestType requestType) {
    wrapped.setRequestType(requestType);
  }

  @Override
  public List<String> getPreferredNodes() {
    return wrapped.getPreferredNodes();
  }

  @Override
  public SolrRequest<T> setPreferredNodes(List<String> nodes) {
    wrapped.setPreferredNodes(nodes);
    return this;
  }

  @Override
  public Principal getUserPrincipal() {
    return wrapped.getUserPrincipal();
  }

  @Override
  public void setUserPrincipal(Principal userPrincipal) {
    wrapped.setUserPrincipal(userPrincipal);
  }

  @Override
  public SolrRequest<T> setBasicAuthCredentials(String user, String password) {
    wrapped.setBasicAuthCredentials(user, password);
    return this;
  }

  @Override
  public String getBasicAuthUser() {
    return wrapped.getBasicAuthUser();
  }

  @Override
  public String getBasicAuthPassword() {
    return wrapped.getBasicAuthPassword();
  }

  @Override
  public boolean requiresCollection() {
    return wrapped.requiresCollection();
  }

  @Override
  public ApiVersion getApiVersion() {
    return wrapped.getApiVersion();
  }

  @Override
  @Deprecated
  public Collection<ContentStream> getContentStreams() throws IOException {
    return wrapped.getContentStreams();
  }

  @Override
  public RequestWriter.ContentWriter getContentWriter(String expectedType) {
    return wrapped.getContentWriter(expectedType);
  }

  @Override
  public String getCollection() {
    return wrapped.getCollection();
  }

  @Override
  public void addHeader(String key, String value) {
    wrapped.addHeader(key, value);
  }

  @Override
  public void addHeaders(Map<String, String> headers) {
    wrapped.addHeaders(headers);
  }

  @Override
  public Map<String, String> getHeaders() {
    return wrapped.getHeaders();
  }
}
