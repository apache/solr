/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.security;

import java.security.Principal;
import java.util.Enumeration;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.servlet.ServletUtils;

/**
 * An {@link AuthorizationContext} implementation that delegates many methods to an underlying
 * {@link HttpServletRequest}
 */
public abstract class HttpServletAuthorizationContext extends AuthorizationContext {

  private final HttpServletRequest servletRequest;

  public HttpServletAuthorizationContext(HttpServletRequest servletRequest) {
    this.servletRequest = servletRequest;
  }

  @Override
  public abstract SolrParams getParams();

  @Override
  public abstract List<CollectionRequest> getCollectionRequests();

  @Override
  public abstract RequestType getRequestType();

  @Override
  public abstract Object getHandler();

  @Override
  public String getResource() {
    return ServletUtils.getPathAfterContext(servletRequest);
  }

  @Override
  public String getRemoteAddr() {
    return servletRequest.getRemoteAddr();
  }

  @Override
  public Principal getUserPrincipal() {
    return servletRequest.getUserPrincipal();
  }

  @Override
  public String getUserName() {
    return servletRequest.getRemoteUser();
  }

  @Override
  public String getHttpHeader(String s) {
    return servletRequest.getHeader(s);
  }

  @Override
  public Enumeration<String> getHeaderNames() {
    return servletRequest.getHeaderNames();
  }

  @Override
  public String getHttpMethod() {
    return servletRequest.getMethod();
  }

  @Override
  public String getRemoteHost() {
    return servletRequest.getRemoteHost();
  }
}
