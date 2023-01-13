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

package org.apache.solr.jersey;

import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.glassfish.hk2.api.Factory;

public class InjectionFactories {
  public static class SolrQueryRequestFactory implements Factory<SolrQueryRequest> {

    private final ContainerRequestContext containerRequestContext;

    @Inject
    public SolrQueryRequestFactory(ContainerRequestContext containerRequestContext) {
      this.containerRequestContext = containerRequestContext;
    }

    @Override
    public SolrQueryRequest provide() {
      return (SolrQueryRequest)
          containerRequestContext.getProperty(RequestContextKeys.SOLR_QUERY_REQUEST);
    }

    @Override
    public void dispose(SolrQueryRequest instance) {}
  }

  public static class SolrQueryResponseFactory implements Factory<SolrQueryResponse> {

    private final ContainerRequestContext containerRequestContext;

    @Inject
    public SolrQueryResponseFactory(ContainerRequestContext containerRequestContext) {
      this.containerRequestContext = containerRequestContext;
    }

    @Override
    public SolrQueryResponse provide() {
      return (SolrQueryResponse)
          containerRequestContext.getProperty(RequestContextKeys.SOLR_QUERY_RESPONSE);
    }

    @Override
    public void dispose(SolrQueryResponse instance) {}
  }

  public static class SingletonFactory<T> implements Factory<T> {

    private final T singletonVal;

    public SingletonFactory(T singletonVal) {
      this.singletonVal = singletonVal;
    }

    @Override
    public T provide() {
      return singletonVal;
    }

    @Override
    public void dispose(T instance) {}
  }
}
