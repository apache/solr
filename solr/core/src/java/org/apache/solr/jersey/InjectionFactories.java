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

import static org.apache.solr.jersey.RequestContextKeys.SOLR_CORE;
import static org.apache.solr.jersey.RequestContextKeys.SOLR_PARAMS;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.api.ServiceLocator;

public class InjectionFactories {

  public static class SolrQueryRequestFactory extends RequestContextBasedFactory
      implements Factory<SolrQueryRequest> {

    @Override
    public SolrQueryRequest provide() {
      return (SolrQueryRequest)
          getRequestContext().getProperty(RequestContextKeys.SOLR_QUERY_REQUEST);
    }

    @Override
    public void dispose(SolrQueryRequest instance) {}
  }

  public static class SolrQueryResponseFactory extends RequestContextBasedFactory
      implements Factory<SolrQueryResponse> {

    @Override
    public SolrQueryResponse provide() {
      return (SolrQueryResponse)
          getRequestContext().getProperty(RequestContextKeys.SOLR_QUERY_RESPONSE);
    }

    @Override
    public void dispose(SolrQueryResponse instance) {}
  }

  /** Fetch the (existing) SolrCore from the request context */
  public static class ReuseFromContextSolrCoreFactory extends RequestContextBasedFactory
      implements Factory<SolrCore> {

    @Override
    public SolrCore provide() {
      return (SolrCore) getRequestContext().getProperty(SOLR_CORE);
    }

    @Override
    public void dispose(SolrCore instance) {}
  }

  public static class ReuseFromContextIndexSchemaFactory implements Factory<IndexSchema> {
    @Context ServiceLocator serviceLocator;

    @Override
    public IndexSchema provide() {
      return doProvide(serviceLocator.getService(SolrCore.class));
    }

    private IndexSchema doProvide(SolrCore solrCore) {
      return solrCore.getLatestSchema();
    }

    @Override
    public void dispose(IndexSchema instance) {}
  }

  public static class ReuseFromContextSolrParamsFactory extends RequestContextBasedFactory
      implements Factory<SolrParams> {

    @Override
    public SolrParams provide() {
      return (SolrParams) getRequestContext().getProperty(SOLR_PARAMS);
    }

    @Override
    public void dispose(SolrParams instance) {}
  }

  /**
   * Allows access to a {@link ContainerRequestContext} via a {@link ServiceLocator}
   *
   * <p>ServiceLocator must be used util https://github.com/eclipse-ee4j/jersey/issues/3503 is
   * resolved.
   */
  public static class RequestContextBasedFactory {
    @Context ServiceLocator serviceLocator;

    public ContainerRequestContext getRequestContext() {
      return serviceLocator.getService(ContainerRequestContext.class);
    }
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
