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

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import javax.inject.Singleton;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.SolrVersion;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * JAX-RS "application" configurations for Solr's {@link org.apache.solr.core.CoreContainer} and
 * {@link SolrCore} instances
 */
@OpenAPIDefinition(
    info =
        @Info(
            title = "v2 API",
            description = "OpenAPI spec for Solr's v2 API endpoints",
            license = @License(name = "ASL 2.0"),
            version = SolrVersion.LATEST_STRING))
public class JerseyApplications {

  public static class CoreContainerApp extends ResourceConfig {
    public CoreContainerApp(PluginBag.JerseyMetricsLookupRegistry beanRegistry) {
      super();

      // Authentication and authorization
      register(SolrRequestAuthorizer.class);

      // Request and response serialization/deserialization
      // TODO: could these be singletons to save per-request object creations?
      register(MessageBodyWriters.JavabinMessageBodyWriter.class);
      register(MessageBodyWriters.XmlMessageBodyWriter.class);
      register(MessageBodyWriters.CsvMessageBodyWriter.class);
      register(SolrJacksonMapper.class);

      // Request lifecycle logic
      register(CatchAllExceptionMapper.class);
      register(NotFoundExceptionMapper.class);
      register(RequestMetricHandling.PreRequestMetricsFilter.class);
      register(RequestMetricHandling.PostRequestMetricsFilter.class);
      register(PostRequestDecorationFilter.class);
      register(
          new AbstractBinder() {
            @Override
            protected void configure() {
              bindFactory(new MetricBeanFactory(beanRegistry))
                  .to(PluginBag.JerseyMetricsLookupRegistry.class)
                  .in(Singleton.class);
            }
          });
      register(
          new AbstractBinder() {
            @Override
            protected void configure() {
              bindFactory(InjectionFactories.SolrQueryRequestFactory.class)
                  .to(SolrQueryRequest.class)
                  .in(RequestScoped.class);
            }
          });
      register(
          new AbstractBinder() {
            @Override
            protected void configure() {
              bindFactory(InjectionFactories.SolrQueryResponseFactory.class)
                  .to(SolrQueryResponse.class)
                  .in(RequestScoped.class);
            }
          });
      // Logging - disabled by default but useful for debugging Jersey execution
      //      setProperties(
      //          Map.of(
      //              "jersey.config.server.tracing.type",
      //              "ALL",
      //              "jersey.config.server.tracing.threshold",
      //              "VERBOSE"));
    }
  }

  public static class SolrCoreApp extends CoreContainerApp {

    public SolrCoreApp(SolrCore solrCore, PluginBag.JerseyMetricsLookupRegistry beanRegistry) {
      super(beanRegistry);

      // Dependency Injection for Jersey resources
      register(
          new AbstractBinder() {
            @Override
            protected void configure() {
              bindFactory(new InjectionFactories.SingletonFactory<>(solrCore))
                  .to(SolrCore.class)
                  .in(Singleton.class);
            }
          });
    }
  }
}
