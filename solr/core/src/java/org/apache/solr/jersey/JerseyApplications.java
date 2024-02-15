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

import java.util.Map;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJsonProvider;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * JAX-RS "application" configurations for Solr's {@link org.apache.solr.core.CoreContainer} and
 * {@link SolrCore} instances
 */
public class JerseyApplications {

  public static class CoreContainerApp extends ResourceConfig {
    public CoreContainerApp() {
      super();

      // Authentication and authorization
      register(SolrRequestAuthorizer.class);

      // Request and response serialization/deserialization
      // TODO: could these be singletons to save per-request object creations?
      register(MessageBodyWriters.JavabinMessageBodyWriter.class);
      register(MessageBodyWriters.XmlMessageBodyWriter.class);
      register(MessageBodyWriters.CsvMessageBodyWriter.class);
      register(MessageBodyWriters.RawMessageBodyWriter.class);
      register(JacksonJsonProvider.class, 5);
      register(MessageBodyReaders.CachingJsonMessageBodyReader.class, 10);
      register(SolrJacksonMapper.class);

      // Request lifecycle logic
      register(CatchAllExceptionMapper.class);
      register(NotFoundExceptionMapper.class);
      register(MediaTypeOverridingFilter.class);
      register(RequestMetricHandling.PreRequestMetricsFilter.class);
      register(RequestMetricHandling.PostRequestMetricsFilter.class);
      register(PostRequestDecorationFilter.class);
      register(PostRequestLoggingFilter.class);
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

      // Explicit Jersey logging is disabled by default but useful for debugging (pt 1)
      // register(LoggingFeature.class);

      setProperties(
          Map.of(
              // Explicit Jersey logging is disabled by default but useful for debugging (pt 2)
              // "jersey.config.server.tracing.type", "ALL",
              // "jersey.config.server.tracing.threshold", "VERBOSE",
              "jersey.config.server.wadl.disableWadl", "true",
              "jersey.config.beanValidation.disable.server", "true",
              "jersey.config.server.disableAutoDiscovery", "true",
              "jersey.config.server.disableJsonProcessing", "true",
              "jersey.config.server.disableMetainfServicesLookup", "true",
              "jersey.config.server.disableMoxyJson", "true",
              "jersey.config.server.resource.validation.disable", "true"));
    }
  }

  public static class SolrCoreApp extends CoreContainerApp {

    public SolrCoreApp() {
      super();

      // Dependency Injection for Jersey resources
      register(
          new AbstractBinder() {
            @Override
            protected void configure() {
              bindFactory(InjectionFactories.ReuseFromContextSolrCoreFactory.class)
                  .to(SolrCore.class)
                  .in(RequestScoped.class);
              bindFactory(InjectionFactories.ReuseFromContextIndexSchemaFactory.class)
                  .to(IndexSchema.class)
                  .in(RequestScoped.class);
              bindFactory(InjectionFactories.ReuseFromContextSolrParamsFactory.class)
                  .to(SolrParams.class)
                  .in(RequestScoped.class);
            }
          });
    }
  }
}
