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
import java.util.Map;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.SolrVersion;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJsonProvider;
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
    public CoreContainerApp() {
      super();

      // Authentication and authorization
      register(SolrRequestAuthorizer.class);

      // Request and response serialization/deserialization
      // TODO: could these be singletons to save per-request object creations?
      register(MessageBodyWriters.JavabinMessageBodyWriter.class);
      register(MessageBodyWriters.XmlMessageBodyWriter.class);
      register(MessageBodyWriters.CsvMessageBodyWriter.class);
      register(JacksonJsonProvider.class);
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

      setProperties(
          Map.of(
              // Explicit Jersey logging is disabled by default but useful for debugging
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
            }
          });
    }
  }
}
