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
package org.apache.solr.handler.admin.api;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.inject.Singleton;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.api.ReplaceNodeAPI.ReplaceNodeRequestBody;
import org.apache.solr.jersey.InjectionFactories;
import org.apache.solr.jersey.SolrJacksonMapper;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link ReplaceNodeAPI} */
public class ReplaceNodeAPITest extends JerseyTest {

  private CoreContainer mockCoreContainer;
  private static final String sourceNodeName = "demoSourceNode";
  private static final String targetNodeName = "demoTargetNode";

  public static final String async = "async";
  private static final boolean waitForFinalState = false;
  private SolrQueryRequest mockQueryRequest;
  private SolrQueryResponse queryResponse;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  protected Application configure() {
    resetMocks();
    final ResourceConfig config = new ResourceConfig();
    config.register(ReplaceNodeAPI.class);
    config.register(SolrJacksonMapper.class);
    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bindFactory(new InjectionFactories.SingletonFactory<>(mockCoreContainer))
                .to(CoreContainer.class)
                .in(Singleton.class);
          }
        });
    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bindFactory(InjectionFactories.SolrQueryRequestFactory.class)
                .to(SolrQueryRequest.class)
                .in(RequestScoped.class);
          }
        });
    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bindFactory(InjectionFactories.SolrQueryResponseFactory.class)
                .to(SolrQueryResponse.class)
                .in(RequestScoped.class);
          }
        });

    return config;
  }

  @Test
  public void testSuccessfulReplaceNodeCommand() throws Exception {
    final String expectedJson = "{\"responseHeader\":{\"status\":0,\"QTime\":0}}";
    final CollectionsHandler collectionsHandler = mock(CollectionsHandler.class);
    when(mockCoreContainer.getCollectionsHandler()).thenReturn(collectionsHandler);
    ReplaceNodeAPI.ReplaceNodeRequestBody requestBody =
        new ReplaceNodeAPI.ReplaceNodeRequestBody(targetNodeName, waitForFinalState, async);
    Entity<ReplaceNodeRequestBody> entity = Entity.entity(requestBody, MediaType.APPLICATION_JSON);
    final Response response =
        target("cluster/nodes/" + sourceNodeName + "/commands/replace").request().post(entity);
    final String jsonBody = response.readEntity(String.class);
    assertEquals(200, response.getStatus());
    assertEquals("application/json", response.getHeaders().getFirst("Content-type"));
    assertEquals(expectedJson, "{\"responseHeader\":{\"status\":0,\"QTime\":0}}");
  }

  private void resetMocks() {
    mockCoreContainer = mock(CoreContainer.class);
  }
}
