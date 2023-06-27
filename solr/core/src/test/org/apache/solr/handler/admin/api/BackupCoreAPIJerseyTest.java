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
import static org.apache.solr.jersey.RequestContextKeys.SOLR_QUERY_RESPONSE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.inject.Singleton;
import javax.ws.rs.client.Entity;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.CatchAllExceptionMapper;
import org.apache.solr.jersey.InjectionFactories;
import org.apache.solr.jersey.SolrJacksonMapper;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class BackupCoreAPIJerseyTest extends JerseyTest {

  CoreContainer mockCoreContainer;
  private static final String CORE_NAME = "demo";

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Override
  public Application configure() {
    resetMocks();
    final ResourceConfig config = new ResourceConfig();
    config.register(BackupCoreAPI.class);
    config.register(SolrJacksonMapper.class);
    config.register(CatchAllExceptionMapper.class);
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
            bindFactory(
                    new Factory<SolrQueryRequest>() {
                      @Override
                      public SolrQueryRequest provide() {
                        return new SolrQueryRequestBase(
                            mockCoreContainer.getCore(CORE_NAME), new ModifiableSolrParams()) {};
                      }

                      @Override
                      public void dispose(SolrQueryRequest instance) {}
                    })
                .to(SolrQueryRequest.class)
                .in(RequestScoped.class);
          }
        });
    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bindFactory(
                    new Factory<SolrQueryResponse>() {
                      @Override
                      public SolrQueryResponse provide() {
                        return new SolrQueryResponse();
                      }

                      @Override
                      public void dispose(SolrQueryResponse instance) {}
                    })
                .to(SolrQueryResponse.class)
                .in(RequestScoped.class);
          }
        });
    return config;
  }
  // @Test
  public void testMissingRequiredParamsResultIn400ForBackup() {
    BackupCoreAPI.BackupCoreRequestBody backupCoreRequestBody =
        new BackupCoreAPI.BackupCoreRequestBody();
    Entity<BackupCoreAPI.BackupCoreRequestBody> entity =
        Entity.entity(backupCoreRequestBody, MediaType.APPLICATION_JSON);
    final Response response =
        target("/cores/" + CORE_NAME + "/backup/Backup_DEMO").request().post(entity);
    final String jsonBody = response.readEntity(String.class);
    System.out.println(jsonBody);
  }

   @Test
  public void testMissingRequiredParameterResultIn400ForIncrementalBackup() {
    BackupCoreAPI.BackupCoreRequestBody backupCoreRequestBody =
        new BackupCoreAPI.BackupCoreRequestBody();
    Entity<BackupCoreAPI.BackupCoreRequestBody> entity =
        Entity.entity(backupCoreRequestBody, MediaType.APPLICATION_JSON);
    final Response response =
        target("/cores/" + CORE_NAME + "/backup/Backup_DEMO/incremental").request().post(entity);
    final String jsonBody = response.readEntity(String.class);
    System.out.println(jsonBody);
    assertEquals(400, response.getStatus());
  }

  private void resetMocks() {
    mockCoreContainer = mock(CoreContainer.class);
    ContainerRequestContext containerRequestContext = mock(ContainerRequestContext.class);
    when(containerRequestContext.getProperty(SOLR_QUERY_RESPONSE))
        .thenReturn(new SolrQueryResponse());
    // final SolrCore solrCore = mock(SolrCore.class);
    // when(mockCoreContainer.getCore(coreName)).thenReturn(solrCore);
    // when(solrCore.getRequestHandler(ReplicationHandler.PATH)).thenReturn(replicationHandler);
  }
}
