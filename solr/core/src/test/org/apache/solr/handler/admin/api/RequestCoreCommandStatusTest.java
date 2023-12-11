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
package org.apache.solr.handler.admin.api;

import java.util.Map;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.jersey.InjectionFactories;
import org.apache.solr.jersey.NotFoundExceptionMapper;
import org.apache.solr.jersey.SolrJacksonMapper;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/** Test for {@link RequestCoreCommandStatus}. */
public class RequestCoreCommandStatusTest extends JerseyTest {
  private CoreContainer coreContainer;
  private CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker;

  private CoreAdminHandler.CoreAdminAsyncTracker.TaskObject taskObject;

  private static SolrQueryRequest solrQueryRequest;

  @BeforeClass
  public static void ensureWorkingMockito() {
    SolrTestCaseJ4.assumeWorkingMockito();
  }

  @Override
  protected Application configure() {
    resetMocks();
    final var resourceConfig = new ResourceConfig();
    resourceConfig.register(SolrJacksonMapper.class);
    resourceConfig.register(RequestCoreCommandStatus.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bindFactory(new InjectionFactories.SingletonFactory<>(solrQueryRequest))
                .to(SolrQueryRequest.class)
                .in(RequestScoped.class);
          }
        });
    resourceConfig.register(
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
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bindFactory(new InjectionFactories.SingletonFactory<>(coreContainer))
                .to(CoreContainer.class)
                .in(RequestScoped.class);
          }
        });
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bindFactory(new InjectionFactories.SingletonFactory<>(coreAdminAsyncTracker))
                .to(CoreAdminHandler.CoreAdminAsyncTracker.class)
                .in(RequestScoped.class);
          }
        });
    return resourceConfig;
  }

  public void resetMocks() {
    coreContainer = Mockito.mock(CoreContainer.class);
    coreAdminAsyncTracker = Mockito.mock(CoreAdminHandler.CoreAdminAsyncTracker.class);
    taskObject = new CoreAdminHandler.CoreAdminAsyncTracker.TaskObject(null, null, false, null);
    solrQueryRequest = Mockito.mock(SolrQueryRequest.class);
  }

  @Test
  public void testRequestStatusCoreCommandTaskNotFound() {
    var taskNotFound = "NOTFOUND-1";
    final Response response =
        target("/cores/command-status/" + taskNotFound).request("application/json").get();
    final var responseStr = response.readEntity(String.class);
    System.out.println(responseStr);
    Assert.assertTrue(200 == response.getStatus());
    Assert.assertTrue(responseStr.contains("notfound"));
  }

  @Test
  public void testRequestStatusCoreCommand_ForRunningTask() {
    var runningTaskId = "RUNNING-1";
    Mockito.when(
            coreAdminAsyncTracker.getRequestStatusMap(
                CoreAdminHandler.CoreAdminAsyncTracker.RUNNING))
        .thenReturn(Map.of(runningTaskId, taskObject));
    final Response response =
        target("/cores/command-status/" + runningTaskId).request("application/json").get();
    final var responseStr = response.readEntity(String.class);
    Assert.assertTrue(200 == response.getStatus());
    Assert.assertTrue(responseStr.contains("running"));
  }

  @Test
  public void testRequestStatusCoreCommand_ForCompletedTask() {
    var completedTaskId = "COMPLETED-1";
    Mockito.when(
            coreAdminAsyncTracker.getRequestStatusMap(
                CoreAdminHandler.CoreAdminAsyncTracker.COMPLETED))
        .thenReturn(Map.of(completedTaskId, taskObject));
    final Response response =
        target("/cores/command-status/" + completedTaskId).request("application/json").get();
    final var responseStr = response.readEntity(String.class);
    Assert.assertTrue(200 == response.getStatus());
    Assert.assertTrue(responseStr.contains("completed"));
  }

  @Test
  public void testRequestStatusCoreCommand_ForFailedTask() {
    var failedTaskId = "FAILED-1";
    Mockito.when(
            coreAdminAsyncTracker.getRequestStatusMap(
                CoreAdminHandler.CoreAdminAsyncTracker.FAILED))
        .thenReturn(Map.of(failedTaskId, taskObject));
    final Response response =
        target("/cores/command-status/" + failedTaskId).request("application/json").get();
    final var responseStr = response.readEntity(String.class);
    Assert.assertTrue(200 == response.getStatus());
    Assert.assertTrue(responseStr.contains("failed"));
  }
}
