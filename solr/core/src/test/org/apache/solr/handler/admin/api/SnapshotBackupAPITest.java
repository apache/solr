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

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.inject.Inject;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler.ReplicationHandlerConfig;
import org.apache.solr.jersey.InjectionFactories;
import org.apache.solr.jersey.SolrJacksonMapper;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link SnapshotBackupAPI}. */
public class SnapshotBackupAPITest extends JerseyTest {

  private SolrCore solrCore;
  private ReplicationHandlerConfig replicationHandlerConfig;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Override
  protected Application configure() {
    resetMocks();
    final ResourceConfig config = new ResourceConfig();
    config.register(TestSnapshotBackupAPI.class);
    config.register(SolrJacksonMapper.class);
    config.register(SolrExceptionTestMapper.class);
    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bindFactory(new InjectionFactories.SingletonFactory<>(solrCore))
                .to(SolrCore.class)
                .in(RequestScoped.class);
          }
        });
    config.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bindFactory(new InjectionFactories.SingletonFactory<>(replicationHandlerConfig))
                .to(ReplicationHandlerConfig.class)
                .in(RequestScoped.class);
          }
        });
    return config;
  }

  @Test
  public void testMissingBody() throws Exception {
    final Response response = target("/cores/demo/replication/backups").request().post(null);
    var status = response.getStatusInfo();
    assertEquals(400, status.getStatusCode());
    assertEquals("Required request-body is missing", status.getReasonPhrase());
  }

  @Test
  public void testSuccessfulBackupCommand() throws Exception {
    int numberToKeep = 7;
    int numberBackupsToKeep = 11;

    when(replicationHandlerConfig.getNumberBackupsToKeep()).thenReturn(numberBackupsToKeep);
    final Response response =
        target("/cores/demo/replication/backups")
            .request()
            .post(Entity.json("{\"name\": \"test\", \"numberToKeep\": " + numberToKeep + "}"));
    System.err.println("RESP " + response);

    assertEquals(numberToKeep, TestSnapshotBackupAPI.numberToKeep.get());
    assertEquals(numberBackupsToKeep, TestSnapshotBackupAPI.numberBackupsToKeep.get());
    assertEquals(200, response.getStatus());
  }

  private void resetMocks() {
    solrCore = mock(SolrCore.class);
    replicationHandlerConfig = mock(ReplicationHandlerConfig.class);
    when(replicationHandlerConfig.getNumberBackupsToKeep()).thenReturn(5);
  }

  public static class SolrExceptionTestMapper implements ExceptionMapper<SolrException> {
    @Override
    public Response toResponse(SolrException e) {
      return Response.status(e.code(), e.getMessage()).build();
    }
  }

  private static class TestSnapshotBackupAPI extends SnapshotBackupAPI {

    private static final AtomicInteger numberToKeep = new AtomicInteger();
    private static final AtomicInteger numberBackupsToKeep = new AtomicInteger();

    @Inject
    public TestSnapshotBackupAPI(
        SolrCore solrCore, ReplicationHandlerConfig replicationHandlerConfig) {
      super(solrCore, replicationHandlerConfig);
    }

    @Override
    protected void doSnapShoot(
        int numberToKeep,
        int numberBackupsToKeep,
        String location,
        String repoName,
        String commitName,
        String name,
        SolrCore solrCore,
        Consumer<NamedList<?>> resultConsumer)
        throws IOException {
      TestSnapshotBackupAPI.numberToKeep.set(numberToKeep);
      TestSnapshotBackupAPI.numberBackupsToKeep.set(numberBackupsToKeep);
    }
  }
}
