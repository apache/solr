package org.apache.solr.handler.replication;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.inject.Singleton;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler;
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

/** Unit test of v2 endpoints for Replication Handler backup command */
public class BackupAPITest extends JerseyTest {

  private CoreContainer mockCoreContainer;
  private ReplicationHandler replicationHandler;
  private static final String coreName = "demo";

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Override
  protected Application configure() {
    resetMocks();
    final ResourceConfig config = new ResourceConfig();
    config.register(BackupAPI.class);
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
  public void testSuccessfulBackupCommand() throws Exception {
    final String expectedJson = "{\"responseHeader\":{\"status\":0,\"QTime\":0}}";
    BackupAPI.BackupReplicationPayload payload =
        new BackupAPI.BackupReplicationPayload(null, null, 0, null, null);
    Entity<BackupAPI.BackupReplicationPayload> entity =
        Entity.entity(payload, MediaType.APPLICATION_JSON);
    final Response response =
        target("/cores/" + coreName + "/replication/backups").request().post(entity);
    final String jsonBody = response.readEntity(String.class);
    verify(replicationHandler)
        .handleRequestBody(any(SolrQueryRequest.class), any(SolrQueryResponse.class));
    assertEquals(200, response.getStatus());
    assertEquals("application/json", response.getHeaders().getFirst("Content-type"));
    assertEquals(expectedJson, "{\"responseHeader\":{\"status\":0,\"QTime\":0}}");
  }

  private void resetMocks() {
    mockCoreContainer = mock(CoreContainer.class);
    replicationHandler = mock(ReplicationHandler.class);
    final SolrCore solrCore = mock(SolrCore.class);
    when(mockCoreContainer.getCore(coreName)).thenReturn(solrCore);
    when(solrCore.getRequestHandler(ReplicationHandler.PATH)).thenReturn(replicationHandler);
  }
}
