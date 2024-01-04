package org.apache.solr.jersey;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.glassfish.hk2.api.Factory;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.process.internal.RequestScoped;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.BeforeClass;
import org.mockito.Mockito;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Application;
import java.io.IOException;

public class JerseySolrTest extends JerseyTest {
    protected CoreContainer coreContainer;
    private CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker;
    private SolrQueryRequest solrQueryRequest;
    private SolrQueryResponse solrQueryResponse;
    public static final String coreName = "demo";
    public JerseySolrTest(){
        System.out.println("Hey I am inside the JerseySolrTest. So yeah");

    }
    @BeforeClass
    public static void ensureWorkingMockito() {
        SolrTestCaseJ4.assumeWorkingMockito();
    }
    @Override
    public Application configure(){
        resetMocks();
        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.register(SolrJacksonMapper.class);
        resourceConfig.register(new SetPropertiesNeededForTestFilter());
        resourceConfig.register(CatchAllExceptionMapper.class);
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
    public  class SetPropertiesNeededForTestFilter  implements ContainerRequestFilter {
        @Override
        public void filter(ContainerRequestContext requestContext) throws IOException {
            requestContext.setProperty(RequestContextKeys.SOLR_QUERY_REQUEST, solrQueryRequest);
            requestContext.setProperty(RequestContextKeys.SOLR_QUERY_RESPONSE, new SolrQueryResponse());
        }
    }
    public void resetMocks() {
        coreContainer = Mockito.mock(CoreContainer.class);
        coreAdminAsyncTracker = Mockito.mock(CoreAdminHandler.CoreAdminAsyncTracker.class);
        solrQueryRequest = Mockito.mock(SolrQueryRequest.class);
        var modifiableSolrParams = new ModifiableSolrParams();
        modifiableSolrParams.add(CommonParams.WT, "application/json");
        Mockito.when(solrQueryRequest.getParams()).thenReturn(modifiableSolrParams);
    }
}
