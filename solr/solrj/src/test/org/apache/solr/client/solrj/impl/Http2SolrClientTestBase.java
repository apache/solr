package org.apache.solr.client.solrj.impl;

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.embedded.JettyConfig;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class Http2SolrClientTestBase<B> extends SolrJettyTestBase {

    @BeforeClass
    public static void beforeTest() throws Exception {
        JettyConfig jettyConfig =
                JettyConfig.builder()
                        .withServlet(
                                new ServletHolder(BasicHttpSolrClientTest.RedirectServlet.class), "/redirect/*")
                        .withServlet(new ServletHolder(BasicHttpSolrClientTest.SlowServlet.class), "/slow/*")
                        .withServlet(new ServletHolder(DebugServlet.class), "/debug/*")
                        .withSSLConfig(sslConfig.buildServerSSLConfig())
                        .build();
        createAndStartJetty(legacyExampleCollection1SolrHome(), jettyConfig);
    }

    @Override
    public void tearDown() throws Exception {
        System.clearProperty("basicauth");
        System.clearProperty(HttpClientUtil.SYS_PROP_HTTP_CLIENT_BUILDER_FACTORY);
        DebugServlet.clear();
        super.tearDown();
    }

    protected abstract B builder(String url, int connectionTimeout, int socketTimeout);

    protected abstract String expectedUserAgent();

    protected abstract void testQuerySetup(SolrRequest.METHOD method, ResponseParser rp) throws Exception;

    @Test
    public void testQueryGet() throws Exception {
        testQuerySetup(SolrRequest.METHOD.GET, null);
        // default method
        assertEquals("get", DebugServlet.lastMethod);
        // agent
        assertEquals(expectedUserAgent(), DebugServlet.headers.get("user-agent"));
        // default wt
        assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
        assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
        // default version
        assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
        // agent
        assertEquals(expectedUserAgent(), DebugServlet.headers.get("user-agent"));
        // content-type
        assertNull(DebugServlet.headers.get("content-type"));
        // param encoding
        assertEquals(1, DebugServlet.parameters.get("a").length);
        assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
    }

    @Test
    public void testQueryPost() throws Exception {
        testQuerySetup(SolrRequest.METHOD.POST, null);

        assertEquals("post", DebugServlet.lastMethod);
        assertEquals(expectedUserAgent(), DebugServlet.headers.get("user-agent"));
        assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
        assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
        assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
        assertEquals(1, DebugServlet.parameters.get("a").length);
        assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
        assertEquals(expectedUserAgent(), DebugServlet.headers.get("user-agent"));
        assertEquals("application/x-www-form-urlencoded", DebugServlet.headers.get("content-type"));
    }

    @Test
    public void testQueryPut() throws Exception {
        testQuerySetup(SolrRequest.METHOD.PUT, null);

        assertEquals("put", DebugServlet.lastMethod);
        assertEquals(expectedUserAgent(), DebugServlet.headers.get("user-agent"));
        assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
        assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
        assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
        assertEquals(1, DebugServlet.parameters.get("a").length);
        assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
        assertEquals(expectedUserAgent(), DebugServlet.headers.get("user-agent"));
        assertEquals("application/x-www-form-urlencoded", DebugServlet.headers.get("content-type"));
    }

    @Test
    public void testQueryXmlGet() throws Exception {
        testQuerySetup(SolrRequest.METHOD.GET, new XMLResponseParser());

        assertEquals("get", DebugServlet.lastMethod);
        assertEquals(expectedUserAgent(), DebugServlet.headers.get("user-agent"));
        assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
        assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
        assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
        assertEquals(1, DebugServlet.parameters.get("a").length);
        assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
        assertEquals(expectedUserAgent(), DebugServlet.headers.get("user-agent"));
    }

    @Test
    public void testQueryXmlPost() throws Exception {
        testQuerySetup(SolrRequest.METHOD.POST, new XMLResponseParser());

        assertEquals("post", DebugServlet.lastMethod);
        assertEquals(expectedUserAgent(), DebugServlet.headers.get("user-agent"));
        assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
        assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
        assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
        assertEquals(1, DebugServlet.parameters.get("a").length);
        assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
        assertEquals(expectedUserAgent(), DebugServlet.headers.get("user-agent"));
        assertEquals("application/x-www-form-urlencoded", DebugServlet.headers.get("content-type"));
    }

    @Test
    public void testQueryXmlPut() throws Exception {
        testQuerySetup(SolrRequest.METHOD.PUT, new XMLResponseParser());

        assertEquals("put", DebugServlet.lastMethod);
        assertEquals(expectedUserAgent(), DebugServlet.headers.get("user-agent"));
        assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
        assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
        assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
        assertEquals(1, DebugServlet.parameters.get("a").length);
        assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
        assertEquals(expectedUserAgent(), DebugServlet.headers.get("user-agent"));
        assertEquals("application/x-www-form-urlencoded", DebugServlet.headers.get("content-type"));
    }
}