package org.apache.solr.client.solrj.impl;

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.SolrPing;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.embedded.JettyConfig;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

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

    protected void validateDelete() {
        // default method
        assertEquals("post", DebugServlet.lastMethod);
        // agent
        assertEquals(expectedUserAgent(), DebugServlet.headers.get("user-agent"));
        // default wt
        assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
        // default version
        assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
        // agent
        assertEquals(expectedUserAgent(), DebugServlet.headers.get("user-agent"));
    }

    public void testGetById(Http2SolrClientBase client) throws Exception {
        DebugServlet.clear();
        Collection<String> ids = Collections.singletonList("a");
        try {
            client.getById("a");
        } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
        }

        try {
            client.getById(ids, null);
        } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
        }

        try {
            client.getById("foo", "a");
        } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
        }

        try {
            client.getById("foo", ids, null);
        } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
        }
    }

    /**
     * test that SolrExceptions thrown by HttpSolrClient can correctly encapsulate http status codes
     * even when not on the list of ErrorCodes solr may return.
     */
    public void testSolrExceptionCodeNotFromSolr(Http2SolrClientBase client) throws IOException, SolrServerException {
        final int status = 527;
        assertEquals(
                status
                        + " didn't generate an UNKNOWN error code, someone modified the list of valid ErrorCode's w/o changing this test to work a different way",
                SolrException.ErrorCode.UNKNOWN,
                SolrException.ErrorCode.getErrorCode(status));

        DebugServlet.setErrorCode(status);
        try {
            SolrQuery q = new SolrQuery("foo");
            client.query(q, SolrRequest.METHOD.GET);
            fail("Didn't get excepted exception from oversided request");
        } catch (SolrException e) {
            assertEquals("Unexpected exception status code", status, e.code());
        }
    }

    /**
     * test that SolrExceptions thrown by HttpSolrClient can correctly encapsulate http status codes
     * even when not on the list of ErrorCodes solr may return.
     */
    public void testSolrExceptionWithNullBaseurl(Http2SolrClientBase client) throws IOException, SolrServerException {
        final int status = 527;
        assertEquals(
                status
                        + " didn't generate an UNKNOWN error code, someone modified the list of valid ErrorCode's w/o changing this test to work a different way",
                SolrException.ErrorCode.UNKNOWN,
                SolrException.ErrorCode.getErrorCode(status));

        DebugServlet.setErrorCode(status);
        try {
            // if client base url is null, request url will be used in exception message
            SolrPing ping = new SolrPing();
            ping.setBasePath(getBaseUrl() + "/debug/foo");
            client.request(ping);

            fail("Didn't get excepted exception from oversided request");
        } catch (SolrException e) {
            assertEquals("Unexpected exception status code", status, e.code());
            assertTrue(e.getMessage().contains(getBaseUrl()));
        }
    }

    protected void testUpdate(Http2SolrClientBase client, String wt, String contentType) throws Exception {
        DebugServlet.clear();
        UpdateRequest req = new UpdateRequest();
        req.add(new SolrInputDocument());
        req.setParam("a", "\u1234");

        try {
            client.request(req);
        } catch (BaseHttpSolrClient.RemoteSolrException ignored) { }
        assertEquals("post", DebugServlet.lastMethod);
        assertEquals(expectedUserAgent(), DebugServlet.headers.get("user-agent"));
        assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
        assertEquals(wt, DebugServlet.parameters.get(CommonParams.WT)[0]);
        assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
        assertEquals(
                client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
        assertEquals(contentType, DebugServlet.headers.get("content-type"));
        assertEquals(1, DebugServlet.parameters.get("a").length);
        assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
    }

}