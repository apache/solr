package org.apache.solr.client.solrj.impl;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.common.params.CommonParams;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class HttpSolrClientJdkImplTest extends Http2SolrClientTestBase<HttpSolrClientJdkImpl.Builder> {

    @After
    public void workaroundToReleaseThreads_noClosableUntilJava21() {
        System.gc();
    }

    @Test
    public void testQueryGet() throws Exception {
        super.testQueryGet();
    }
    @Test
    public void testQueryPost() throws Exception {
        super.testQueryPost();
    }
    @Test
    public void testQueryPut() throws Exception {
        super.testQueryPut();
    }
    @Test
    public void testQueryXmlGet() throws Exception {
        super.testQueryXmlGet();
    }

    @Test
    public void testQueryXmlPost() throws Exception {
        super.testQueryXmlPost();
    }

    @Test
    public void testQueryXmlPut() throws Exception {
        super.testQueryXmlPut();
    }

    @Test
    public void testDelete() throws Exception {
        DebugServlet.clear();
        String url = getBaseUrl() + "/debug/foo";
        try (HttpSolrClientJdkImpl client = new HttpSolrClientJdkImpl.Builder(url).build()) {
            try {
                client.deleteById("id");
            } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
            }
            assertEquals(
                    client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
            assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
            validateDelete();
        }
    }
    @Test
    public void testDeleteXml() throws Exception {
       DebugServlet.clear();
        String url = getBaseUrl() + "/debug/foo";
        try (HttpSolrClientJdkImpl client =
                     new HttpSolrClientJdkImpl.Builder(url).withResponseParser(new XMLResponseParser()).build()) {
            try {
                client.deleteByQuery("*:*");
            } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
            }
            assertEquals(
                    client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
            assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
            validateDelete();
        }
    }

    @Override
    protected void testQuerySetup(SolrRequest.METHOD method, ResponseParser rp) throws Exception {
        DebugServlet.clear();
        String url = getBaseUrl() + "/debug/foo";
        SolrQuery q = new SolrQuery("foo");
        q.setParam("a", "\u1234");
        HttpSolrClientJdkImpl.Builder b = new HttpSolrClientJdkImpl.Builder(url);
        if(rp != null) {
            b.withResponseParser(rp);
        }
        try (HttpSolrClientJdkImpl client = b.build()) {
            client.query(q, method);
            assertEquals(
                    client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
        } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
        }
    }

    @Test
    public void testGetById() throws Exception {
        DebugServlet.clear();
        try (HttpSolrClientJdkImpl client =
                     new HttpSolrClientJdkImpl.Builder(getBaseUrl() + "/debug/foo").build()) {
            super.testGetById(client);
        }
    }

    @Test
    public void testTimeout() throws Exception {
        SolrQuery q = new SolrQuery("*:*");
        try (HttpSolrClientJdkImpl client =
                     builder(getBaseUrl() + "/slow/foo", DEFAULT_CONNECTION_TIMEOUT, 2000, HttpSolrClientJdkImpl.Builder.class)
                             .build()) {
            client.query(q, SolrRequest.METHOD.GET);
            fail("No exception thrown.");
        } catch (SolrServerException e) {
            assertTrue(e.getMessage().contains("timeout") || e.getMessage().contains("Timeout"));
        }
    }

    @Test
    public void test0IdleTimeout() throws Exception {
        SolrQuery q = new SolrQuery("*:*");
        try (HttpSolrClientJdkImpl client =
                     builder(getBaseUrl() + "/debug/foo", DEFAULT_CONNECTION_TIMEOUT, 0, HttpSolrClientJdkImpl.Builder.class)
                             .build()) {
            try {
                client.query(q, SolrRequest.METHOD.GET);
            } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
            }
        }
    }

    @Test
    public void testRequestTimeout() throws Exception {
        SolrQuery q = new SolrQuery("*:*");
        try (HttpSolrClientJdkImpl client =
                     builder(getBaseUrl() + "/slow/foo", DEFAULT_CONNECTION_TIMEOUT, 0, HttpSolrClientJdkImpl.Builder.class)
                             .withRequestTimeout(500, TimeUnit.MILLISECONDS)
                             .build()) {
            client.query(q, SolrRequest.METHOD.GET);
            fail("No exception thrown.");
        } catch (SolrServerException e) {
            assertTrue(e.getMessage().contains("timeout") || e.getMessage().contains("Timeout"));
        }
    }


    public void testSolrExceptionCodeNotFromSolr() throws IOException, SolrServerException {
        try (HttpSolrClientJdkImpl client = new HttpSolrClientJdkImpl.Builder(getBaseUrl() + "/debug/foo").build()) {
            super.testSolrExceptionCodeNotFromSolr(client);
        } finally {
            DebugServlet.clear();
        }
    }

    @Test
    public void testSolrExceptionWithNullBaseurl() throws IOException, SolrServerException {
        try (HttpSolrClientJdkImpl client = new HttpSolrClientJdkImpl.Builder(null).build()) {
            super.testSolrExceptionWithNullBaseurl(client);
        } finally {
            DebugServlet.clear();
        }
    }

    @Test
    public void testUpdateDefault() throws Exception {
        String url = getBaseUrl() + "/debug/foo";
        try (HttpSolrClientJdkImpl client = new HttpSolrClientJdkImpl.Builder(url).build()) {
            testUpdate(client, "javabin", "application/javabin");
        }
    }

    @Test
    public void testUpdateXml() throws Exception {
        String url = getBaseUrl() + "/debug/foo";
        try (HttpSolrClientJdkImpl client = new HttpSolrClientJdkImpl.Builder(url)
                             .withRequestWriter(new RequestWriter())
                             .withResponseParser(new XMLResponseParser())
                             .build()) {
            testUpdate(client, "xml", "application/xml; charset=UTF-8");
        }
    }
    @Test
    public void testUpdateJavabin() throws Exception {
        String url = getBaseUrl() + "/debug/foo";
        try (HttpSolrClientJdkImpl client = new HttpSolrClientJdkImpl.Builder(url)
                             .withRequestWriter(new BinaryRequestWriter())
                             .withResponseParser(new BinaryResponseParser())
                             .build()) {
            testUpdate(client, "javabin", "application/javabin");
        }
    }

    @Test
    public void testCollectionParameters() throws IOException, SolrServerException {
        HttpSolrClientJdkImpl baseUrlClient = new HttpSolrClientJdkImpl.Builder(getBaseUrl()).build();
        HttpSolrClientJdkImpl collection1UrlClient = new HttpSolrClientJdkImpl.Builder(getCoreUrl()).build();
        testCollectionParameters(baseUrlClient, collection1UrlClient);
    }

    @Test
    public void testQueryString() throws Exception {
        testQueryString(HttpSolrClientJdkImpl.class, HttpSolrClientJdkImpl.Builder.class);
    }

    @Test
    public void testGetRawStream() throws Exception {
        try(HttpSolrClientJdkImpl client = builder(
                getBaseUrl() + "/debug/foo", DEFAULT_CONNECTION_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT, HttpSolrClientJdkImpl.Builder.class)
                .build()) {
            super.testGetRawStream(client);
        }
    }

    protected String expectedUserAgent() {
        return "Solr[" + HttpSolrClientJdkImpl.class.getName() + "] 1.0";
    }

    @Override
    protected <B extends HttpSolrClientBuilderBase> B builder(String url, int connectionTimeout, int socketTimeout, Class<B> type) {
        HttpSolrClientJdkImpl.Builder b = new HttpSolrClientJdkImpl.Builder(url).withConnectionTimeout(connectionTimeout, TimeUnit.MILLISECONDS)
                .withIdleTimeout(socketTimeout, TimeUnit.MILLISECONDS);
        return type.cast(b);
    }
}
