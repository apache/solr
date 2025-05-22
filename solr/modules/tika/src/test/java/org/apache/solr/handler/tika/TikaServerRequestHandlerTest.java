package org.apache.solr.handler.tika;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.NamedListBasedSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.ContentStreamLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.processor.UpdateRequestProcessor;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TikaServerRequestHandlerTest {

    private TikaServerRequestHandler handler;

    @Mock
    private SolrCore mockSolrCore;
    @Mock
    private SolrResourceLoader mockResourceLoader;
    @Mock
    private SolrQueryRequest mockSolrQueryRequest;
    @Mock
    private UpdateRequestProcessor mockUpdateRequestProcessor;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        handler = new TikaServerRequestHandler();
        when(mockSolrCore.getResourceLoader()).thenReturn(mockResourceLoader);
    }

    private Object getField(Object instance, String fieldName) throws NoSuchFieldException, IllegalAccessException {
        Field field = instance.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(instance);
    }

    @Test
    public void testInformLoadsParamsCorrectly() throws Exception {
        NamedList<Object> initArgs = new NamedList<>();
        initArgs.add("tikaServer.url", "http://localhost:9998/tika");
        initArgs.add("tikaServer.connectionTimeout", "15000");
        initArgs.add("tikaServer.socketTimeout", "120000");
        initArgs.add("tikaServer.idField", "myCustomId");
        initArgs.add("tikaServer.returnMetadata", "false");
        initArgs.add("tikaServer.metadataPrefix", "custom_meta_");
        initArgs.add("tikaServer.contentField", "custom_content");

        handler.init(initArgs);
        handler.inform(mockSolrCore);

        assertEquals("http://localhost:9998/tika", getField(handler, "tikaServerUrl"));
        assertEquals(15000, getField(handler, "connectionTimeout"));
        assertEquals(120000, getField(handler, "socketTimeout"));
        assertEquals("myCustomId", getField(handler, "idField"));
        assertEquals(false, getField(handler, "returnMetadata"));
        assertEquals("custom_meta_", getField(handler, "metadataPrefix"));
        assertEquals("custom_content", getField(handler, "contentField"));
    }

    @Test
    public void testInformDefaults() throws Exception {
        NamedList<Object> initArgs = new NamedList<>();
        initArgs.add("tikaServer.url", "http://anotherhost/tika");
        // No other params, so defaults should apply

        handler.init(initArgs);
        handler.inform(mockSolrCore);

        assertEquals("http://anotherhost/tika", getField(handler, "tikaServerUrl"));
        // Default values from TikaServerRequestHandler
        assertEquals(5000, getField(handler, "connectionTimeout"));
        assertEquals(60000, getField(handler, "socketTimeout"));
        assertEquals(null, getField(handler, "idField")); // Default is null
        assertEquals(true, getField(handler, "returnMetadata"));
        assertEquals("", getField(handler, "metadataPrefix"));
        assertEquals("content", getField(handler, "contentField"));
    }

    @Test
    public void testInformMissingTikaUrlThrowsException() {
        NamedList<Object> initArgs = new NamedList<>(); // Empty, so no tikaServer.url
        handler.init(initArgs);

        SolrException e = assertThrows(SolrException.class, () -> handler.inform(mockSolrCore));
        assertEquals(SolrException.ErrorCode.SERVER_ERROR.code, e.code());
        assertTrue(e.getMessage().contains("Missing required parameter: tikaServer.url"));
    }

    @Test
    public void testNewLoaderReturnsConfiguredLoader() throws Exception {
        NamedList<Object> initArgs = new NamedList<>();
        String url = "http://loader-test-url/tika";
        int connTimeout = 2222;
        int sockTimeout = 3333;
        String idFld = "loaderIdField";
        boolean retMeta = false;
        String metaPre = "loader_";
        String contentFld = "loader_content";

        initArgs.add("tikaServer.url", url);
        initArgs.add("tikaServer.connectionTimeout", String.valueOf(connTimeout));
        initArgs.add("tikaServer.socketTimeout", String.valueOf(sockTimeout));
        initArgs.add("tikaServer.idField", idFld);
        initArgs.add("tikaServer.returnMetadata", String.valueOf(retMeta));
        initArgs.add("tikaServer.metadataPrefix", metaPre);
        initArgs.add("tikaServer.contentField", contentFld);

        handler.init(initArgs);
        handler.inform(mockSolrCore);

        ContentStreamLoader csl = handler.newLoader(mockSolrQueryRequest, mockUpdateRequestProcessor);

        assertNotNull("ContentStreamLoader should not be null", csl);
        assertTrue("Loader should be an instance of TikaServerDocumentLoader", csl instanceof TikaServerDocumentLoader);

        TikaServerDocumentLoader tsdl = (TikaServerDocumentLoader) csl;
        assertEquals(url, getField(tsdl, "tikaServerUrl"));
        assertEquals(connTimeout, getField(tsdl, "connectionTimeout"));
        assertEquals(sockTimeout, getField(tsdl, "socketTimeout"));
        assertEquals(idFld, getField(tsdl, "idField"));
        assertEquals(retMeta, getField(tsdl, "returnMetadata"));
        assertEquals(metaPre, getField(tsdl, "metadataPrefix"));
        assertEquals(contentFld, getField(tsdl, "contentField"));
    }
}
