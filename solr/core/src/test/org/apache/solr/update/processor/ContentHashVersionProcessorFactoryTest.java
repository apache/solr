package org.apache.solr.update.processor;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Test;

import java.util.List;

import static org.apache.solr.update.processor.ContentHashVersionProcessorFactory.CONTENT_HASH_ENABLED_PARAM;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ContentHashVersionProcessorFactoryTest {

  @Test
  public void shouldHaveSensibleDefaultValues() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    assertEquals(List.of("*"), factory.getIncludeFields());
    assertEquals("content_hash", factory.getHashFieldName());
    assertTrue(factory.discardSameDocuments());
  }

  @Test
  public void shouldInitWithHashFieldName() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    NamedList<String> args = new NamedList<>();
    args.add("hashFieldName", "_hash_field_");
    factory.init(args);

    assertEquals("_hash_field_", factory.getHashFieldName());
  }

  @Test
  public void shouldInitWithAllField() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    NamedList<String> args = new NamedList<>();
    args.add("includeFields", "*");
    factory.init(args);

    assertEquals(1, factory.getIncludeFields().size());
    assertEquals("*", factory.getIncludeFields().getFirst());
  }

  @Test
  public void shouldInitWithIncludedFields() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    NamedList<String> args = new NamedList<>();
    args.add("includeFields", " field1,field2 , field3 ");
    factory.init(args);

    assertEquals(3, factory.getIncludeFields().size());
    assertEquals(List.of("field1", "field2", "field3"), factory.getIncludeFields());
  }

  @Test
  public void shouldInitWithExcludedFields() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    NamedList<String> args = new NamedList<>();
    args.add("excludeFields", " field1,field2 , field3 ");
    factory.init(args);

    assertEquals(4, factory.getExcludeFields().size());
    assertEquals(List.of("field1", "field2", "field3", "content_hash"), factory.getExcludeFields());
  }

  @Test
  public void shouldSelectRejectSameHashStrategy() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    NamedList<String> args = new NamedList<>();
    args.add("hashCompareStrategy", "discard");
    factory.init(args);

    assertTrue(factory.discardSameDocuments());
  }

  @Test
  public void shouldSelectLogStrategy() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    NamedList<String> args = new NamedList<>();
    args.add("hashCompareStrategy", "log");
    factory.init(args);

    assertFalse(factory.discardSameDocuments());
  }

  @Test
  public void shouldSelectDiscardStrategy() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    NamedList<String> args = new NamedList<>();
    args.add("hashCompareStrategy", "discard");
    factory.init(args);

    assertTrue(factory.discardSameDocuments());
  }

  @Test(expected = SolrException.class)
  public void shouldSelectUnsupportedStrategy() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    NamedList<String> args = new NamedList<>();
    args.add("hashCompareStrategy", "unsupported value");
    factory.init(args);
  }

  @Test(expected = SolrException.class)
  public void shouldRejectExcludeAllFields() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    NamedList<String> args = new NamedList<>();
    args.add("excludeFields", "*");
    factory.init(args);
  }

  @Test
  public void shouldDisableContentHashByQueryParameter() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    UpdateRequestProcessor next = mock(UpdateRequestProcessor.class);
    SolrQueryRequest updateRequest = createUpdateRequest(false); // Request disables processor

    UpdateRequestProcessor instance = factory.getInstance(updateRequest, mock(SolrQueryResponse.class), next);

    assertEquals(instance, next);
  }

  @Test
  public void shouldEnableContentHashByQueryParameter() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    UpdateRequestProcessor next = mock(UpdateRequestProcessor.class);
    SolrQueryRequest updateRequest = createUpdateRequest(true); // Request enables processor

    UpdateRequestProcessor instance = factory.getInstance(updateRequest, mock(SolrQueryResponse.class), next);

    assertNotEquals(instance, next);
  }

  private static SolrQueryRequest createUpdateRequest(boolean enableContentHashParamValue) {
    SolrQueryRequest req = mock(SolrQueryRequest.class);
    SolrParams solrParams = mock(SolrParams.class);
    when(solrParams.getBool(eq(CONTENT_HASH_ENABLED_PARAM), anyBoolean())).thenReturn(enableContentHashParamValue);
    when(req.getParams()).thenReturn(solrParams);

    return req;
  }
}
