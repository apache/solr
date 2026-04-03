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
package org.apache.solr.update.processor;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.apache.solr.update.processor.ContentHashVersionProcessorFactory.CONTENT_HASH_ENABLED_PARAM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

public class ContentHashVersionProcessorFactoryTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    assumeWorkingMockito();
  }

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

    UpdateRequestProcessor instance =
        factory.getInstance(updateRequest, mock(SolrQueryResponse.class), next);

    assertEquals(instance, next);
  }

  @Test
  public void shouldEnableContentHashByQueryParameter() {
    ContentHashVersionProcessorFactory factory = new ContentHashVersionProcessorFactory();
    UpdateRequestProcessor next = mock(UpdateRequestProcessor.class);
    SolrQueryRequest updateRequest = createUpdateRequest(true); // Request enables processor

    UpdateRequestProcessor instance =
        factory.getInstance(updateRequest, mock(SolrQueryResponse.class), next);

    assertNotEquals(instance, next);
  }

  private static SolrQueryRequest createUpdateRequest(boolean enableContentHashParamValue) {
    SolrQueryRequest req = mock(SolrQueryRequest.class);
    SolrParams solrParams = mock(SolrParams.class);
    when(solrParams.getBool(eq(CONTENT_HASH_ENABLED_PARAM), anyBoolean()))
        .thenReturn(enableContentHashParamValue);
    when(req.getParams()).thenReturn(solrParams);

    return req;
  }
}
