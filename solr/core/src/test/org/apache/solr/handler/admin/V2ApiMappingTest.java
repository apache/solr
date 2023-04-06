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

package org.apache.solr.handler.admin;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.api.AnnotatedApi;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

/**
 * A base class that facilitates testing V2 to v1 API mappings by mocking out the underlying request
 * handler and "capturing" the v1 request that the handler "sees" following v2-to-v1 conversion.
 *
 * <p>This base class is only appropriate for testing those v2 APIs implemented as a mapping-layer
 * on top of an existing RequestHandler based implementation. API classes containing "real" logic or
 * that don't rely on an underlying RequestHandler for their logic should use a different test
 * harness.
 *
 * <p>Subclasses are required to implement {@link #populateApiBag()}, {@link #isCoreSpecific()}, and
 * {@link #createUnderlyingRequestHandler()} to properly use this harness. See the method-level
 * Javadocs on each for more information. With these methods implemented, subclasses can test their
 * API mappings by calling any of the 'capture' methods included on this harness, which take in the
 * v2 API path, method, etc. and use them to look up and run the matching API (if a match can be
 * found). Most of these helper methods return the (post-v1-conversion) {@link SolrParams} seen by
 * the mocked RequestHandler.
 */
public abstract class V2ApiMappingTest<T extends RequestHandlerBase> extends SolrTestCaseJ4 {

  protected ApiBag apiBag;
  protected ArgumentCaptor<SolrQueryRequest> queryRequestCaptor;
  @Mock protected T mockRequestHandler;

  /**
   * A hook allowing subclasses to insert the v2 endpoints-under-test into the {@link ApiBag}
   * container.
   */
  public abstract void populateApiBag();

  /**
   * Instantiates a Mockito mock for the particular RequestHandler used by endpoints under test.
   *
   * <p>The created mock is used to capture the v1 request that actually makes its way to the
   * request handler. Subclasses may stub out specific response values or behaviors as desired, but
   * in most cases an unmodified mock (i.e. {@code return mock(CollectionsHandler.class)}) is
   * sufficient.
   */
  public abstract T createUnderlyingRequestHandler();

  /**
   * Indicates whether the {@link ApiBag} used to lookup and invoke v2 APIs should be "core-level"
   * (i.e. core- specific) or "container-level" (i.e. core-agnostic).
   */
  public abstract boolean isCoreSpecific();

  public T getRequestHandler() {
    return mockRequestHandler;
  }

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setUpMocks() {
    mockRequestHandler = createUnderlyingRequestHandler();
    queryRequestCaptor = ArgumentCaptor.forClass(SolrQueryRequest.class);

    apiBag = new ApiBag(isCoreSpecific());
    populateApiBag();
  }

  protected SolrQueryRequest captureConvertedV1Request(
      String v2Path, String v2Method, String v2RequestBody) throws Exception {
    final HashMap<String, String> parts = new HashMap<>();
    final Api api = apiBag.lookup(v2Path, v2Method, parts);
    final SolrQueryResponse rsp = new SolrQueryResponse();
    final LocalSolrQueryRequest req =
        new LocalSolrQueryRequest(null, Map.of()) {
          @Override
          public List<CommandOperation> getCommands(boolean validateInput) {
            if (v2RequestBody == null) return Collections.emptyList();
            return ApiBag.getCommandOperations(
                new ContentStreamBase.StringStream(v2RequestBody), api.getCommandSchema(), true);
          }

          @Override
          public Collection<ContentStream> getContentStreams() {
            return List.of(new ContentStreamBase.StringStream(v2RequestBody));
          }

          @Override
          public Map<String, String> getPathTemplateValues() {
            return parts;
          }

          @Override
          public String getHttpMethod() {
            return v2Method;
          }
        };

    api.call(req, rsp);
    verify(mockRequestHandler).handleRequestBody(queryRequestCaptor.capture(), any());
    return queryRequestCaptor.getValue();
  }

  protected SolrParams captureConvertedV1Params(String path, String method, String v2RequestBody)
      throws Exception {
    return captureConvertedV1Request(path, method, v2RequestBody).getParams();
  }

  protected SolrParams captureConvertedV1Params(
      String path, String method, Map<String, String[]> queryParams) throws Exception {
    final HashMap<String, String> parts = new HashMap<>();
    final Api api = apiBag.lookup(path, method, parts);
    final SolrQueryResponse rsp = new SolrQueryResponse();
    final LocalSolrQueryRequest req =
        new LocalSolrQueryRequest(null, queryParams) {
          @Override
          public List<CommandOperation> getCommands(boolean validateInput) {
            return Collections.emptyList();
          }

          @Override
          public Map<String, String> getPathTemplateValues() {
            return parts;
          }

          @Override
          public String getHttpMethod() {
            return method;
          }
        };

    api.call(req, rsp);
    verify(mockRequestHandler).handleRequestBody(queryRequestCaptor.capture(), any());
    return queryRequestCaptor.getValue().getParams();
  }

  // TODO Combine with method above
  protected SolrParams captureConvertedV1Params(String path, String method, SolrParams queryParams)
      throws Exception {
    return captureConvertedV1Params(path, method, queryParams, null);
  }

  protected SolrParams captureConvertedV1Params(
      String path, String method, SolrParams queryParams, String v2RequestBody) throws Exception {
    final HashMap<String, String> parts = new HashMap<>();
    final Api api = apiBag.lookup(path, method, parts);
    final SolrQueryResponse rsp = new SolrQueryResponse();
    final LocalSolrQueryRequest req =
        new LocalSolrQueryRequest(null, queryParams) {
          @Override
          public List<CommandOperation> getCommands(boolean validateInput) {
            if (v2RequestBody == null) return Collections.emptyList();
            return ApiBag.getCommandOperations(
                new ContentStreamBase.StringStream(v2RequestBody), api.getCommandSchema(), true);
          }

          @Override
          public Map<String, String> getPathTemplateValues() {
            return parts;
          }

          @Override
          public String getHttpMethod() {
            return method;
          }
        };

    api.call(req, rsp);
    verify(mockRequestHandler).handleRequestBody(queryRequestCaptor.capture(), any());
    return queryRequestCaptor.getValue().getParams();
  }

  protected AnnotatedApi assertAnnotatedApiExistsFor(String method, String path) {
    final AnnotatedApi api = getAnnotatedApiFor(method, path);
    assertNotNull(
        "Expected to find API mapping for [" + method + " " + path + "] but none found!", api);
    return api;
  }

  protected AnnotatedApi getAnnotatedApiFor(String method, String path) {
    final HashMap<String, String> parts = new HashMap<>();
    final Api api = apiBag.lookup(path, method, parts);
    if (api == null) {
      fail("Expected to find API for path [" + path + "], but no API mapping found.");
    }
    if (!(api instanceof AnnotatedApi)) {
      fail(
          "Expected AnnotatedApi for path ["
              + path
              + "], but found non-annotated API ["
              + api
              + "]");
    }

    return (AnnotatedApi) api;
  }

  protected T createMock(Class<T> clazz) {
    return mock(clazz);
  }
}
