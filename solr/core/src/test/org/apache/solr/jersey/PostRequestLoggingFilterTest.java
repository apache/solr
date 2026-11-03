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

package org.apache.solr.jersey;

import static org.apache.solr.jersey.MessageBodyReaders.CachingDelegatingMessageBodyReader.DESERIALIZED_REQUEST_BODY_KEY;
import static org.apache.solr.jersey.RequestContextKeys.SOLR_QUERY_RESPONSE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ResourceContext;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.UriInfo;
import java.io.ByteArrayInputStream;
import java.lang.annotation.Annotation;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.CreateCollectionRequestBody;
import org.apache.solr.client.api.model.CreateReplicaRequestBody;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.SolrQueryResponse;
import org.glassfish.hk2.api.ServiceLocator;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJsonProvider;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.spi.ExceptionMappers;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for {@link PostRequestLoggingFilter}
 *
 * <p>Tests primarily focus on exercising the string operations used to flatten the request into a
 * log message.
 */
public class PostRequestLoggingFilterTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Test
  public void testBuildQueryParameterString_Simple() {
    final var queryParams = new MultivaluedHashMap<String, String>();
    queryParams.putSingle("paramName1", "paramValue1");
    queryParams.putSingle("paramName2", "paramValue2");

    final var queryParamStr =
        PostRequestLoggingFilter.filterAndStringifyQueryParameters(queryParams);

    assertEquals("paramName1=paramValue1&paramName2=paramValue2", queryParamStr);
  }

  @Test
  public void testBuildQueryParameterString_DuplicateParams() {
    final var queryParams = new MultivaluedHashMap<String, String>();
    queryParams.putSingle("paramName1", "paramValue1");
    queryParams.put("paramName2", List.of("paramValue2a", "paramValue2b"));

    final var queryParamStr =
        PostRequestLoggingFilter.filterAndStringifyQueryParameters(queryParams);

    assertEquals(
        "paramName1=paramValue1&paramName2=paramValue2a&paramName2=paramValue2b", queryParamStr);
  }

  @Test
  public void testBuildQueryParameterString_SpecialChars() {
    final var queryParams = new MultivaluedHashMap<String, String>();
    queryParams.putSingle("paramName1", "paramValue1");
    queryParams.putSingle(
        "paramName=2", "paramValue=2"); // The name and value themselves contain an equals sign

    final var queryParamStr =
        PostRequestLoggingFilter.filterAndStringifyQueryParameters(queryParams);

    assertEquals("paramName1=paramValue1&paramName%3D2=paramValue%3D2", queryParamStr);
  }

  @Test
  public void testBuildQueryParameterString_ExcludesFilteredParameters() {
    final var queryParams = new MultivaluedHashMap<String, String>();
    queryParams.putSingle("paramName1", "paramValue1");
    queryParams.putSingle("paramName2", "paramValue2");
    queryParams.putSingle("hiddenParam1", "hiddenValue1");
    queryParams.putSingle("hiddenParam2", "hiddenValue2");
    queryParams.putSingle("logParamsList", "paramName1,paramName2");

    final var queryParamStr =
        PostRequestLoggingFilter.filterAndStringifyQueryParameters(queryParams);

    assertEquals("paramName1=paramValue1&paramName2=paramValue2", queryParamStr);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCachingJsonMessageBodyReaderDelegateUsesConfiguredObjectMapper()
      throws Exception {
    final var delegate = new MessageBodyReaders.CachingJsonMessageBodyReader().getDelegate();

    // Mapper SolrJacksonMapper.createObjectMapper creates a mapper with FAIL_ON_UNKNOWN_PROPERTIES
    // set to false, hence we expect a json with an unknown field to be deserialized without any
    // exception
    final var entityStream =
        new ByteArrayInputStream(
            "{\"name\": \"test\", \"unknownField\": \"someValue\"}"
                .getBytes(StandardCharsets.UTF_8));

    final var result =
        (CreateCollectionRequestBody)
            delegate.readFrom(
                (Class<Object>) (Class<?>) CreateCollectionRequestBody.class,
                CreateCollectionRequestBody.class,
                new Annotation[0],
                MediaType.APPLICATION_JSON_TYPE,
                new MultivaluedHashMap<>(),
                entityStream);

    assertEquals("test", result.name);
  }

  @Test
  public void testCachingJsonMessageBodyReaderDelegateReusingObjectMapper() {
    final var delegate =
        (JacksonJsonProvider) new MessageBodyReaders.CachingJsonMessageBodyReader().getDelegate();
    final var delegate2 =
        (JacksonJsonProvider) new MessageBodyReaders.CachingJsonMessageBodyReader().getDelegate();

    ObjectMapper mapper1 = delegate2.locateMapper(Object.class, MediaType.APPLICATION_JSON_TYPE);
    ObjectMapper mapper2 = delegate.locateMapper(Object.class, MediaType.APPLICATION_JSON_TYPE);
    assertSame(mapper1, mapper2);
  }

  @Test
  public void testJsonReaderCachesValidBody() throws Exception {
    final var context = mock(ContainerRequestContext.class);
    final var body =
        (CreateCollectionRequestBody)
            readJsonBody(CreateCollectionRequestBody.class, "{\"name\":\"test\"}", context);
    assertEquals("test", body.name);
    verify(context).setProperty(DESERIALIZED_REQUEST_BODY_KEY, body);
  }

  @Test
  public void testJsonReaderRejectsMalformedSyntax() {
    final var exception =
        expectThrows(
            SolrException.class,
            () -> readJsonBody(CreateCollectionRequestBody.class, "{\"name\":", null));
    assertEquals(400, exception.code());
    assertTrue(exception.getCause() instanceof JsonParseException);
    assertTrue(exception.getMessage().startsWith("Invalid request body:"));
  }

  @Test
  public void testJsonReaderRejectsWrongFieldType() {
    final var exception =
        expectThrows(
            SolrException.class,
            () ->
                readJsonBody(
                    CreateCollectionRequestBody.class, "{\"replicationFactor\":\"abc\"}", null));
    assertEquals(400, exception.code());
    assertTrue(exception.getCause() instanceof MismatchedInputException);
    assertTrue(exception.getMessage().contains("abc"));
  }

  @Test
  public void testJsonReaderPreservesDefinitionFailures() {
    // An abstract server-side entity type cannot be repaired by changing the request body.
    expectThrows(InvalidDefinitionException.class, () -> readJsonBody(Runnable.class, "{}", null));
  }

  @Test
  public void testServerJacksonFailuresRemainServerErrors() throws Exception {
    final var definitionFailure =
        expectThrows(
            InvalidDefinitionException.class,
            () -> readJsonBody(Runnable.class, "{}", null));
    final var outputFailure =
        expectThrows(
            JsonMappingException.class,
            () -> SolrJacksonMapper.getObjectMapper().writeValueAsString(new FailingResponse()));

    final var handler = new ApplicationHandler(new JerseyApplications.CoreContainerApp());
    try {
      final var mappers = handler.getInjectionManager().getInstance(ExceptionMappers.class);
      for (Exception failure : List.of(definitionFailure, outputFailure)) {
        final var mapper = mappers.findMapping(failure);
        assertEquals(CatchAllExceptionMapper.class, mapper.getClass());
        final var catchAll = (CatchAllExceptionMapper) mapper;
        final var context = mock(ContainerRequestContext.class);
        when(context.getProperty(SOLR_QUERY_RESPONSE)).thenReturn(new SolrQueryResponse());
        catchAll.resourceContext = mock(ResourceContext.class);
        when(catchAll.resourceContext.getResource(ContainerRequestContext.class)).thenReturn(context);
        assertEquals(500, catchAll.toResponse(failure).getStatus());
      }
    } finally {
      handler.getInjectionManager().shutdown();
    }
  }

  @SuppressWarnings("unchecked")
  private Object readJsonBody(
      Class<?> type, String json, ContainerRequestContext context) throws Exception {
    final var reader = new MessageBodyReaders.CachingJsonMessageBodyReader();
    reader.serviceLocator = mock(ServiceLocator.class);
    when(reader.serviceLocator.getService(ContainerRequestContext.class)).thenReturn(context);
    return reader.readFrom(
        (Class<Object>) type,
        type,
        new Annotation[0],
        MediaType.APPLICATION_JSON_TYPE,
        new MultivaluedHashMap<>(),
        new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
  }

  public static class FailingResponse {
    public String getValue() {
      throw new IllegalStateException("server serialization failure");
    }
  }

  @Test
  public void testRequestBodyStringIsEmptyIfNoRequestBodyFound() {
    // NOTE: no request body is set on the context.
    final var mockContext = mock(ContainerRequestContext.class);

    final var requestBodyStr = PostRequestLoggingFilter.buildRequestBodyString(mockContext);

    assertEquals("{}", requestBodyStr);
  }

  @Test
  public void testRequestBodyStringIsEmptyIfRequestBodyWasUnexpectedType() {
    // NOTE: Request body is set, but of an unexpected type (i.e. not a JacksonReflectMapWriter)
    final var mockContext = mock(ContainerRequestContext.class);
    final var mockUriInfo = mock(UriInfo.class);
    when(mockUriInfo.getPath()).thenReturn("/somepath");
    when(mockContext.getUriInfo()).thenReturn(mockUriInfo);
    when(mockContext.getProperty(DESERIALIZED_REQUEST_BODY_KEY)).thenReturn("unexpectedType");

    final var requestBodyStr = PostRequestLoggingFilter.buildRequestBodyString(mockContext);

    assertEquals("{}", requestBodyStr);
  }

  @Test
  public void testRequestBodyRepresentedAsJsonWhenFound() {
    final var requestBody = new CreateReplicaRequestBody();
    requestBody.name = "someReplicaName";
    requestBody.type = "NRT";
    requestBody.async = "someAsyncId";
    final var mockContext = mock(ContainerRequestContext.class);
    when(mockContext.getProperty(DESERIALIZED_REQUEST_BODY_KEY)).thenReturn(requestBody);

    final var requestBodyStr = PostRequestLoggingFilter.buildRequestBodyString(mockContext);

    assertEquals(
        "{  \"name\":\"someReplicaName\",  \"type\":\"NRT\",  \"async\":\"someAsyncId\"}",
        requestBodyStr);
  }
}
