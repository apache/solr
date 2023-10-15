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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.UriInfo;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.CreateReplicaRequestBody;
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
