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

import java.util.List;
import javax.ws.rs.core.MultivaluedHashMap;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

/**
 * Unit tests for {@link PostRequestLoggingFilter}
 *
 * <p>Tests primarily focus on exercising the string operations used to flatten the request into a
 * log message.
 */
public class PostRequestLoggingFilterTest extends SolrTestCaseJ4 {
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
}
