/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.jersey;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.util.Utils;
import org.junit.Test;

/**
 * Unit tests for the {@link JacksonReflectMapWriter} interface.
 *
 * <p>The core logic being tested here really lives in SolrJ's {@link Utils#reflectWrite}, and is
 * tested in SolrJ accordingly. But it's still useful to have some sanity check tests here in
 * 'core', where JAX-RS response classes such as {@link SolrJerseyResponse} and libraries such as
 * Jackson can be accessed.
 */
public class JacksonReflectMapWriterTest extends SolrTestCaseJ4 {

  @Test
  public void testJacksonSerializationHandlesUnknownProperties() throws IOException {
    final SolrJerseyResponse response = new SolrJerseyResponse();
    response.responseHeader.status = 123;
    response.responseHeader.setUnknownProperty("someField", "someVal");

    final String jsonOutput = new ObjectMapper().writeValueAsString(response);

    assertTrue(
        "Expected JSON " + jsonOutput + " to contain different value",
        jsonOutput.contains("\"status\":123"));
    assertTrue(
        "Expected JSON " + jsonOutput + " to contain different value",
        jsonOutput.contains("\"someField\":\"someVal\""));
  }

  @Test
  public void testJacksonDeserializationHandlesUnknownProperties() throws IOException {
    final String inputJson =
        "{\"responseHeader\": {\"status\": 123, \"someUnexpectedField\": \"someVal\"}}";

    final SolrJerseyResponse response =
        new ObjectMapper()
            .readValue(inputJson.getBytes(StandardCharsets.UTF_8), SolrJerseyResponse.class);

    assertEquals(123, response.responseHeader.status);
    assertEquals("someVal", response.responseHeader.unknownProperties().get("someUnexpectedField"));
  }
}
