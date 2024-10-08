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

package org.apache.solr.client.solrj;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.solrj.request.CollectionsApi.ListCollectionsResponse;
import org.apache.solr.common.util.NamedList;
import org.junit.Assert;
import org.junit.Test;

/**
 * A test ensuring that specific generated SolrRequest classes deserialize responses into the
 * correct type.
 *
 * <p>See SOLR-17326 for more context. Consider removing once SOLR-17329 has been completed.
 */
public class ApiMustacheTemplateTests {

  /**
   * Tests the return type in generated response classes. This test ensures that responses are still
   * returning API models in case of a naming conflict between response class and return type (API
   * model).
   */
  @Test
  public void testParsedReturnTypes() {
    JacksonParsingResponse<?> response = getJacksonParsingResponse();

    Object data = null;
    try {
      // Parsing may fail if wrong class is used
      data = response.getParsed();
    } catch (Exception e) {
      // Parsing should not fail for the given example.
      Assert.fail("Response parsing failed with error " + e.getMessage());
    }

    // In case of a naming conflict, even if parsing would succeed, the type would still be the same
    // as response
    Assert.assertNotSame(data.getClass(), response.getClass());
    Assert.assertFalse(data instanceof JacksonParsingResponse<?>);

    // Currently all response types extend SolrJerseyResponse. Adjust if this change in the future.
    Assert.assertTrue(data instanceof SolrJerseyResponse);
  }

  /**
   * @return Returns a dummy response of {@link ListCollectionsResponse} with data.
   */
  private static JacksonParsingResponse<?> getJacksonParsingResponse() {
    JacksonParsingResponse<?> response = new ListCollectionsResponse(); // API response

    // Provide a dummy response for ListCollectionsResponse
    InputStream inputStream =
        new ByteArrayInputStream(
            "{\"responseHeader\":{\"status\":0,\"QTime\":0},\"collections\":[\"testCollection\"]}"
                .getBytes(StandardCharsets.UTF_8));
    NamedList<Object> responseData = new NamedList<>();
    responseData.add("stream", inputStream);
    response.setResponse(responseData);

    return response;
  }
}
