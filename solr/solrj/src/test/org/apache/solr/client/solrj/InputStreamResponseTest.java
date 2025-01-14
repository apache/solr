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

import static org.hamcrest.Matchers.containsString;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.impl.InputStreamResponseParser;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

/** Unit tests for {@link InputStreamResponse} */
public class InputStreamResponseTest extends SolrTestCase {

  @Test
  public void testDetectsWhenResponseDoesntMatchExpectedFormat() {
    final var inputStreamResponse = new InputStreamResponse();
    final var rawNl = new NamedList<Object>();
    rawNl.add("someKey", "someVal");

    final var thrown =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              inputStreamResponse.setResponse(rawNl);
            });
    assertThat(thrown.getMessage(), containsString("Missing key 'stream'"));
  }

  @Test
  public void testAllowsAccessToStatusAndStream() throws IOException {
    final var inputStreamResponse = new InputStreamResponse();
    final var rawNl =
        InputStreamResponseParser.createInputStreamNamedList(
            200, new ByteArrayInputStream(new byte[] {'h', 'e', 'l', 'l', 'o'}));
    inputStreamResponse.setResponse(rawNl);

    assertEquals(200, inputStreamResponse.getHttpStatus());
    try (final var is = inputStreamResponse.getResponseStream()) {
      final var streamVal = new String(is.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals("hello", streamVal);
    }
  }

  @Test
  public void testThrowsErrorIfUnexpectedResponseEncountered() {
    final var inputStreamResponse = new InputStreamResponse();
    final var rawNl =
        InputStreamResponseParser.createInputStreamNamedList(
            500, new ByteArrayInputStream(new byte[] {'h', 'e', 'l', 'l', 'o'}));
    inputStreamResponse.setResponse(rawNl);

    final var thrown =
        expectThrows(
            SolrException.class,
            () -> {
              inputStreamResponse.getResponseStreamIfSuccessful();
            });
    assertEquals(500, thrown.code()); // Matches that status of the HTTP response
    assertThat(thrown.getMessage(), containsString("Unexpected status code"));
  }
}
