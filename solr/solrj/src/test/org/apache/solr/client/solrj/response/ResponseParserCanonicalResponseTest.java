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
package org.apache.solr.client.solrj.response;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.response.json.JsonMapResponseParser;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.JsonTextWriter;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

/**
 * Pins the {@link ResponseParser#processCanonicalResponse} contract: whatever a parser's natural
 * output looks like, this method returns the canonical shape the SolrJ response classes read — a
 * NamedList tree with SolrDocumentList for document sections. The conversion belongs to the parser,
 * so a client does not need to know which parsers require it.
 */
public class ResponseParserCanonicalResponseTest extends SolrTestCase {

  private static final String JSON =
      """
      {"responseHeader":{"status":0,"QTime":1},\
      "response":{"numFound":1,"start":0,"numFoundExact":true,"docs":[{"id":"1"}]}}""";

  private static InputStream json() {
    return new ByteArrayInputStream(JSON.getBytes(UTF_8));
  }

  /** The JSON map parser's own output is raw: Maps where the response classes expect NamedLists. */
  @Test
  public void testJsonMapParserRawOutputIsNotCanonical() throws Exception {
    NamedList<Object> raw = new JsonMapResponseParser().processResponse(json(), null);
    assertTrue("raw header should be a Map", raw.get("responseHeader") instanceof Map);
    assertFalse(
        "raw header should not be a NamedList", raw.get("responseHeader") instanceof NamedList);
    assertFalse(
        "raw response should not be a SolrDocumentList",
        raw.get("response") instanceof SolrDocumentList);
  }

  /** ... and processCanonicalResponse converts it, without the caller asking. */
  @Test
  public void testJsonMapParserCanonicalResponseIsConverted() throws Exception {
    NamedList<Object> out = new JsonMapResponseParser().processCanonicalResponse(json(), null);
    assertTrue("header must be a NamedList", out.get("responseHeader") instanceof NamedList);
    assertTrue(
        "response must be a SolrDocumentList", out.get("response") instanceof SolrDocumentList);
    assertEquals(1, ((SolrDocumentList) out.get("response")).getNumFound());
  }

  /** Parsers that are canonical already inherit the default and are unchanged by it. */
  @Test
  public void testCanonicalParsersPassThrough() throws Exception {
    String xml =
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <response><lst name="responseHeader"><int name="status">0</int></lst></response>""";
    NamedList<Object> out =
        new XMLResponseParser()
            .processCanonicalResponse(new ByteArrayInputStream(xml.getBytes(UTF_8)), null);
    assertTrue("header must be a NamedList", out.get("responseHeader") instanceof NamedList);
  }

  /**
   * A parser that needs the response written a particular way supplies that param itself, rather
   * than relying on every caller to know it. The JSON map parser needs {@code json.nl=map}: under
   * the default {@code flat} a NamedList arrives as an array of alternating names and values, whose
   * structure cannot be recovered.
   */
  @Test
  public void testJsonMapParserRequestsNlMap() {
    SolrParams params = new JsonMapResponseParser().getRequestParams();
    assertNotNull("the JSON map parser must ask for a recoverable NamedList form", params);
    assertEquals(JsonTextWriter.JSON_NL_MAP, params.get(JsonTextWriter.JSON_NL_STYLE));
  }

  /** Parsers that need nothing beyond wt contribute no params. */
  @Test
  public void testCanonicalParsersRequestNoParams() {
    assertNull(new JavaBinResponseParser().getRequestParams());
    assertNull(new XMLResponseParser().getRequestParams());
  }
}
