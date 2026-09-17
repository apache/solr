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
import org.apache.solr.client.solrj.response.json.CanonicalJsonResponseParser;
import org.apache.solr.client.solrj.response.json.JsonMapResponseParser;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

/**
 * Pins {@link ResponseParser#processResponse}'s canonical-shape contract: a {@link NamedList} tree
 * with {@link SolrDocumentList} for document sections. {@link JsonMapResponseParser} is the one
 * deliberate exception — {@link CanonicalJsonResponseParser} is the subclass that converts.
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

  /** ... and the canonical subclass converts it, without the caller asking. */
  @Test
  public void testCanonicalJsonResponseParserConverts() throws Exception {
    NamedList<Object> out = new CanonicalJsonResponseParser().processResponse(json(), null);
    assertTrue("header must be a NamedList", out.get("responseHeader") instanceof NamedList);
    assertTrue(
        "response must be a SolrDocumentList", out.get("response") instanceof SolrDocumentList);
    assertEquals(1, ((SolrDocumentList) out.get("response")).getNumFound());
  }

  /** A parser that is canonical by construction needs no conversion. */
  @Test
  public void testXmlResponseParserIsAlreadyCanonical() throws Exception {
    String xml =
        """
        <?xml version="1.0" encoding="UTF-8"?>
        <response><lst name="responseHeader"><int name="status">0</int></lst></response>""";
    NamedList<Object> out =
        new XMLResponseParser()
            .processResponse(new ByteArrayInputStream(xml.getBytes(UTF_8)), null);
    assertTrue("header must be a NamedList", out.get("responseHeader") instanceof NamedList);
  }
}
