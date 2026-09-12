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

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.response.json.JsonMapResponseParser;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.junit.Test;

/** Tests that {@link SolrResponseBase} getters work across ResponseParsers (SOLR-17316). */
public class SolrResponseBaseTest extends SolrTestCase {

  /** The JSON parser yields a Map header with Long numbers, the case that regressed. */
  @Test
  public void testStatusAndQTimeWithJsonParser() throws Exception {
    String json = "{\"responseHeader\":{\"status\":0,\"QTime\":7}}";
    NamedList<Object> parsed =
        new JsonMapResponseParser()
            .processResponse(
                new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), "UTF-8");

    SolrResponseBase response = new SolrResponseBase();
    response.setResponse(parsed);

    assertEquals(0, response.getStatus());
    assertEquals(7, response.getQTime());
  }

  /** The binary parser yields a NamedList header with Integer numbers (the original happy path). */
  @Test
  public void testStatusAndQTimeWithBinaryStyleHeader() {
    NamedList<Object> header = new SimpleOrderedMap<>();
    header.add("status", 0);
    header.add("QTime", 7);
    NamedList<Object> body = new SimpleOrderedMap<>();
    body.add("responseHeader", header);

    SolrResponseBase response = new SolrResponseBase();
    response.setResponse(body);

    assertEquals(0, response.getStatus());
    assertEquals(7, response.getQTime());
  }

  /** With no responseHeader the getters return 0 rather than throwing. */
  @Test
  public void testStatusAndQTimeWithNoHeader() {
    SolrResponseBase response = new SolrResponseBase();
    response.setResponse(new SimpleOrderedMap<>());

    assertEquals(0, response.getStatus());
    assertEquals(0, response.getQTime());
  }
}
