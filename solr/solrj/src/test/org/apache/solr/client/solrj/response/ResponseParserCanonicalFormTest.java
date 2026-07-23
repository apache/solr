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

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.response.json.JsonMapResponseParser;
import org.junit.Test;

/**
 * Pins the producesCanonicalForm() contract that gates response normalization: only the JSON map
 * parser (which yields raw Maps/Lists) needs normalizing; the parsers that already produce the
 * canonical NamedList/SolrDocumentList shape must report true so they pass through untouched.
 */
public class ResponseParserCanonicalFormTest extends SolrTestCase {

  @Test
  public void testCanonicalParsersReportTrue() {
    assertTrue(new JavaBinResponseParser().producesCanonicalForm());
    assertTrue(new XMLResponseParser().producesCanonicalForm());
    assertTrue(new InputStreamResponseParser("json").producesCanonicalForm());
  }

  @Test
  public void testJsonMapParserReportsFalse() {
    assertFalse(new JsonMapResponseParser().producesCanonicalForm());
  }
}
