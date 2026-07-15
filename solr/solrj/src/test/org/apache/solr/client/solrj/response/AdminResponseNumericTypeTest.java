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

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.junit.Test;

/**
 * Non-binary parsers deliver integers as Long. These response classes widen via Number rather than
 * casting to Integer/Long/Float, so a Long value must not throw (SOLR-17316). Each assertion fails
 * with a ClassCastException without the widening.
 */
public class AdminResponseNumericTypeTest extends SolrTestCase {

  /** AnalysisResponseBase.buildTokenInfo: start/end/position widened from Number. */
  @Test
  public void testAnalysisTokenInfo() {
    NamedList<Object> token = new SimpleOrderedMap<>();
    token.add("text", "foo");
    token.add("start", 1L); // JSON yields Long
    token.add("end", 4L);
    token.add("position", 2L);

    var probe =
        new AnalysisResponseBase() {
          TokenInfo build(NamedList<?> nl) {
            return buildTokenInfo(nl);
          }
        };
    AnalysisResponseBase.TokenInfo info = probe.build(token);
    assertEquals(1, info.getStart());
    assertEquals(4, info.getEnd());
    assertEquals(2, info.getPosition());
  }

  /** LukeResponse.getMaxDoc/getNumTerms: widened from Number. */
  @Test
  public void testLukeIndexInfo() {
    NamedList<Object> index = new SimpleOrderedMap<>();
    index.add("maxDoc", 10L); // JSON yields Long
    index.add("numTerms", 42L);
    NamedList<Object> body = new SimpleOrderedMap<>();
    body.add("index", index);

    LukeResponse r = new LukeResponse();
    r.setResponse(body);
    assertEquals(Integer.valueOf(10), r.getMaxDoc());
    assertEquals(Integer.valueOf(42), r.getNumTerms());
  }

  /** LukeResponse.FieldInfo.distinct: widened from Number. */
  @Test
  public void testLukeFieldDistinct() {
    NamedList<Object> field = new SimpleOrderedMap<>();
    field.add("type", "string");
    field.add("distinct", 5L); // JSON yields Long
    NamedList<Object> fields = new SimpleOrderedMap<>();
    fields.add("cat", field);
    NamedList<Object> body = new SimpleOrderedMap<>();
    body.add("fields", fields);

    LukeResponse r = new LukeResponse();
    r.setResponse(body);
    assertEquals(5, r.getFieldInfo("cat").getDistinct());
  }

  /** SchemaResponse.getSchemaVersion: widened from Number (JSON yields Double for 1.6). */
  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testSchemaVersion() {
    Map schema = new LinkedHashMap();
    schema.put("version", 1.6d); // JSON yields Double
    schema.put("fields", new java.util.ArrayList<>());
    schema.put("dynamicFields", new java.util.ArrayList<>());
    schema.put("fieldTypes", new java.util.ArrayList<>());
    schema.put("copyFields", new java.util.ArrayList<>());
    NamedList<Object> body = new SimpleOrderedMap<>();
    body.add("schema", schema);

    org.apache.solr.client.solrj.response.schema.SchemaResponse r =
        new org.apache.solr.client.solrj.response.schema.SchemaResponse();
    r.setResponse(body);
    Float version = r.getSchemaRepresentation().getVersion();
    assertEquals(1.6f, version.floatValue(), 0.0001f);
  }
}
