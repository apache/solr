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
package org.apache.solr.client.solrj.request.schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.Utils;
import org.junit.Test;

/** Pins the JSON bodies that {@link SchemaRequest.Update} requests send to the Schema API. */
public class SchemaRequestBodyTest extends SolrTestCase {

  @Test
  public void testAddFieldTypeBodyMatchesPlainMaps() throws IOException {
    FieldTypeDefinition fieldType = new FieldTypeDefinition();
    fieldType.setAttributes(map("name", "text_test", "class", "solr.TextField"));
    AnalyzerDefinition analyzer = new AnalyzerDefinition();
    analyzer.setAttributes(map("type", "index"));
    analyzer.setCharFilters(List.of(map("class", "solr.HTMLStripCharFilterFactory")));
    analyzer.setTokenizer(map("class", "solr.StandardTokenizerFactory"));
    analyzer.setFilters(
        List.of(
            map("class", "solr.LowerCaseFilterFactory"),
            map("class", "solr.ASCIIFoldingFilterFactory")));
    fieldType.setIndexAnalyzer(analyzer);
    fieldType.setSimilarity(map("class", "solr.BM25SimilarityFactory", "k1", 1.2));

    Map<String, Object> expected =
        map(
            "add-field-type",
            map(
                "name",
                "text_test",
                "class",
                "solr.TextField",
                "indexAnalyzer",
                map(
                    "type",
                    "index",
                    "charFilters",
                    List.of(map("class", "solr.HTMLStripCharFilterFactory")),
                    "tokenizer",
                    map("class", "solr.StandardTokenizerFactory"),
                    "filters",
                    List.of(
                        map("class", "solr.LowerCaseFilterFactory"),
                        map("class", "solr.ASCIIFoldingFilterFactory"))),
                "similarity",
                map("class", "solr.BM25SimilarityFactory", "k1", 1.2)));

    assertEquals(json(expected), body(new SchemaRequest.AddFieldType(fieldType)));
  }

  @Test
  public void testAddFieldBodyMatchesPlainMaps() throws IOException {
    Map<String, Object> attributes = map("name", "f1", "type", "string", "stored", true);
    assertEquals(json(map("add-field", attributes)), body(new SchemaRequest.AddField(attributes)));
  }

  @Test
  public void testMultiUpdateRepeatsCommandKeys() throws IOException {
    SchemaRequest.MultiUpdate multiUpdate =
        new SchemaRequest.MultiUpdate(
            List.of(
                new SchemaRequest.AddField(map("name", "f1", "type", "string")),
                new SchemaRequest.AddField(map("name", "f2", "type", "string")),
                new SchemaRequest.DeleteField("f0")));
    String body = body(multiUpdate);

    // The Schema API takes several commands of the same kind in one object, so the body must keep
    // the repeated keys instead of collapsing them as a Map would.
    assertEquals(2, count(body, "\"add-field\":"));
    assertEquals(
        "{"
            + entry("add-field", map("name", "f1", "type", "string"))
            + ","
            + entry("add-field", map("name", "f2", "type", "string"))
            + ","
            + entry("delete-field", map("name", "f0"))
            + "}",
        body);
  }

  /** The {@code "key":value} text of a one-entry object, without its braces. */
  private static String entry(String key, Object value) {
    String object = json(map(key, value));
    return object.substring(1, object.length() - 1);
  }

  /** Compact JSON, as the request writers produce it. */
  private static String json(Object o) {
    return Utils.toJSONString(o, -1);
  }

  private static String body(SchemaRequest.Update request) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    request.getContentWriter("application/json").write(out);
    return out.toString(StandardCharsets.UTF_8);
  }

  private static int count(String haystack, String needle) {
    int count = 0;
    for (int i = haystack.indexOf(needle); i >= 0; i = haystack.indexOf(needle, i + 1)) {
      count++;
    }
    return count;
  }

  private static Map<String, Object> map(Object... keysAndValues) {
    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      map.put((String) keysAndValues[i], keysAndValues[i + 1]);
    }
    return map;
  }
}
