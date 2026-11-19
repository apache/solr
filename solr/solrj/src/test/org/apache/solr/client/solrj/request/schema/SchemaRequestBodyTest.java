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
import org.junit.Test;

/** Pins the JSON bodies that {@link SchemaRequest.Update} requests send to the Schema API. */
public class SchemaRequestBodyTest extends SolrTestCase {

  @Test
  public void testAddFieldTypeBody() throws IOException {
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

    assertJSONEquals(
        """
        {
          "add-field-type": {
            "name": "text_test",
            "class": "solr.TextField",
            "indexAnalyzer": {
              "type": "index",
              "charFilters": [{"class": "solr.HTMLStripCharFilterFactory"}],
              "tokenizer": {"class": "solr.StandardTokenizerFactory"},
              "filters": [
                {"class": "solr.LowerCaseFilterFactory"},
                {"class": "solr.ASCIIFoldingFilterFactory"}
              ]
            },
            "similarity": {"class": "solr.BM25SimilarityFactory", "k1": 1.2}
          }
        }
        """,
        body(new SchemaRequest.AddFieldType(fieldType)));
  }

  @Test
  public void testAddFieldBody() throws IOException {
    assertJSONEquals(
        """
        {"add-field": {"name": "f1", "type": "string", "stored": true}}
        """,
        body(new SchemaRequest.AddField(map("name", "f1", "type", "string", "stored", true))));
  }

  @Test
  public void testMultiUpdateRepeatsCommandKeys() throws IOException {
    SchemaRequest.MultiUpdate multiUpdate =
        new SchemaRequest.MultiUpdate(
            List.of(
                new SchemaRequest.AddField(map("name", "f1", "type", "string")),
                new SchemaRequest.AddField(map("name", "f2", "type", "string")),
                new SchemaRequest.DeleteField("f0")));

    // The Schema API takes several commands of the same kind in one object (ref guide, Schema API,
    // "Multiple Commands in a Single POST"); the server reads the body key by key with
    // CommandOperation.parse, so the repeated key is preserved and each command runs. Compared as
    // text: a JSON parser would collapse the duplicate.
    assertEquals(
        """
        {"add-field":{"name":"f1","type":"string"},\
        "add-field":{"name":"f2","type":"string"},\
        "delete-field":{"name":"f0"}}""",
        body(multiUpdate));
  }

  private static String body(SchemaRequest.Update request) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    request.getContentWriter("application/json").write(out);
    return out.toString(StandardCharsets.UTF_8);
  }

  private static Map<String, Object> map(Object... keysAndValues) {
    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      map.put((String) keysAndValues[i], keysAndValues[i + 1]);
    }
    return map;
  }
}
