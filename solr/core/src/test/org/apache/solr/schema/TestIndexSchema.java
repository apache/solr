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
package org.apache.solr.schema;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import org.apache.commons.io.input.ReaderInputStream;
import org.apache.lucene.util.Version;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.Assert;
import org.junit.Test;

public class TestIndexSchema {
  private static final String schemaXML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
          + "<schema name=\"test-x\" version=\"0.1\">\n"
          + "  <types>\n"
          + "    <fieldType name=\"int\" class=\"solr.TrieIntField\" precisionStep=\"0\" omitNorms=\"true\" positionIncrementGap=\"0\"/>\n"
          + "  </types>\n"
          + "  <fields></fields>\n"
          + " </schema>\n";

  private static IndexSchema newIndexSchema(String xml) {
    SolrResourceLoader loader = new SolrResourceLoader(FileSystems.getDefault().getPath("."));
    return new IndexSchema(
        "test",
        IndexSchemaFactory.getConfigResource(null, asInputStream(xml), loader, "test-resource"),
        Version.LATEST,
        loader,
        null);
  }

  private static void addIntField(IndexSchema sch, String name) {
    sch.fields.put(name, new SchemaField(name, sch.fieldTypes.get("int")));
  }

  private static String toXml(IndexSchema sch) throws IOException {
    StringWriter sw = new StringWriter();
    sch.persist(sw);
    return sw.toString();
  }

  @Test
  public void testDuplicateFields() throws IOException {
    IndexSchema sch = newIndexSchema(schemaXML);
    Assert.assertNotNull(sch);
    addIntField(sch, "a#20;b");
    addIntField(sch, "a\u0014b");
    String xml = toXml(sch);
    IndexSchema sch2 = newIndexSchema(xml);
    Assert.assertEquals(2, sch2.fields.size());
  }

  @Test
  public void testFieldNameFidelity() throws IOException {
    IndexSchema sch = newIndexSchema(schemaXML);
    Assert.assertNotNull(sch);
    addIntField(sch, "a\u0014b");
    String xml = toXml(sch);
    IndexSchema sch2 = newIndexSchema(xml);
    Assert.assertEquals(1, sch2.fields.size());
    Assert.assertNotNull(sch2.fields.get("a\u0014b"));
  }

  private static ReaderInputStream asInputStream(String xml) {
    return new ReaderInputStream(new StringReader(xml), StandardCharsets.UTF_8);
  }
}
