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
package org.apache.solr.response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test that demonstrates NoOpResponseWriter can be used to disable implicit response writers that
 * are loaded via ImplicitPlugins.json.
 */
public class NoOpResponseWriterTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-noop.xml", "schema.xml");
  }

  @Test
  public void testWrite() throws IOException {
    NoOpResponseWriter writer = new NoOpResponseWriter();

    Writer stringWriter = new StringWriter();

    writer.write(stringWriter, null, null);

    assertEquals(NoOpResponseWriter.MESSAGE, stringWriter.toString());
  }

  @Test
  public void testGetContentType() {
    NoOpResponseWriter writer = new NoOpResponseWriter();

    String contentType = writer.getContentType(null, null);
    assertEquals(QueryResponseWriter.CONTENT_TYPE_TEXT_UTF8, contentType);
  }

  @Test
  public void testCsvResponseWriterDisabled() throws Exception {
    QueryResponseWriter csvWriter = h.getCore().getQueryResponseWriter("csv");

    assertNotNull("CSV response writer should be registered", csvWriter);
    assertTrue(
        "CSV response writer should be NoOpResponseWriter, not the implicit CSVResponseWriter",
        csvWriter instanceof NoOpResponseWriter);

    // Verify it returns the NoOp message when used
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    csvWriter.write(out, null, null);
    String output = out.toString(StandardCharsets.UTF_8);
    assertEquals("CSV writer should return NoOp message", NoOpResponseWriter.MESSAGE, output);
  }
}
