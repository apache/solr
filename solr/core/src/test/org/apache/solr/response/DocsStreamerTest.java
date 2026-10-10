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

import java.util.Arrays;
import java.util.List;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;

public class DocsStreamerTest extends SolrTestCaseJ4 {

  private static final List<Float> VECTOR = Arrays.asList(1.1f, 2.2f, 3.3f, 4.4f);

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(
        "solr.index.updatelog.enabled", "false"); // schema12 doesn't support _version_
  }

  // Each test method initializes its own core, because the schemas holding the quantized
  // field types are not the same file as the one holding the plain DenseVectorField.
  public void testDenseVectorField() throws Exception {
    try {
      initCore("solrconfig.xml", "schema12.xml");
      assertStoredValues("vector", VECTOR);
    } finally {
      deleteCore();
    }
  }

  // ScalarQuantizedDenseVectorField is a subclass of DenseVectorField, and the
  // ExternalizeStoredValuesAsObjects marker is consulted with instanceof, so the subclass takes
  // the same toObject path as its superclass: its stored values are externalized as Floats.
  // This pins that agreement so it cannot change without a test failing.
  public void testScalarQuantizedDenseVectorField() throws Exception {
    try {
      initCore("solrconfig_codec.xml", "schema-densevector-quantized.xml");
      // plain DenseVectorField, marker inherited from FloatPointField: Float objects
      assertStoredValues("vector", VECTOR);
      // subclass of it, so it inherits the marker too: Float objects as well
      assertStoredValues("v_scalar_default", VECTOR);
    } finally {
      deleteCore();
    }
  }

  // Same for the other DenseVectorField subclass.
  public void testBinaryQuantizedDenseVectorField() throws Exception {
    try {
      initCore("solrconfig-basic.xml", "schema-densevector-bq.xml");
      assertStoredValues("v_bq", VECTOR);
    } finally {
      deleteCore();
    }
  }

  // Asserts what DocsStreamer.getValue returns for every stored field that fieldName creates
  // for VECTOR. The first created field is the indexed one, so it is skipped.
  private void assertStoredValues(String fieldName, List<?> expected) {
    SchemaField sf = h.getCore().getLatestSchema().getField(fieldName);
    List<IndexableField> fields = sf.createFields(VECTOR);
    assertEquals(fieldName + " created field count", expected.size() + 1, fields.size());
    for (int idx = 1; idx < fields.size(); ++idx) {
      Object value = DocsStreamer.getValue(sf, fields.get(idx));
      Object want = expected.get(idx - 1);
      String label = fieldName + " element " + (idx - 1);
      assertNotNull(label, value);
      assertEquals(label + " type", want.getClass(), value.getClass());
      assertEquals(label, want, value);
    }
  }
}
