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

import java.util.Arrays;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.solr.core.AbstractBadConfigTestBase;

/**
 * Basic tests of {@link StrFloatLateInteractionVectorField} FieldType &amp; SchemaField
 * initialization
 */
public class TestLateInteractionVectorFieldInit extends AbstractBadConfigTestBase {

  public void test_bad_ft_opts() throws Exception {
    assertConfigs(
        "solrconfig-basic.xml",
        "bad-schema-late-vec-ft-nodim.xml",
        StrFloatLateInteractionVectorField.VECTOR_DIMENSION);
    assertConfigs(
        "solrconfig-basic.xml",
        "bad-schema-late-vec-ft-sim.xml",
        StrFloatLateInteractionVectorField.SIMILARITY_FUNCTION);
    assertConfigs(
        "solrconfig-basic.xml",
        "bad-schema-late-vec-ft-nodv.xml",
        "require these properties to be true: docValues");
    assertConfigs(
        "solrconfig-basic.xml",
        "bad-schema-late-vec-ft-indexed.xml",
        "require these properties to be false:");
  }

  public void test_bad_field_opts() throws Exception {
    assertConfigs(
        "solrconfig-basic.xml", "bad-schema-late-vec-field-nodv.xml", "docValues: bad_field");
    assertConfigs(
        "solrconfig-basic.xml", "bad-schema-late-vec-field-indexed.xml", "indexed: bad_field");
    assertConfigs(
        "solrconfig-basic.xml",
        "bad-schema-late-vec-field-multivalued.xml",
        "multiValued: bad_field");
  }

  public void test_SchemaFields() throws Exception {
    try {
      initCore("solrconfig-basic.xml", "schema-late-vec.xml");
      final IndexSchema schema = h.getCore().getLatestSchema();

      final SchemaField def3 = schema.getField("lv_3_def");
      final SchemaField def4 = schema.getField("lv_4_def");
      final SchemaField nostored3 = schema.getField("lv_3_nostored");
      final SchemaField nostored4 = schema.getField("lv_4_nostored");
      final SchemaField cosine4 = schema.getField("lv_4_cosine");

      // these should be true for everyone
      for (SchemaField sf : Arrays.asList(def3, def4, cosine4, nostored3, nostored4)) {
        assertNotNull(sf.getName(), sf);
        assertNotNull(sf.getName(), sf.getType());
        assertNotNull(sf.getName(), sf.getType() instanceof StrFloatLateInteractionVectorField);
        assertTrue(sf.getName(), sf.hasDocValues());
        assertFalse(sf.getName(), sf.multiValued());
        assertFalse(sf.getName(), sf.indexed());
      }

      for (SchemaField sf : Arrays.asList(def3, nostored3)) {
        assertEquals(
            sf.getName(), 3, ((StrFloatLateInteractionVectorField) sf.getType()).getDimension());
      }
      for (SchemaField sf : Arrays.asList(def4, cosine4, nostored4)) {
        assertEquals(
            sf.getName(), 4, ((StrFloatLateInteractionVectorField) sf.getType()).getDimension());
      }
      for (SchemaField sf : Arrays.asList(def3, def4, cosine4)) {
        assertTrue(sf.getName(), sf.stored());
      }
      for (SchemaField sf : Arrays.asList(nostored3, nostored4)) {
        assertFalse(sf.getName(), sf.stored());
      }
      for (SchemaField sf : Arrays.asList(def3, def4, nostored3, nostored4)) {
        assertEquals(
            sf.getName(),
            StrFloatLateInteractionVectorField.DEFAULT_SIMILARITY,
            ((StrFloatLateInteractionVectorField) sf.getType()).getSimilarityFunction());
      }

      assertEquals(
          cosine4.getName(),
          VectorSimilarityFunction.COSINE,
          ((StrFloatLateInteractionVectorField) cosine4.getType()).getSimilarityFunction());

    } finally {
      deleteCore();
    }
  }
}
