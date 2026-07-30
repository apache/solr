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

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Before;
import org.junit.BeforeClass;

public class TestSchemaField extends SolrTestCaseJ4 {

  @BeforeClass
  public static void create() throws Exception {
    initCore("solrconfig_codec.xml", "schema_postingsformat.xml");
  }

  @Before
  public void cleanup() {
    clearIndex();
  }

  public void testFieldTypes() {
    assertFieldTypeFormats("str_none", null, null);
    assertFieldTypeFormats("str_direct_asserting", "Direct", "Asserting");
    assertFieldTypeFormats("str_standard_simple", "Lucene84", "Lucene80");
  }

  private void assertFieldTypeFormats(
      String fieldTypeName, String expectedPostingsFormat, String expectedDocValuesFormat) {
    FieldType ft = h.getCore().getLatestSchema().getFieldTypeByName(fieldTypeName);
    assertNotNull("Field type " + fieldTypeName + " not found  - schema got changed?", ft);
    assertEquals(
        "Field type "
            + ft.getTypeName()
            + " wrong "
            + FieldProperties.POSTINGS_FORMAT
            + "  - schema got changed?",
        expectedPostingsFormat,
        ft.getNamedPropertyValues(true).get(FieldProperties.POSTINGS_FORMAT));
    assertEquals(
        "Field type "
            + ft.getTypeName()
            + " wrong "
            + FieldProperties.DOC_VALUES_FORMAT
            + "  - schema got changed?",
        expectedDocValuesFormat,
        ft.getNamedPropertyValues(true).get(FieldProperties.DOC_VALUES_FORMAT));
  }

  public void testFields() {
    assertFieldFormats("str_none_f", null, null);
    assertFieldFormats("str_direct_asserting_f", "Direct", "Asserting");
    assertFieldFormats("str_standard_simple_f", "Lucene84", "Lucene80");

    assertFieldFormats("str_none_lucene80_f", "Lucene80", null);
    assertFieldFormats("str_standard_lucene90_f", "Lucene90", "Lucene80");

    assertFieldFormats("str_none_asserting_f", null, "Asserting");
    assertFieldFormats("str_standard_asserting_f", "Lucene84", "Asserting");
  }

  public void testDynamicFields() {
    assertFieldFormats("any_lucene80", "Lucene80", "Asserting");
    assertFieldFormats("any_direct", "Direct", "Asserting");
    assertFieldFormats("any_lucene70", "Lucene70", null);

    assertFieldFormats("any_asserting", null, "Asserting");
    assertFieldFormats("any_simple", "Direct", "Lucene80");
  }

  private void assertFieldFormats(
      String fieldName, String expectedPostingsFormat, String expectedDocValuesFormat) {
    SchemaField f = h.getCore().getLatestSchema().getField(fieldName);
    assertNotNull("Field " + fieldName + " not found  - schema got changed?", f);
    assertEquals(
        "Field "
            + f.getName()
            + " wrong "
            + FieldProperties.POSTINGS_FORMAT
            + "  - schema got changed?",
        expectedPostingsFormat,
        f.getPostingsFormat());
    assertEquals(
        "Field "
            + f.getName()
            + " wrong "
            + FieldProperties.DOC_VALUES_FORMAT
            + "  - schema got changed?",
        expectedDocValuesFormat,
        f.getDocValuesFormat());
  }

  public void testSchemaCodecFactory() {
    // Verify that the PostingsFormat is the one overridden in the field, not the field type.
    PostingsFormat postingsFormat = h.getCore().getCodec().postingsFormat();
    assertTrue(postingsFormat instanceof PerFieldPostingsFormat);
    PerFieldPostingsFormat perFieldPostingsFormat = (PerFieldPostingsFormat) postingsFormat;
    assertEquals(
        "Lucene90",
        perFieldPostingsFormat.getPostingsFormatForField("str_standard_lucene90_f").getName());

    // Verify that the DocValuesFormat is the one overridden in the field, not the field type.
    DocValuesFormat docValuesFormat = h.getCore().getCodec().docValuesFormat();
    assertTrue(docValuesFormat instanceof PerFieldDocValuesFormat);
    PerFieldDocValuesFormat perFieldPDocValuesFormat = (PerFieldDocValuesFormat) docValuesFormat;
    assertEquals(
        "Asserting",
        perFieldPDocValuesFormat.getDocValuesFormatForField("str_standard_asserting_f").getName());
  }
}
