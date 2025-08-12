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
package org.apache.solr.search;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.ByteVectorSimilarityFunction;
import org.apache.lucene.queries.function.valuesource.FloatVectorSimilarityFunction;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.BinaryField;
import org.apache.solr.schema.DenseVectorField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IntPointField;
import org.apache.solr.schema.SchemaField;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test for {@link VectorSimilaritySourceParser} */
public class VectorSimilaritySourceParserTest extends SolrTestCase {
  private static final VectorSimilaritySourceParser vecSimilarity =
      new VectorSimilaritySourceParser();
  private SolrQueryRequest request;
  private SolrParams localParams;
  private SolrParams params;
  private IndexSchema indexSchema;

  @BeforeClass
  public static void beforeClass() {
    assumeWorkingMockito();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    resetMocks();
  }

  @Test
  public void testReportErrorPassingZeroArg() throws SyntaxError {
    SolrException error =
        assertThrows(SolrException.class, () -> parseWithMocks("vectorSimilarity()"));
    assertEquals(
        "Invalid number of arguments. Please provide either two or four arguments.",
        error.getMessage());
  }

  @Test
  public void testReportErrorPassingOneArg() throws SyntaxError {
    SolrException error =
        assertThrows(SolrException.class, () -> parseWithMocks("vectorSimilarity(field1)"));
    assertEquals(
        "Invalid number of arguments. Please provide either two or four arguments.",
        error.getMessage());
  }

  @Test
  public void testReportErrorIfSecArgsEmpty() throws Exception {
    SchemaField field1 = new SchemaField("field1", new DenseVectorField(5));
    when(indexSchema.getField("field1")).thenReturn(field1);

    SolrException error =
        assertThrows(SolrException.class, () -> parseWithMocks("vectorSimilarity(field1,)"));
    assertEquals(
        "Invalid number of arguments. Please provide either two or four arguments.",
        error.getMessage());
  }

  @Test
  public void testReportErrorIfFirstArgNotVector() throws SyntaxError {
    SchemaField field1 = new SchemaField("field1", new IntPointField());
    when(indexSchema.getField("field1")).thenReturn(field1);

    SolrException error =
        assertThrows(SolrException.class, () -> parseWithMocks("vectorSimilarity(field1, field2)"));
    assertEquals(
        "Type mismatch: Expected [DenseVectorField], but found a different field type for field: [field1]",
        error.getMessage());
  }

  @Test
  public void testReportErrorIfSecArgNotVector() throws SyntaxError {
    DenseVectorField fieldType = new DenseVectorField(5);
    SchemaField field1 = new SchemaField("field1", fieldType);
    SchemaField field2 = new SchemaField("field2", new BinaryField());
    when(indexSchema.getField("field1")).thenReturn(field1);
    when(indexSchema.getField("field2")).thenReturn(field2);

    SolrException error =
        assertThrows(SolrException.class, () -> parseWithMocks("vectorSimilarity(field1, field2)"));
    assertEquals(
        "Type mismatch: Expected [DenseVectorField], but found a different field type for field: [field2]",
        error.getMessage());
  }

  @Test
  public void testReportErrorIfFieldMissmatch() throws SyntaxError {
    DenseVectorField vectorField1 =
        new DenseVectorField(5, VectorSimilarityFunction.COSINE, VectorEncoding.BYTE);
    SchemaField field1 = new SchemaField("field1", vectorField1);
    DenseVectorField vectorField2 =
        new DenseVectorField(5, VectorSimilarityFunction.COSINE, VectorEncoding.FLOAT32);
    SchemaField field2 = new SchemaField("field2", vectorField2);
    DenseVectorField vectorField3 =
        new DenseVectorField(5, VectorSimilarityFunction.DOT_PRODUCT, VectorEncoding.FLOAT32);
    SchemaField field3 = new SchemaField("field3", vectorField3);

    when(indexSchema.getField("field1")).thenReturn(field1);
    when(indexSchema.getField("field2")).thenReturn(field2);
    when(indexSchema.getField("field3")).thenReturn(field3);

    SolrException error =
        assertThrows(SolrException.class, () -> parseWithMocks("vectorSimilarity(field1, field2)"));
    assertEquals(
        "Invalid arguments: vector field field1 and vector field field2 must have the same vectorEncoding and similarityFunction",
        error.getMessage());

    error =
        assertThrows(SolrException.class, () -> parseWithMocks("vectorSimilarity(field2, field3)"));
    assertEquals(
        "Invalid arguments: vector field field2 and vector field field3 must have the same vectorEncoding and similarityFunction",
        error.getMessage());
  }

  @Test
  public void test2ArgsByteVectorField() throws SyntaxError {
    DenseVectorField vectorField =
        new DenseVectorField(5, VectorSimilarityFunction.COSINE, VectorEncoding.BYTE);
    SchemaField field1 = new SchemaField("field1", vectorField);
    SchemaField field2 = new SchemaField("field2", vectorField);
    when(indexSchema.getField("field1")).thenReturn(field1);
    when(indexSchema.getField("field2")).thenReturn(field2);

    ValueSource valueSource = parseWithMocks("vectorSimilarity(field1, field2)");
    assertTrue(valueSource instanceof ByteVectorSimilarityFunction);
  }

  @Test
  public void test2ArgsFloatVectorAndConst() throws Exception {
    DenseVectorField vectorField =
        new DenseVectorField(5, VectorSimilarityFunction.COSINE, VectorEncoding.FLOAT32);
    SchemaField field1 = new SchemaField("field1", vectorField);
    when(indexSchema.getField("field1")).thenReturn(field1);

    ValueSource valueSource = parseWithMocks("vectorSimilarity(field1, [1, 2, 3, 4, 5])");
    assertTrue(valueSource instanceof FloatVectorSimilarityFunction);
  }

  private void resetMocks() {
    request = mock(SolrQueryRequest.class);
    localParams = mock(SolrParams.class);
    params = mock(SolrParams.class);
    indexSchema = mock(IndexSchema.class);
    when(request.getSchema()).thenReturn(indexSchema);
  }

  protected ValueSource parseWithMocks(final String input) throws SyntaxError {
    final String funcPrefix = "vectorSimilarity(";
    assert input.startsWith(funcPrefix);
    final FunctionQParser fqp =
        new FunctionQParser(input.substring(funcPrefix.length()), localParams, params, request);
    return vecSimilarity.parse(fqp);
  }
}
