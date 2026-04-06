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
package org.apache.solr.schema.numericrange;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.document.DoubleRange;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;

/** Tests for {@link DoubleRangeField} */
public class DoubleRangeFieldTest extends SolrTestCase {

  @BeforeClass
  public static void ensureAssumptions() {
    assumeWorkingMockito();
  }

  public void test1DRangeParsing() {
    DoubleRangeField fieldType = createFieldType(1);

    // Valid 1D range with floating-point values
    DoubleRangeField.RangeValue range = fieldType.parseRangeValue("[1.5 TO 3.14]");
    assertEquals(1, range.getDimensions());
    assertEquals(1.5, range.mins[0], 0.0);
    assertEquals(3.14, range.maxs[0], 0.0);

    // Integer values are accepted and parsed as doubles
    range = fieldType.parseRangeValue("[10 TO 20]");
    assertEquals(10.0, range.mins[0], 0.0);
    assertEquals(20.0, range.maxs[0], 0.0);

    // With extra whitespace
    range = fieldType.parseRangeValue("[  1.5   TO   3.14  ]");
    assertEquals(1.5, range.mins[0], 0.0);
    assertEquals(3.14, range.maxs[0], 0.0);

    // Negative numbers
    range = fieldType.parseRangeValue("[-3.5 TO -1.0]");
    assertEquals(-3.5, range.mins[0], 0.0);
    assertEquals(-1.0, range.maxs[0], 0.0);

    // Point range (min == max)
    range = fieldType.parseRangeValue("[5.0 TO 5.0]");
    assertEquals(5.0, range.mins[0], 0.0);
    assertEquals(5.0, range.maxs[0], 0.0);
  }

  public void test2DRangeParsing() {
    DoubleRangeField fieldType = createFieldType(2);

    // Valid 2D range (bounding box)
    DoubleRangeField.RangeValue range = fieldType.parseRangeValue("[1.0,2.0 TO 3.0,4.0]");
    assertEquals(2, range.getDimensions());
    assertEquals(1.0, range.mins[0], 0.0);
    assertEquals(2.0, range.mins[1], 0.0);
    assertEquals(3.0, range.maxs[0], 0.0);
    assertEquals(4.0, range.maxs[1], 0.0);

    // With extra whitespace
    range = fieldType.parseRangeValue("[  1.0 , 2.0   TO   3.0 , 4.0  ]");
    assertEquals(1.0, range.mins[0], 0.0);
    assertEquals(2.0, range.mins[1], 0.0);
    assertEquals(3.0, range.maxs[0], 0.0);
    assertEquals(4.0, range.maxs[1], 0.0);
  }

  public void test3DRangeParsing() {
    DoubleRangeField fieldType = createFieldType(3);

    // Valid 3D range (bounding cube)
    DoubleRangeField.RangeValue range = fieldType.parseRangeValue("[1.0,2.0,3.0 TO 4.0,5.0,6.0]");
    assertEquals(3, range.getDimensions());
    assertEquals(1.0, range.mins[0], 0.0);
    assertEquals(2.0, range.mins[1], 0.0);
    assertEquals(3.0, range.mins[2], 0.0);
    assertEquals(4.0, range.maxs[0], 0.0);
    assertEquals(5.0, range.maxs[1], 0.0);
    assertEquals(6.0, range.maxs[2], 0.0);
  }

  public void test4DRangeParsing() {
    DoubleRangeField fieldType = createFieldType(4);

    // Valid 4D range (tesseract)
    DoubleRangeField.RangeValue range =
        fieldType.parseRangeValue("[1.0,2.0,3.0,4.0 TO 5.0,6.0,7.0,8.0]");
    assertEquals(4, range.getDimensions());
    assertEquals(1.0, range.mins[0], 0.0);
    assertEquals(2.0, range.mins[1], 0.0);
    assertEquals(3.0, range.mins[2], 0.0);
    assertEquals(4.0, range.mins[3], 0.0);
    assertEquals(5.0, range.maxs[0], 0.0);
    assertEquals(6.0, range.maxs[1], 0.0);
    assertEquals(7.0, range.maxs[2], 0.0);
    assertEquals(8.0, range.maxs[3], 0.0);
  }

  public void testInvalidRangeFormat() {
    DoubleRangeField fieldType = createFieldType(1);

    // Missing brackets
    SolrException e1 =
        expectThrows(SolrException.class, () -> fieldType.parseRangeValue("1.5 TO 3.14"));
    assertThat(e1.getMessage(), containsString("Invalid range format"));
    assertThat(e1.getMessage(), containsString("Expected: [min1,min2,... TO max1,max2,...]"));

    // Missing TO keyword
    SolrException e2 =
        expectThrows(SolrException.class, () -> fieldType.parseRangeValue("[1.5 3.14]"));
    assertThat(e2.getMessage(), containsString("Invalid range format"));

    // Empty value
    SolrException e3 = expectThrows(SolrException.class, () -> fieldType.parseRangeValue(""));
    assertThat(e3.getMessage(), containsString("Range value cannot be null or empty"));

    // Null value
    SolrException e4 = expectThrows(SolrException.class, () -> fieldType.parseRangeValue(null));
    assertThat(e4.getMessage(), containsString("Range value cannot be null or empty"));
  }

  public void testInvalidNumbers() {
    DoubleRangeField fieldType = createFieldType(1);

    // Non-numeric values
    SolrException e1 =
        expectThrows(SolrException.class, () -> fieldType.parseRangeValue("[abc TO def]"));
    assertThat(e1.getMessage(), containsString("Invalid range"));
    assertThat(e1.getMessage(), containsString("where min and max values are doubles"));

    // Partially numeric
    SolrException e2 =
        expectThrows(SolrException.class, () -> fieldType.parseRangeValue("[1.5 TO xyz]"));
    assertThat(e2.getMessage(), containsString("Invalid range"));
    assertThat(e2.getMessage(), containsString("where min and max values are doubles"));
  }

  public void testDimensionMismatch() {
    DoubleRangeField fieldType1D = createFieldType(1);
    DoubleRangeField fieldType2D = createFieldType(2);

    // 2D value on 1D field
    SolrException e1 =
        expectThrows(
            SolrException.class, () -> fieldType1D.parseRangeValue("[1.0,2.0 TO 3.0,4.0]"));
    assertThat(e1.getMessage(), containsString("Range dimensions"));
    assertThat(e1.getMessage(), containsString("do not match field type numDimensions"));

    // 1D value on 2D field
    SolrException e2 =
        expectThrows(SolrException.class, () -> fieldType2D.parseRangeValue("[1.0 TO 2.0]"));
    assertThat(e2.getMessage(), containsString("Range dimensions"));
    assertThat(e2.getMessage(), containsString("do not match field type numDimensions"));

    // Min/max dimension mismatch
    SolrException e3 =
        expectThrows(
            SolrException.class,
            () -> fieldType2D.parseRangeValue("[1.0,2.0 TO 3.0]")); // 2D mins, 1D maxs
    assertThat(e3.getMessage(), containsString("Min and max dimensions must match"));
  }

  public void testMinGreaterThanMax() {
    DoubleRangeField fieldType = createFieldType(1);

    // Min > max should fail
    SolrException e1 =
        expectThrows(SolrException.class, () -> fieldType.parseRangeValue("[3.14 TO 1.5]"));
    assertThat(e1.getMessage(), containsString("Min value must be <= max value"));
    assertThat(e1.getMessage(), containsString("dimension 0"));

    // For 2D
    DoubleRangeField fieldType2D = createFieldType(2);
    SolrException e2 =
        expectThrows(
            SolrException.class,
            () -> fieldType2D.parseRangeValue("[3.0,2.0 TO 1.0,4.0]")); // First dimension invalid
    assertThat(e2.getMessage(), containsString("Min value must be <= max value"));
    assertThat(e2.getMessage(), containsString("dimension 0"));
  }

  public void testFieldCreation1D() {
    DoubleRangeField fieldType = createFieldType(1);
    SchemaField schemaField = createSchemaField(fieldType, "double_range");

    IndexableField field = fieldType.createField(schemaField, "[1.0 TO 2.0]");
    assertNotNull(field);
    assertTrue(field instanceof DoubleRange);
    assertEquals("double_range", field.name());
  }

  public void testFieldCreation2D() {
    DoubleRangeField fieldType = createFieldType(2);
    SchemaField schemaField = createSchemaField(fieldType, "double_range_2d");

    IndexableField field = fieldType.createField(schemaField, "[0.0,0.0 TO 10.0,10.0]");
    assertNotNull(field);
    assertTrue(field instanceof DoubleRange);
    assertEquals("double_range_2d", field.name());
  }

  public void testStoredField() {
    DoubleRangeField fieldType = createFieldType(1);
    SchemaField schemaField = createSchemaField(fieldType, "double_range");

    String value = "[1.0 TO 2.0]";
    IndexableField storedField = fieldType.getStoredField(schemaField, value);
    assertNotNull(storedField);
    assertEquals("double_range", storedField.name());
    assertEquals(value, storedField.stringValue());
  }

  public void testToInternal() {
    DoubleRangeField fieldType = createFieldType(1);

    // Valid value should pass through after validation
    String value = "[1.5 TO 3.14]";
    String internal = fieldType.toInternal(value);
    assertEquals(value, internal);

    // Invalid value should throw exception
    SolrException e = expectThrows(SolrException.class, () -> fieldType.toInternal("invalid"));
    assertThat(e.getMessage(), containsString("Invalid range format"));
  }

  public void testToNativeType() {
    DoubleRangeField fieldType = createFieldType(1);

    // String input
    Object nativeType = fieldType.toNativeType("[1.5 TO 3.14]");
    assertTrue(nativeType instanceof DoubleRangeField.RangeValue);
    DoubleRangeField.RangeValue range = (DoubleRangeField.RangeValue) nativeType;
    assertEquals(1.5, range.mins[0], 0.0);
    assertEquals(3.14, range.maxs[0], 0.0);

    // RangeValue input (should pass through)
    DoubleRangeField.RangeValue inputRange =
        new DoubleRangeField.RangeValue(new double[] {5.0}, new double[] {15.0});
    Object result = fieldType.toNativeType(inputRange);
    assertSame(inputRange, result);

    // Null input
    assertNull(fieldType.toNativeType(null));
  }

  public void testSortFieldThrowsException() {
    DoubleRangeField fieldType = createFieldType(1);
    SchemaField schemaField = createSchemaField(fieldType, "double_range");

    // Sorting should not be supported
    SolrException e =
        expectThrows(SolrException.class, () -> fieldType.getSortField(schemaField, true));
    assertThat(e.getMessage(), containsString("Cannot sort on DoubleRangeField"));
    assertThat(e.getMessage(), containsString("double_range"));
  }

  public void testUninversionType() {
    DoubleRangeField fieldType = createFieldType(1);
    SchemaField schemaField = createSchemaField(fieldType, "double_range");

    // Should return null (no field cache support)
    assertNull(fieldType.getUninversionType(schemaField));
  }

  public void testInvalidNumDimensions() {
    DoubleRangeField field = new DoubleRangeField();
    Map<String, String> args = new HashMap<>();
    IndexSchema schema = createMockSchema();

    // Test numDimensions = 0
    args.put("numDimensions", "0");
    SolrException e1 = expectThrows(SolrException.class, () -> field.init(schema, args));
    assertThat(e1.getMessage(), containsString("numDimensions must be between 1 and 4"));
    assertThat(e1.getMessage(), containsString("but was [0]"));

    // Test numDimensions = 5 (too high)
    args.put("numDimensions", "5");
    DoubleRangeField field2 = new DoubleRangeField();
    SolrException e2 = expectThrows(SolrException.class, () -> field2.init(schema, args));
    assertThat(e2.getMessage(), containsString("numDimensions must be between 1 and 4"));
    assertThat(e2.getMessage(), containsString("but was [5]"));

    // Test negative numDimensions
    args.put("numDimensions", "-1");
    DoubleRangeField field3 = new DoubleRangeField();
    SolrException e3 = expectThrows(SolrException.class, () -> field3.init(schema, args));
    assertThat(e3.getMessage(), containsString("numDimensions must be between 1 and 4"));
    assertThat(e3.getMessage(), containsString("but was [-1]"));
  }

  public void testRangeValueToString() {
    DoubleRangeField fieldType = createFieldType(2);
    DoubleRangeField.RangeValue range = fieldType.parseRangeValue("[1.0,2.0 TO 3.0,4.0]");

    String str = range.toString();
    assertEquals("[1.0,2.0 TO 3.0,4.0]", str);
  }

  public void testScientificNotation() {
    DoubleRangeField fieldType = createFieldType(1);

    // Integer mantissa with positive exponent
    DoubleRangeField.RangeValue range = fieldType.parseRangeValue("[123e4 TO 567e8]");
    assertEquals(123e4, range.mins[0], 0.0);
    assertEquals(567e8, range.maxs[0], 0.0);

    // Decimal mantissa with negative exponent
    range = fieldType.parseRangeValue("[-1.2e-4 TO 3.4e-2]");
    assertEquals(-1.2e-4, range.mins[0], 0.0);
    assertEquals(3.4e-2, range.maxs[0], 0.0);

    // Uppercase E
    range = fieldType.parseRangeValue("[1.5E3 TO 2.5E3]");
    assertEquals(1.5e3, range.mins[0], 0.0);
    assertEquals(2.5e3, range.maxs[0], 0.0);

    // Explicit positive exponent sign
    range = fieldType.parseRangeValue("[1.0e+2 TO 9.9e+2]");
    assertEquals(1.0e+2, range.mins[0], 0.0);
    assertEquals(9.9e+2, range.maxs[0], 0.0);

    // Negative mantissa with negative exponent
    range = fieldType.parseRangeValue("[-9.9E-9 TO -1.1E-9]");
    assertEquals(-9.9e-9, range.mins[0], 0.0);
    assertEquals(-1.1e-9, range.maxs[0], 0.0);

    // Multi-dimensional with scientific notation
    DoubleRangeField fieldType2D = createFieldType(2);
    range = fieldType2D.parseRangeValue("[1e2,2.0e1 TO 3e2,4.0e1]");
    assertEquals(1e2, range.mins[0], 0.0);
    assertEquals(2.0e1, range.mins[1], 0.0);
    assertEquals(3e2, range.maxs[0], 0.0);
    assertEquals(4.0e1, range.maxs[1], 0.0);

    // Single-bound scientific notation via toInternal
    String val = "[1.2e3 TO 4.5e3]";
    assertEquals(val, fieldType.toInternal(val));
  }

  public void testExtremeValues() {
    DoubleRangeField fieldType = createFieldType(1);

    // Test with very negative and very positive values expressible without scientific notation
    DoubleRangeField.RangeValue range = fieldType.parseRangeValue("[-9999999.0 TO 9999999.0]");
    assertEquals(-9999999.0, range.mins[0], 0.0);
    assertEquals(9999999.0, range.maxs[0], 0.0);

    // Test with small fractional values
    range = fieldType.parseRangeValue("[0.0001 TO 0.9999]");
    assertEquals(0.0001, range.mins[0], 0.0);
    assertEquals(0.9999, range.maxs[0], 0.0);

    // Test with values beyond float range
    range = fieldType.parseRangeValue("[1.0e100 TO 1.0e200]");
    assertEquals(1.0e100, range.mins[0], 0.0);
    assertEquals(1.0e200, range.maxs[0], 0.0);
  }

  private IndexSchema createMockSchema() {
    final var schema = mock(IndexSchema.class);
    when(schema.getVersion()).thenReturn(1.7f);
    return schema;
  }

  private DoubleRangeField createFieldType(int numDimensions) {
    DoubleRangeField field = new DoubleRangeField();
    Map<String, String> args = new HashMap<>();
    args.put("numDimensions", String.valueOf(numDimensions));

    field.init(createMockSchema(), args);

    return field;
  }

  private SchemaField createSchemaField(DoubleRangeField fieldType, String name) {
    final var fieldProperties =
        0b1 | 0b100; // INDEXED | STORED - constants cannot be accessed directly due to visibility.
    return new SchemaField(name, fieldType, fieldProperties, null);
  }
}
