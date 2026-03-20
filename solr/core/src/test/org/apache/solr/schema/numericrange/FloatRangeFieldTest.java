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
import org.apache.lucene.document.FloatRange;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;

/** Tests for {@link FloatRangeField} */
public class FloatRangeFieldTest extends SolrTestCase {

  @BeforeClass
  public static void ensureAssumptions() {
    assumeWorkingMockito();
  }

  public void test1DRangeParsing() {
    FloatRangeField fieldType = createFieldType(1);

    // Valid 1D range with floating-point values
    FloatRangeField.RangeValue range = fieldType.parseRangeValue("[1.5 TO 3.14]");
    assertEquals(1, range.getDimensions());
    assertEquals(1.5f, range.mins[0], 0.0f);
    assertEquals(3.14f, range.maxs[0], 0.0f);

    // Integer values are accepted and parsed as floats
    range = fieldType.parseRangeValue("[10 TO 20]");
    assertEquals(10.0f, range.mins[0], 0.0f);
    assertEquals(20.0f, range.maxs[0], 0.0f);

    // With extra whitespace
    range = fieldType.parseRangeValue("[  1.5   TO   3.14  ]");
    assertEquals(1.5f, range.mins[0], 0.0f);
    assertEquals(3.14f, range.maxs[0], 0.0f);

    // Negative numbers
    range = fieldType.parseRangeValue("[-3.5 TO -1.0]");
    assertEquals(-3.5f, range.mins[0], 0.0f);
    assertEquals(-1.0f, range.maxs[0], 0.0f);

    // Point range (min == max)
    range = fieldType.parseRangeValue("[5.0 TO 5.0]");
    assertEquals(5.0f, range.mins[0], 0.0f);
    assertEquals(5.0f, range.maxs[0], 0.0f);
  }

  public void test2DRangeParsing() {
    FloatRangeField fieldType = createFieldType(2);

    // Valid 2D range (bounding box)
    FloatRangeField.RangeValue range = fieldType.parseRangeValue("[1.0,2.0 TO 3.0,4.0]");
    assertEquals(2, range.getDimensions());
    assertEquals(1.0f, range.mins[0], 0.0f);
    assertEquals(2.0f, range.mins[1], 0.0f);
    assertEquals(3.0f, range.maxs[0], 0.0f);
    assertEquals(4.0f, range.maxs[1], 0.0f);

    // With extra whitespace
    range = fieldType.parseRangeValue("[  1.0 , 2.0   TO   3.0 , 4.0  ]");
    assertEquals(1.0f, range.mins[0], 0.0f);
    assertEquals(2.0f, range.mins[1], 0.0f);
    assertEquals(3.0f, range.maxs[0], 0.0f);
    assertEquals(4.0f, range.maxs[1], 0.0f);
  }

  public void test3DRangeParsing() {
    FloatRangeField fieldType = createFieldType(3);

    // Valid 3D range (bounding cube)
    FloatRangeField.RangeValue range = fieldType.parseRangeValue("[1.0,2.0,3.0 TO 4.0,5.0,6.0]");
    assertEquals(3, range.getDimensions());
    assertEquals(1.0f, range.mins[0], 0.0f);
    assertEquals(2.0f, range.mins[1], 0.0f);
    assertEquals(3.0f, range.mins[2], 0.0f);
    assertEquals(4.0f, range.maxs[0], 0.0f);
    assertEquals(5.0f, range.maxs[1], 0.0f);
    assertEquals(6.0f, range.maxs[2], 0.0f);
  }

  public void test4DRangeParsing() {
    FloatRangeField fieldType = createFieldType(4);

    // Valid 4D range (tesseract)
    FloatRangeField.RangeValue range =
        fieldType.parseRangeValue("[1.0,2.0,3.0,4.0 TO 5.0,6.0,7.0,8.0]");
    assertEquals(4, range.getDimensions());
    assertEquals(1.0f, range.mins[0], 0.0f);
    assertEquals(2.0f, range.mins[1], 0.0f);
    assertEquals(3.0f, range.mins[2], 0.0f);
    assertEquals(4.0f, range.mins[3], 0.0f);
    assertEquals(5.0f, range.maxs[0], 0.0f);
    assertEquals(6.0f, range.maxs[1], 0.0f);
    assertEquals(7.0f, range.maxs[2], 0.0f);
    assertEquals(8.0f, range.maxs[3], 0.0f);
  }

  public void testInvalidRangeFormat() {
    FloatRangeField fieldType = createFieldType(1);

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
    FloatRangeField fieldType = createFieldType(1);

    // Non-numeric values
    SolrException e1 =
        expectThrows(SolrException.class, () -> fieldType.parseRangeValue("[abc TO def]"));
    assertThat(e1.getMessage(), containsString("Invalid range"));
    assertThat(e1.getMessage(), containsString("where min and max values are floats"));

    // Partially numeric
    SolrException e2 =
        expectThrows(SolrException.class, () -> fieldType.parseRangeValue("[1.5 TO xyz]"));
    assertThat(e2.getMessage(), containsString("Invalid range"));
    assertThat(e2.getMessage(), containsString("where min and max values are floats"));
  }

  public void testDimensionMismatch() {
    FloatRangeField fieldType1D = createFieldType(1);
    FloatRangeField fieldType2D = createFieldType(2);

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
    FloatRangeField fieldType = createFieldType(1);

    // Min > max should fail
    SolrException e1 =
        expectThrows(SolrException.class, () -> fieldType.parseRangeValue("[3.14 TO 1.5]"));
    assertThat(e1.getMessage(), containsString("Min value must be <= max value"));
    assertThat(e1.getMessage(), containsString("dimension 0"));

    // For 2D
    FloatRangeField fieldType2D = createFieldType(2);
    SolrException e2 =
        expectThrows(
            SolrException.class,
            () -> fieldType2D.parseRangeValue("[3.0,2.0 TO 1.0,4.0]")); // First dimension invalid
    assertThat(e2.getMessage(), containsString("Min value must be <= max value"));
    assertThat(e2.getMessage(), containsString("dimension 0"));
  }

  public void testFieldCreation1D() {
    FloatRangeField fieldType = createFieldType(1);
    SchemaField schemaField = createSchemaField(fieldType, "float_range");

    IndexableField field = fieldType.createField(schemaField, "[1.0 TO 2.0]");
    assertNotNull(field);
    assertTrue(field instanceof FloatRange);
    assertEquals("float_range", field.name());
  }

  public void testFieldCreation2D() {
    FloatRangeField fieldType = createFieldType(2);
    SchemaField schemaField = createSchemaField(fieldType, "float_range_2d");

    IndexableField field = fieldType.createField(schemaField, "[0.0,0.0 TO 10.0,10.0]");
    assertNotNull(field);
    assertTrue(field instanceof FloatRange);
    assertEquals("float_range_2d", field.name());
  }

  public void testStoredField() {
    FloatRangeField fieldType = createFieldType(1);
    SchemaField schemaField = createSchemaField(fieldType, "float_range");

    String value = "[1.0 TO 2.0]";
    IndexableField storedField = fieldType.getStoredField(schemaField, value);
    assertNotNull(storedField);
    assertEquals("float_range", storedField.name());
    assertEquals(value, storedField.stringValue());
  }

  public void testToInternal() {
    FloatRangeField fieldType = createFieldType(1);

    // Valid value should pass through after validation
    String value = "[1.5 TO 3.14]";
    String internal = fieldType.toInternal(value);
    assertEquals(value, internal);

    // Invalid value should throw exception
    SolrException e = expectThrows(SolrException.class, () -> fieldType.toInternal("invalid"));
    assertThat(e.getMessage(), containsString("Invalid range format"));
  }

  public void testToNativeType() {
    FloatRangeField fieldType = createFieldType(1);

    // String input
    Object nativeType = fieldType.toNativeType("[1.5 TO 3.14]");
    assertTrue(nativeType instanceof FloatRangeField.RangeValue);
    FloatRangeField.RangeValue range = (FloatRangeField.RangeValue) nativeType;
    assertEquals(1.5f, range.mins[0], 0.0f);
    assertEquals(3.14f, range.maxs[0], 0.0f);

    // RangeValue input (should pass through)
    FloatRangeField.RangeValue inputRange =
        new FloatRangeField.RangeValue(new float[] {5.0f}, new float[] {15.0f});
    Object result = fieldType.toNativeType(inputRange);
    assertSame(inputRange, result);

    // Null input
    assertNull(fieldType.toNativeType(null));
  }

  public void testSortFieldThrowsException() {
    FloatRangeField fieldType = createFieldType(1);
    SchemaField schemaField = createSchemaField(fieldType, "float_range");

    // Sorting should not be supported
    SolrException e =
        expectThrows(SolrException.class, () -> fieldType.getSortField(schemaField, true));
    assertThat(e.getMessage(), containsString("Cannot sort on FloatRangeField"));
    assertThat(e.getMessage(), containsString("float_range"));
  }

  public void testUninversionType() {
    FloatRangeField fieldType = createFieldType(1);
    SchemaField schemaField = createSchemaField(fieldType, "float_range");

    // Should return null (no field cache support)
    assertNull(fieldType.getUninversionType(schemaField));
  }

  public void testInvalidNumDimensions() {
    FloatRangeField field = new FloatRangeField();
    Map<String, String> args = new HashMap<>();
    IndexSchema schema = createMockSchema();

    // Test numDimensions = 0
    args.put("numDimensions", "0");
    SolrException e1 = expectThrows(SolrException.class, () -> field.init(schema, args));
    assertThat(e1.getMessage(), containsString("numDimensions must be between 1 and 4"));
    assertThat(e1.getMessage(), containsString("but was [0]"));

    // Test numDimensions = 5 (too high)
    args.put("numDimensions", "5");
    FloatRangeField field2 = new FloatRangeField();
    SolrException e2 = expectThrows(SolrException.class, () -> field2.init(schema, args));
    assertThat(e2.getMessage(), containsString("numDimensions must be between 1 and 4"));
    assertThat(e2.getMessage(), containsString("but was [5]"));

    // Test negative numDimensions
    args.put("numDimensions", "-1");
    FloatRangeField field3 = new FloatRangeField();
    SolrException e3 = expectThrows(SolrException.class, () -> field3.init(schema, args));
    assertThat(e3.getMessage(), containsString("numDimensions must be between 1 and 4"));
    assertThat(e3.getMessage(), containsString("but was [-1]"));
  }

  public void testRangeValueToString() {
    FloatRangeField fieldType = createFieldType(2);
    FloatRangeField.RangeValue range = fieldType.parseRangeValue("[1.0,2.0 TO 3.0,4.0]");

    String str = range.toString();
    assertEquals("[1.0,2.0 TO 3.0,4.0]", str);
  }

  public void testExtremeValues() {
    FloatRangeField fieldType = createFieldType(1);

    // Test with very negative and very positive values expressible without scientific notation
    FloatRangeField.RangeValue range = fieldType.parseRangeValue("[-9999999.0 TO 9999999.0]");
    assertEquals(-9999999.0f, range.mins[0], 0.0f);
    assertEquals(9999999.0f, range.maxs[0], 0.0f);

    // Test with small fractional values
    range = fieldType.parseRangeValue("[0.0001 TO 0.9999]");
    assertEquals(0.0001f, range.mins[0], 0.0f);
    assertEquals(0.9999f, range.maxs[0], 0.0f);
  }

  private IndexSchema createMockSchema() {
    final var schema = mock(IndexSchema.class);
    when(schema.getVersion()).thenReturn(1.7f);
    return schema;
  }

  private FloatRangeField createFieldType(int numDimensions) {
    FloatRangeField field = new FloatRangeField();
    Map<String, String> args = new HashMap<>();
    args.put("numDimensions", String.valueOf(numDimensions));

    field.init(createMockSchema(), args);

    return field;
  }

  private SchemaField createSchemaField(FloatRangeField fieldType, String name) {
    final var fieldProperties =
        0b1 | 0b100; // INDEXED | STORED - constants cannot be accessed directly due to visibility.
    return new SchemaField(name, fieldType, fieldProperties, null);
  }
}
