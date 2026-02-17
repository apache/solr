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

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.document.IntRange;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;

/** Tests for {@link IntRangeField} */
public class IntRangeFieldTest extends SolrTestCase {

  @BeforeClass
  public static void ensureAssumptions() {
    assumeWorkingMockito();
  }

  public void test1DRangeParsing() {
    IntRangeField fieldType = createFieldType(1);

    // Valid 1D range
    IntRangeField.RangeValue range = fieldType.parseRangeValue("[10 TO 20]");
    assertEquals(1, range.getDimensions());
    assertEquals(10, range.mins[0]);
    assertEquals(20, range.maxs[0]);

    // With extra whitespace
    range = fieldType.parseRangeValue("[  10   TO   20  ]");
    assertEquals(10, range.mins[0]);
    assertEquals(20, range.maxs[0]);

    // Negative numbers
    range = fieldType.parseRangeValue("[-100 TO -50]");
    assertEquals(-100, range.mins[0]);
    assertEquals(-50, range.maxs[0]);

    // Point range (min == max)
    range = fieldType.parseRangeValue("[5 TO 5]");
    assertEquals(5, range.mins[0]);
    assertEquals(5, range.maxs[0]);
  }

  public void test2DRangeParsing() {
    IntRangeField fieldType = createFieldType(2);

    // Valid 2D range (bounding box)
    IntRangeField.RangeValue range = fieldType.parseRangeValue("[10,20 TO 30,40]");
    assertEquals(2, range.getDimensions());
    assertEquals(10, range.mins[0]);
    assertEquals(20, range.mins[1]);
    assertEquals(30, range.maxs[0]);
    assertEquals(40, range.maxs[1]);

    // With extra whitespace
    range = fieldType.parseRangeValue("[  10 , 20   TO   30 , 40  ]");
    assertEquals(10, range.mins[0]);
    assertEquals(20, range.mins[1]);
    assertEquals(30, range.maxs[0]);
    assertEquals(40, range.maxs[1]);
  }

  public void test3DRangeParsing() {
    IntRangeField fieldType = createFieldType(3);

    // Valid 3D range (bounding cube)
    IntRangeField.RangeValue range = fieldType.parseRangeValue("[10,20,30 TO 40,50,60]");
    assertEquals(3, range.getDimensions());
    assertEquals(10, range.mins[0]);
    assertEquals(20, range.mins[1]);
    assertEquals(30, range.mins[2]);
    assertEquals(40, range.maxs[0]);
    assertEquals(50, range.maxs[1]);
    assertEquals(60, range.maxs[2]);
  }

  public void test4DRangeParsing() {
    IntRangeField fieldType = createFieldType(4);

    // Valid 4D range (tesseract)
    IntRangeField.RangeValue range = fieldType.parseRangeValue("[10,20,30,40 TO 50,60,70,80]");
    assertEquals(4, range.getDimensions());
    assertEquals(10, range.mins[0]);
    assertEquals(20, range.mins[1]);
    assertEquals(30, range.mins[2]);
    assertEquals(40, range.mins[3]);
    assertEquals(50, range.maxs[0]);
    assertEquals(60, range.maxs[1]);
    assertEquals(70, range.maxs[2]);
    assertEquals(80, range.maxs[3]);
  }

  public void testInvalidRangeFormat() {
    IntRangeField fieldType = createFieldType(1);

    // Missing brackets
    expectThrows(SolrException.class, () -> fieldType.parseRangeValue("10 TO 20"));

    // Missing TO keyword
    expectThrows(SolrException.class, () -> fieldType.parseRangeValue("[10 20]"));

    // Empty value
    expectThrows(SolrException.class, () -> fieldType.parseRangeValue(""));

    // Null value
    expectThrows(SolrException.class, () -> fieldType.parseRangeValue(null));
  }

  public void testInvalidNumbers() {
    IntRangeField fieldType = createFieldType(1);

    // Non-numeric values
    expectThrows(SolrException.class, () -> fieldType.parseRangeValue("[abc TO def]"));

    // Partially numeric
    expectThrows(SolrException.class, () -> fieldType.parseRangeValue("[10 TO xyz]"));

    // Floating point (should fail for IntRange)
    expectThrows(SolrException.class, () -> fieldType.parseRangeValue("[10.5 TO 20.5]"));
  }

  public void testDimensionMismatch() {
    IntRangeField fieldType1D = createFieldType(1);
    IntRangeField fieldType2D = createFieldType(2);

    // 2D value on 1D field
    expectThrows(SolrException.class, () -> fieldType1D.parseRangeValue("[10,20 TO 30,40]"));

    // 1D value on 2D field
    expectThrows(SolrException.class, () -> fieldType2D.parseRangeValue("[10 TO 20]"));

    // Min/max dimension mismatch
    expectThrows(
        SolrException.class,
        () -> fieldType2D.parseRangeValue("[10,20 TO 30]")); // 2D mins, 1D maxs
  }

  public void testMinGreaterThanMax() {
    IntRangeField fieldType = createFieldType(1);

    // Min > max should fail
    expectThrows(SolrException.class, () -> fieldType.parseRangeValue("[20 TO 10]"));

    // For 2D
    IntRangeField fieldType2D = createFieldType(2);
    expectThrows(
        SolrException.class,
        () -> fieldType2D.parseRangeValue("[30,20 TO 10,40]")); // First dimension invalid
  }

  public void testFieldCreation1D() {
    IntRangeField fieldType = createFieldType(1);
    SchemaField schemaField = createSchemaField(fieldType, "price_range");

    IndexableField field = fieldType.createField(schemaField, "[100 TO 200]");
    assertNotNull(field);
    assertTrue(field instanceof IntRange);
    assertEquals("price_range", field.name());
  }

  public void testFieldCreation2D() {
    IntRangeField fieldType = createFieldType(2);
    SchemaField schemaField = createSchemaField(fieldType, "bbox");

    IndexableField field = fieldType.createField(schemaField, "[0,0 TO 10,10]");
    assertNotNull(field);
    assertTrue(field instanceof IntRange);
    assertEquals("bbox", field.name());
  }

  public void testStoredField() {
    IntRangeField fieldType = createFieldType(1);
    SchemaField schemaField = createSchemaField(fieldType, "price_range");

    String value = "[100 TO 200]";
    IndexableField storedField = fieldType.getStoredField(schemaField, value);
    assertNotNull(storedField);
    assertEquals("price_range", storedField.name());
    assertEquals(value, storedField.stringValue());
  }

  public void testToInternal() {
    IntRangeField fieldType = createFieldType(1);

    // Valid value should pass through after validation
    String value = "[10 TO 20]";
    String internal = fieldType.toInternal(value);
    assertEquals(value, internal);

    // Invalid value should throw exception
    expectThrows(SolrException.class, () -> fieldType.toInternal("invalid"));
  }

  public void testToNativeType() {
    IntRangeField fieldType = createFieldType(1);

    // String input
    Object nativeType = fieldType.toNativeType("[10 TO 20]");
    assertTrue(nativeType instanceof IntRangeField.RangeValue);
    IntRangeField.RangeValue range = (IntRangeField.RangeValue) nativeType;
    assertEquals(10, range.mins[0]);
    assertEquals(20, range.maxs[0]);

    // RangeValue input (should pass through)
    IntRangeField.RangeValue inputRange =
        new IntRangeField.RangeValue(new int[] {5}, new int[] {15});
    Object result = fieldType.toNativeType(inputRange);
    assertSame(inputRange, result);

    // Null input
    assertNull(fieldType.toNativeType(null));
  }

  public void testSortFieldThrowsException() {
    IntRangeField fieldType = createFieldType(1);
    SchemaField schemaField = createSchemaField(fieldType, "price_range");

    // Sorting should not be supported
    expectThrows(SolrException.class, () -> fieldType.getSortField(schemaField, true));
  }

  public void testUninversionType() {
    IntRangeField fieldType = createFieldType(1);
    SchemaField schemaField = createSchemaField(fieldType, "price_range");

    // Should return null (no field cache support)
    assertNull(fieldType.getUninversionType(schemaField));
  }

  public void testDocValuesNotSupported() {
    IndexSchema schema = createMockSchema();
    IntRangeField field = new IntRangeField();

    Map<String, String> args = new HashMap<>();
    args.put("numDimensions", "1");
    args.put("docValues", "true");

    // Should throw exception when docValues is enabled
    expectThrows(SolrException.class, () -> field.setArgs(schema, args));
  }

  public void testInvalidNumDimensions() {
    IntRangeField field = new IntRangeField();
    Map<String, String> args = new HashMap<>();
    IndexSchema schema = createMockSchema();

    // Test numDimensions = 0
    args.put("numDimensions", "0");
    expectThrows(SolrException.class, () -> field.init(schema, args));

    // Test numDimensions = 5 (too high)
    args.put("numDimensions", "5");
    IntRangeField field2 = new IntRangeField();
    expectThrows(SolrException.class, () -> field2.init(schema, args));

    // Test negative numDimensions
    args.put("numDimensions", "-1");
    IntRangeField field3 = new IntRangeField();
    expectThrows(SolrException.class, () -> field3.init(schema, args));
  }

  public void testRangeValueToString() {
    IntRangeField fieldType = createFieldType(2);
    IntRangeField.RangeValue range = fieldType.parseRangeValue("[10,20 TO 30,40]");

    String str = range.toString();
    assertEquals("[10,20 TO 30,40]", str);
  }

  public void testExtremeValues() {
    IntRangeField fieldType = createFieldType(1);

    // Test with Integer.MIN_VALUE and Integer.MAX_VALUE
    IntRangeField.RangeValue range =
        fieldType.parseRangeValue("[" + Integer.MIN_VALUE + " TO " + Integer.MAX_VALUE + "]");
    assertEquals(Integer.MIN_VALUE, range.mins[0]);
    assertEquals(Integer.MAX_VALUE, range.maxs[0]);
  }

  private IndexSchema createMockSchema() {
    final var schema = mock(IndexSchema.class);
    when(schema.getVersion()).thenReturn(1.7f);
    return schema;
  }

  private IntRangeField createFieldType(int numDimensions) {
    IntRangeField field = new IntRangeField();
    Map<String, String> args = new HashMap<>();
    args.put("numDimensions", String.valueOf(numDimensions));

    field.init(createMockSchema(), args);

    return field;
  }

  private SchemaField createSchemaField(IntRangeField fieldType, String name) {
    return new SchemaField(name, fieldType, SchemaField.INDEXED | SchemaField.STORED, null);
  }
}
