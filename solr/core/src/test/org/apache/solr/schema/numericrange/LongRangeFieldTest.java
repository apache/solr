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
import org.apache.lucene.document.LongRange;
import org.apache.lucene.index.IndexableField;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.junit.BeforeClass;

/** Tests for {@link LongRangeField} */
public class LongRangeFieldTest extends SolrTestCase {

  @BeforeClass
  public static void ensureAssumptions() {
    assumeWorkingMockito();
  }

  public void test1DRangeParsing() {
    LongRangeField fieldType = createFieldType(1);

    // Valid 1D range
    LongRangeField.RangeValue range = fieldType.parseRangeValue("[10 TO 20]");
    assertEquals(1, range.getDimensions());
    assertEquals(10L, range.mins[0]);
    assertEquals(20L, range.maxs[0]);

    // With extra whitespace
    range = fieldType.parseRangeValue("[  10   TO   20  ]");
    assertEquals(10L, range.mins[0]);
    assertEquals(20L, range.maxs[0]);

    // Negative numbers
    range = fieldType.parseRangeValue("[-100 TO -50]");
    assertEquals(-100L, range.mins[0]);
    assertEquals(-50L, range.maxs[0]);

    // Point range (min == max)
    range = fieldType.parseRangeValue("[5 TO 5]");
    assertEquals(5L, range.mins[0]);
    assertEquals(5L, range.maxs[0]);

    // Values outside int range
    range = fieldType.parseRangeValue("[3000000000 TO 4000000000]");
    assertEquals(3_000_000_000L, range.mins[0]);
    assertEquals(4_000_000_000L, range.maxs[0]);
  }

  public void test2DRangeParsing() {
    LongRangeField fieldType = createFieldType(2);

    // Valid 2D range (bounding box)
    LongRangeField.RangeValue range = fieldType.parseRangeValue("[10,20 TO 30,40]");
    assertEquals(2, range.getDimensions());
    assertEquals(10L, range.mins[0]);
    assertEquals(20L, range.mins[1]);
    assertEquals(30L, range.maxs[0]);
    assertEquals(40L, range.maxs[1]);

    // With extra whitespace
    range = fieldType.parseRangeValue("[  10 , 20   TO   30 , 40  ]");
    assertEquals(10L, range.mins[0]);
    assertEquals(20L, range.mins[1]);
    assertEquals(30L, range.maxs[0]);
    assertEquals(40L, range.maxs[1]);
  }

  public void test3DRangeParsing() {
    LongRangeField fieldType = createFieldType(3);

    // Valid 3D range (bounding cube)
    LongRangeField.RangeValue range = fieldType.parseRangeValue("[10,20,30 TO 40,50,60]");
    assertEquals(3, range.getDimensions());
    assertEquals(10L, range.mins[0]);
    assertEquals(20L, range.mins[1]);
    assertEquals(30L, range.mins[2]);
    assertEquals(40L, range.maxs[0]);
    assertEquals(50L, range.maxs[1]);
    assertEquals(60L, range.maxs[2]);
  }

  public void test4DRangeParsing() {
    LongRangeField fieldType = createFieldType(4);

    // Valid 4D range (tesseract)
    LongRangeField.RangeValue range = fieldType.parseRangeValue("[10,20,30,40 TO 50,60,70,80]");
    assertEquals(4, range.getDimensions());
    assertEquals(10L, range.mins[0]);
    assertEquals(20L, range.mins[1]);
    assertEquals(30L, range.mins[2]);
    assertEquals(40L, range.mins[3]);
    assertEquals(50L, range.maxs[0]);
    assertEquals(60L, range.maxs[1]);
    assertEquals(70L, range.maxs[2]);
    assertEquals(80L, range.maxs[3]);
  }

  public void testInvalidRangeFormat() {
    LongRangeField fieldType = createFieldType(1);

    // Missing brackets
    SolrException e1 =
        expectThrows(SolrException.class, () -> fieldType.parseRangeValue("10 TO 20"));
    assertThat(e1.getMessage(), containsString("Invalid range format"));
    assertThat(e1.getMessage(), containsString("Expected: [min1,min2,... TO max1,max2,...]"));

    // Missing TO keyword
    SolrException e2 =
        expectThrows(SolrException.class, () -> fieldType.parseRangeValue("[10 20]"));
    assertThat(e2.getMessage(), containsString("Invalid range format"));

    // Empty value
    SolrException e3 = expectThrows(SolrException.class, () -> fieldType.parseRangeValue(""));
    assertThat(e3.getMessage(), containsString("Range value cannot be null or empty"));

    // Null value
    SolrException e4 = expectThrows(SolrException.class, () -> fieldType.parseRangeValue(null));
    assertThat(e4.getMessage(), containsString("Range value cannot be null or empty"));
  }

  public void testInvalidNumbers() {
    LongRangeField fieldType = createFieldType(1);

    // Non-numeric values
    SolrException e1 =
        expectThrows(SolrException.class, () -> fieldType.parseRangeValue("[abc TO def]"));
    assertThat(e1.getMessage(), containsString("Invalid range"));
    assertThat(e1.getMessage(), containsString("where min and max values are longs"));

    // Partially numeric
    SolrException e2 =
        expectThrows(SolrException.class, () -> fieldType.parseRangeValue("[10 TO xyz]"));
    assertThat(e2.getMessage(), containsString("Invalid range"));
    assertThat(e2.getMessage(), containsString("where min and max values are longs"));

    // Floating point (should fail for LongRange)
    SolrException e3 =
        expectThrows(SolrException.class, () -> fieldType.parseRangeValue("[10.5 TO 20.5]"));
    assertThat(e3.getMessage(), containsString("Invalid range"));
    assertThat(e3.getMessage(), containsString("where min and max values are longs"));
  }

  public void testDimensionMismatch() {
    LongRangeField fieldType1D = createFieldType(1);
    LongRangeField fieldType2D = createFieldType(2);

    // 2D value on 1D field
    SolrException e1 =
        expectThrows(SolrException.class, () -> fieldType1D.parseRangeValue("[10,20 TO 30,40]"));
    assertThat(e1.getMessage(), containsString("Range dimensions"));
    assertThat(e1.getMessage(), containsString("do not match field type numDimensions"));

    // 1D value on 2D field
    SolrException e2 =
        expectThrows(SolrException.class, () -> fieldType2D.parseRangeValue("[10 TO 20]"));
    assertThat(e2.getMessage(), containsString("Range dimensions"));
    assertThat(e2.getMessage(), containsString("do not match field type numDimensions"));

    // Min/max dimension mismatch
    SolrException e3 =
        expectThrows(
            SolrException.class,
            () -> fieldType2D.parseRangeValue("[10,20 TO 30]")); // 2D mins, 1D maxs
    assertThat(e3.getMessage(), containsString("Min and max dimensions must match"));
  }

  public void testMinGreaterThanMax() {
    LongRangeField fieldType = createFieldType(1);

    // Min > max should fail
    SolrException e1 =
        expectThrows(SolrException.class, () -> fieldType.parseRangeValue("[20 TO 10]"));
    assertThat(e1.getMessage(), containsString("Min value must be <= max value"));
    assertThat(e1.getMessage(), containsString("dimension 0"));

    // For 2D
    LongRangeField fieldType2D = createFieldType(2);
    SolrException e2 =
        expectThrows(
            SolrException.class,
            () -> fieldType2D.parseRangeValue("[30,20 TO 10,40]")); // First dimension invalid
    assertThat(e2.getMessage(), containsString("Min value must be <= max value"));
    assertThat(e2.getMessage(), containsString("dimension 0"));
  }

  public void testFieldCreation1D() {
    LongRangeField fieldType = createFieldType(1);
    SchemaField schemaField = createSchemaField(fieldType, "long_range");

    IndexableField field = fieldType.createField(schemaField, "[100 TO 200]");
    assertNotNull(field);
    assertTrue(field instanceof LongRange);
    assertEquals("long_range", field.name());
  }

  public void testFieldCreation2D() {
    LongRangeField fieldType = createFieldType(2);
    SchemaField schemaField = createSchemaField(fieldType, "long_range_2d");

    IndexableField field = fieldType.createField(schemaField, "[0,0 TO 10,10]");
    assertNotNull(field);
    assertTrue(field instanceof LongRange);
    assertEquals("long_range_2d", field.name());
  }

  public void testStoredField() {
    LongRangeField fieldType = createFieldType(1);
    SchemaField schemaField = createSchemaField(fieldType, "long_range");

    String value = "[100 TO 200]";
    IndexableField storedField = fieldType.getStoredField(schemaField, value);
    assertNotNull(storedField);
    assertEquals("long_range", storedField.name());
    assertEquals(value, storedField.stringValue());
  }

  public void testToInternal() {
    LongRangeField fieldType = createFieldType(1);

    // Valid value should pass through after validation
    String value = "[10 TO 20]";
    String internal = fieldType.toInternal(value);
    assertEquals(value, internal);

    // Invalid value should throw exception
    SolrException e = expectThrows(SolrException.class, () -> fieldType.toInternal("invalid"));
    assertThat(e.getMessage(), containsString("Invalid range format"));
  }

  public void testToNativeType() {
    LongRangeField fieldType = createFieldType(1);

    // String input
    Object nativeType = fieldType.toNativeType("[10 TO 20]");
    assertTrue(nativeType instanceof LongRangeField.RangeValue);
    LongRangeField.RangeValue range = (LongRangeField.RangeValue) nativeType;
    assertEquals(10L, range.mins[0]);
    assertEquals(20L, range.maxs[0]);

    // RangeValue input (should pass through)
    LongRangeField.RangeValue inputRange =
        new LongRangeField.RangeValue(new long[] {5L}, new long[] {15L});
    Object result = fieldType.toNativeType(inputRange);
    assertSame(inputRange, result);

    // Null input
    assertNull(fieldType.toNativeType(null));
  }

  public void testSortFieldThrowsException() {
    LongRangeField fieldType = createFieldType(1);
    SchemaField schemaField = createSchemaField(fieldType, "long_range");

    // Sorting should not be supported
    SolrException e =
        expectThrows(SolrException.class, () -> fieldType.getSortField(schemaField, true));
    assertThat(e.getMessage(), containsString("Cannot sort on LongRangeField"));
    assertThat(e.getMessage(), containsString("long_range"));
  }

  public void testUninversionType() {
    LongRangeField fieldType = createFieldType(1);
    SchemaField schemaField = createSchemaField(fieldType, "long_range");

    // Should return null (no field cache support)
    assertNull(fieldType.getUninversionType(schemaField));
  }

  public void testInvalidNumDimensions() {
    LongRangeField field = new LongRangeField();
    Map<String, String> args = new HashMap<>();
    IndexSchema schema = createMockSchema();

    // Test numDimensions = 0
    args.put("numDimensions", "0");
    SolrException e1 = expectThrows(SolrException.class, () -> field.init(schema, args));
    assertThat(e1.getMessage(), containsString("numDimensions must be between 1 and 4"));
    assertThat(e1.getMessage(), containsString("but was [0]"));

    // Test numDimensions = 5 (too high)
    args.put("numDimensions", "5");
    LongRangeField field2 = new LongRangeField();
    SolrException e2 = expectThrows(SolrException.class, () -> field2.init(schema, args));
    assertThat(e2.getMessage(), containsString("numDimensions must be between 1 and 4"));
    assertThat(e2.getMessage(), containsString("but was [5]"));

    // Test negative numDimensions
    args.put("numDimensions", "-1");
    LongRangeField field3 = new LongRangeField();
    SolrException e3 = expectThrows(SolrException.class, () -> field3.init(schema, args));
    assertThat(e3.getMessage(), containsString("numDimensions must be between 1 and 4"));
    assertThat(e3.getMessage(), containsString("but was [-1]"));
  }

  public void testRangeValueToString() {
    LongRangeField fieldType = createFieldType(2);
    LongRangeField.RangeValue range = fieldType.parseRangeValue("[10,20 TO 30,40]");

    String str = range.toString();
    assertEquals("[10,20 TO 30,40]", str);
  }

  public void testExtremeValues() {
    LongRangeField fieldType = createFieldType(1);

    // Test with Long.MIN_VALUE and Long.MAX_VALUE
    LongRangeField.RangeValue range =
        fieldType.parseRangeValue("[" + Long.MIN_VALUE + " TO " + Long.MAX_VALUE + "]");
    assertEquals(Long.MIN_VALUE, range.mins[0]);
    assertEquals(Long.MAX_VALUE, range.maxs[0]);
  }

  private IndexSchema createMockSchema() {
    final var schema = mock(IndexSchema.class);
    when(schema.getVersion()).thenReturn(1.7f);
    return schema;
  }

  private LongRangeField createFieldType(int numDimensions) {
    LongRangeField field = new LongRangeField();
    Map<String, String> args = new HashMap<>();
    args.put("numDimensions", String.valueOf(numDimensions));

    field.init(createMockSchema(), args);

    return field;
  }

  private SchemaField createSchemaField(LongRangeField fieldType, String name) {
    final var fieldProperties =
        0b1 | 0b100; // INDEXED | STORED - constants cannot be accessed directly due to visibility.
    return new SchemaField(name, fieldType, fieldProperties, null);
  }
}
