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
package org.apache.solr.analytics.value;

import java.util.Iterator;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.value.AnalyticsValueStream.ExpressionType;
import org.apache.solr.analytics.value.FillableTestValue.TestFloatValue;
import org.apache.solr.analytics.value.constant.ConstantFloatValue;
import org.junit.Test;

public class CastingFloatValueTest extends SolrTestCaseJ4 {

  @Test
  public void doubleCastingTest() {
    TestFloatValue val = new TestFloatValue();

    val.setValue(20F).setExists(true);
    assertEquals(20.0, ((DoubleValue) val).getDouble(), .00001);
    assertTrue(((DoubleValue) val).exists());

    val.setValue(1234F).setExists(true);
    assertEquals(1234.0, ((DoubleValue) val).getDouble(), .00001);
    assertTrue(((DoubleValue) val).exists());
  }

  @Test
  public void stringCastingTest() {
    TestFloatValue val = new TestFloatValue();

    val.setValue(20F).setExists(true);
    assertEquals("20.0", ((StringValue) val).getString());
    assertTrue(((StringValue) val).exists());

    val.setValue(1234F).setExists(true);
    assertEquals("1234.0", ((StringValue) val).getString());
    assertTrue(((StringValue) val).exists());
  }

  @Test
  public void objectCastingTest() {
    TestFloatValue val = new TestFloatValue();

    val.setValue(20F).setExists(true);
    assertEquals(20F, ((AnalyticsValue) val).getObject());
    assertTrue(((AnalyticsValue) val).exists());

    val.setValue(1234F).setExists(true);
    assertEquals(1234F, ((AnalyticsValue) val).getObject());
    assertTrue(((AnalyticsValue) val).exists());
  }

  @Test
  public void floatStreamCastingTest() {
    TestFloatValue val = new TestFloatValue();

    // No values
    val.setExists(false);
    ((FloatValueStream) val)
        .streamFloats(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue(20F).setExists(true);
    Iterator<Float> values = List.of(20F).iterator();
    ((FloatValueStream) val)
        .streamFloats(
            value -> {
              assertTrue(values.hasNext());
              assertEquals(values.next(), value, .00001);
            });
    assertFalse(values.hasNext());
  }

  @Test
  public void doubleStreamCastingTest() {
    TestFloatValue val = new TestFloatValue();

    // No values
    val.setExists(false);
    ((DoubleValueStream) val)
        .streamDoubles(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue(20F).setExists(true);
    Iterator<Double> values = List.of(20.0).iterator();
    ((DoubleValueStream) val)
        .streamDoubles(
            value -> {
              assertTrue(values.hasNext());
              assertEquals(values.next(), value, .00001);
            });
    assertFalse(values.hasNext());
  }

  @Test
  public void stringStreamCastingTest() {
    TestFloatValue val = new TestFloatValue();

    // No values
    val.setExists(false);
    ((StringValueStream) val)
        .streamStrings(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue(20F).setExists(true);
    Iterator<String> values = List.of("20.0").iterator();
    ((StringValueStream) val)
        .streamStrings(
            value -> {
              assertTrue(values.hasNext());
              assertEquals(values.next(), value);
            });
    assertFalse(values.hasNext());
  }

  @Test
  public void objectStreamCastingTest() {
    TestFloatValue val = new TestFloatValue();

    // No values
    val.setExists(false);
    ((AnalyticsValueStream) val)
        .streamObjects(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue(20F).setExists(true);
    Iterator<Object> values = List.<Object>of(20F).iterator();
    ((AnalyticsValueStream) val)
        .streamObjects(
            value -> {
              assertTrue(values.hasNext());
              assertEquals(values.next(), value);
            });
    assertFalse(values.hasNext());
  }

  @Test
  public void constantConversionTest() {
    TestFloatValue val = new TestFloatValue(ExpressionType.CONST);
    val.setValue(12354.234F).setExists(true);
    AnalyticsValueStream conv = val.convertToConstant();
    assertTrue(conv instanceof ConstantFloatValue);
    assertEquals(12354.234F, ((ConstantFloatValue) conv).getFloat(), .0000001);

    val = new TestFloatValue(ExpressionType.FIELD);
    val.setValue(12354.234F).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestFloatValue(ExpressionType.UNREDUCED_MAPPING);
    val.setValue(12354.234F).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestFloatValue(ExpressionType.REDUCTION);
    val.setValue(12354.234F).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestFloatValue(ExpressionType.REDUCED_MAPPING);
    val.setValue(12354.234F).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);
  }
}
