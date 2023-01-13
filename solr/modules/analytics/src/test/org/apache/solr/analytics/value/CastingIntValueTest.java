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
import org.apache.solr.analytics.value.FillableTestValue.TestIntValue;
import org.apache.solr.analytics.value.constant.ConstantIntValue;
import org.junit.Test;

public class CastingIntValueTest extends SolrTestCaseJ4 {

  @Test
  public void longCastingTest() {
    TestIntValue val = new TestIntValue();

    val.setValue(20).setExists(true);
    assertEquals(20L, ((LongValue) val).getLong());
    assertTrue(((LongValue) val).exists());

    val.setValue(1234).setExists(true);
    assertEquals(1234L, ((LongValue) val).getLong());
    assertTrue(((LongValue) val).exists());
  }

  @Test
  public void floatCastingTest() {
    TestIntValue val = new TestIntValue();

    val.setValue(20).setExists(true);
    assertEquals(20F, ((FloatValue) val).getFloat(), .00001);
    assertTrue(((FloatValue) val).exists());

    val.setValue(1234).setExists(true);
    assertEquals(1234F, ((FloatValue) val).getFloat(), .00001);
    assertTrue(((FloatValue) val).exists());
  }

  @Test
  public void doubleCastingTest() {
    TestIntValue val = new TestIntValue();

    val.setValue(20).setExists(true);
    assertEquals(20.0, ((DoubleValue) val).getDouble(), .00001);
    assertTrue(((DoubleValue) val).exists());

    val.setValue(1234).setExists(true);
    assertEquals(1234.0, ((DoubleValue) val).getDouble(), .00001);
    assertTrue(((DoubleValue) val).exists());
  }

  @Test
  public void stringCastingTest() {
    TestIntValue val = new TestIntValue();

    val.setValue(20).setExists(true);
    assertEquals("20", ((StringValue) val).getString());
    assertTrue(((StringValue) val).exists());

    val.setValue(1234).setExists(true);
    assertEquals("1234", ((StringValue) val).getString());
    assertTrue(((StringValue) val).exists());
  }

  @Test
  public void objectCastingTest() {
    TestIntValue val = new TestIntValue();

    val.setValue(20).setExists(true);
    assertEquals(20, ((AnalyticsValue) val).getObject());
    assertTrue(((AnalyticsValue) val).exists());

    val.setValue(1234).setExists(true);
    assertEquals(1234, ((AnalyticsValue) val).getObject());
    assertTrue(((AnalyticsValue) val).exists());
  }

  @Test
  public void intStreamCastingTest() {
    TestIntValue val = new TestIntValue();

    // No values
    val.setExists(false);
    ((IntValueStream) val)
        .streamInts(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue(20).setExists(true);
    Iterator<Integer> values = List.of(20).iterator();
    ((IntValueStream) val)
        .streamInts(
            value -> {
              assertTrue(values.hasNext());
              assertEquals(values.next().intValue(), value);
            });
    assertFalse(values.hasNext());
  }

  @Test
  public void longStreamCastingTest() {
    TestIntValue val = new TestIntValue();

    // No values
    val.setExists(false);
    ((LongValueStream) val)
        .streamLongs(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue(20).setExists(true);
    Iterator<Long> values = List.of(20L).iterator();
    ((LongValueStream) val)
        .streamLongs(
            value -> {
              assertTrue(values.hasNext());
              assertEquals(values.next().longValue(), value);
            });
    assertFalse(values.hasNext());
  }

  @Test
  public void floatStreamCastingTest() {
    TestIntValue val = new TestIntValue();

    // No values
    val.setExists(false);
    ((FloatValueStream) val)
        .streamFloats(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue(20).setExists(true);
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
    TestIntValue val = new TestIntValue();

    // No values
    val.setExists(false);
    ((DoubleValueStream) val)
        .streamDoubles(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue(20).setExists(true);
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
    TestIntValue val = new TestIntValue();

    // No values
    val.setExists(false);
    ((StringValueStream) val)
        .streamStrings(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue(20).setExists(true);
    Iterator<String> values = List.of("20").iterator();
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
    TestIntValue val = new TestIntValue();

    // No values
    val.setExists(false);
    ((AnalyticsValueStream) val)
        .streamObjects(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue(20).setExists(true);
    Iterator<Object> values = List.<Object>of(20).iterator();
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
    TestIntValue val = new TestIntValue(ExpressionType.CONST);
    val.setValue(1234).setExists(true);
    AnalyticsValueStream conv = val.convertToConstant();
    assertTrue(conv instanceof ConstantIntValue);
    assertEquals(1234, ((ConstantIntValue) conv).getInt());

    val = new TestIntValue(ExpressionType.FIELD);
    val.setValue(1234).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestIntValue(ExpressionType.UNREDUCED_MAPPING);
    val.setValue(1234).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestIntValue(ExpressionType.REDUCTION);
    val.setValue(1234).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestIntValue(ExpressionType.REDUCED_MAPPING);
    val.setValue(1234).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);
  }
}
