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
import org.apache.solr.analytics.value.FillableTestValue.TestLongValue;
import org.apache.solr.analytics.value.constant.ConstantLongValue;
import org.junit.Test;

public class CastingLongValueTest extends SolrTestCaseJ4 {

  @Test
  public void doubleCastingTest() {
    TestLongValue val = new TestLongValue();

    val.setValue(20L).setExists(true);
    assertEquals(20.0, ((DoubleValue) val).getDouble(), .00001);
    assertTrue(((DoubleValue) val).exists());

    val.setValue(1234L).setExists(true);
    assertEquals(1234.0, ((DoubleValue) val).getDouble(), .00001);
    assertTrue(((DoubleValue) val).exists());
  }

  @Test
  public void stringCastingTest() {
    TestLongValue val = new TestLongValue();

    val.setValue(20L).setExists(true);
    assertEquals("20", ((StringValue) val).getString());
    assertTrue(((StringValue) val).exists());

    val.setValue(1234L).setExists(true);
    assertEquals("1234", ((StringValue) val).getString());
    assertTrue(((StringValue) val).exists());
  }

  @Test
  public void objectCastingTest() {
    TestLongValue val = new TestLongValue();

    val.setValue(20L).setExists(true);
    assertEquals(20L, ((AnalyticsValue) val).getObject());
    assertTrue(((AnalyticsValue) val).exists());

    val.setValue(1234L).setExists(true);
    assertEquals(1234L, ((AnalyticsValue) val).getObject());
    assertTrue(((AnalyticsValue) val).exists());
  }

  @Test
  public void longStreamCastingTest() {
    TestLongValue val = new TestLongValue();

    // No values
    val.setExists(false);
    ((LongValueStream) val)
        .streamLongs(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue(20L).setExists(true);
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
  public void doubleStreamCastingTest() {
    TestLongValue val = new TestLongValue();

    // No values
    val.setExists(false);
    ((DoubleValueStream) val)
        .streamDoubles(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue(20L).setExists(true);
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
    TestLongValue val = new TestLongValue();

    // No values
    val.setExists(false);
    ((StringValueStream) val)
        .streamStrings(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue(20L).setExists(true);
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
    TestLongValue val = new TestLongValue();

    // No values
    val.setExists(false);
    ((AnalyticsValueStream) val)
        .streamObjects(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue(20L).setExists(true);
    Iterator<Object> values = List.<Object>of(20L).iterator();
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
    TestLongValue val = new TestLongValue(ExpressionType.CONST);
    val.setValue(12341L).setExists(true);
    AnalyticsValueStream conv = val.convertToConstant();
    assertTrue(conv instanceof ConstantLongValue);
    assertEquals(12341L, ((ConstantLongValue) conv).getLong());

    val = new TestLongValue(ExpressionType.FIELD);
    val.setValue(12341L).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestLongValue(ExpressionType.UNREDUCED_MAPPING);
    val.setValue(12341L).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestLongValue(ExpressionType.REDUCTION);
    val.setValue(12341L).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestLongValue(ExpressionType.REDUCED_MAPPING);
    val.setValue(12341L).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);
  }
}
