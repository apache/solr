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
import org.apache.solr.analytics.value.FillableTestValue.TestBooleanValue;
import org.apache.solr.analytics.value.constant.ConstantBooleanValue;
import org.junit.Test;

public class CastingBooleanValueTest extends SolrTestCaseJ4 {

  @Test
  public void stringCastingTest() {
    TestBooleanValue val = new TestBooleanValue();

    val.setValue(false).setExists(true);
    assertEquals("false", ((StringValue) val).getString());
    assertTrue(((StringValue) val).exists());

    val.setValue(true).setExists(true);
    assertEquals("true", ((StringValue) val).getString());
    assertTrue(((StringValue) val).exists());
  }

  @Test
  public void objectCastingTest() {
    TestBooleanValue val = new TestBooleanValue();

    val.setValue(false).setExists(true);
    assertEquals(Boolean.FALSE, ((AnalyticsValue) val).getObject());
    assertTrue(((AnalyticsValue) val).exists());

    val.setValue(true).setExists(true);
    assertEquals(Boolean.TRUE, ((AnalyticsValue) val).getObject());
    assertTrue(((AnalyticsValue) val).exists());
  }

  @Test
  public void booleanStreamCastingTest() {
    TestBooleanValue val = new TestBooleanValue();

    // No values
    val.setExists(false);
    ((BooleanValueStream) val)
        .streamBooleans(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue(false).setExists(true);
    Iterator<Boolean> values = List.of(false).iterator();
    ((BooleanValueStream) val)
        .streamBooleans(
            value -> {
              assertTrue(values.hasNext());
              assertEquals(values.next(), value);
            });
    assertFalse(values.hasNext());
  }

  @Test
  public void stringStreamCastingTest() {
    TestBooleanValue val = new TestBooleanValue();

    // No values
    val.setExists(false);
    ((StringValueStream) val)
        .streamStrings(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue(false).setExists(true);
    Iterator<String> values = List.of("false").iterator();
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
    TestBooleanValue val = new TestBooleanValue();

    // No values
    val.setExists(false);
    ((AnalyticsValueStream) val)
        .streamObjects(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue(false).setExists(true);
    Iterator<Object> values = List.<Object>of(Boolean.FALSE).iterator();
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
    TestBooleanValue val = new TestBooleanValue(ExpressionType.CONST);
    val.setValue(true).setExists(true);
    AnalyticsValueStream conv = val.convertToConstant();
    assertTrue(conv instanceof ConstantBooleanValue);
    assertTrue(((ConstantBooleanValue) conv).getBoolean());

    val = new TestBooleanValue(ExpressionType.FIELD);
    val.setValue(true).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestBooleanValue(ExpressionType.UNREDUCED_MAPPING);
    val.setValue(true).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestBooleanValue(ExpressionType.REDUCTION);
    val.setValue(true).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestBooleanValue(ExpressionType.REDUCED_MAPPING);
    val.setValue(true).setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);
  }
}
