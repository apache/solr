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

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.value.AnalyticsValueStream.ExpressionType;
import org.apache.solr.analytics.value.FillableTestValue.TestDateValue;
import org.apache.solr.analytics.value.constant.ConstantDateValue;
import org.junit.Test;

public class CastingDateValueTest extends SolrTestCaseJ4 {

  @Test
  public void dateCastingTest() throws DateTimeParseException {
    Date date = Date.from(Instant.parse("1800-01-01T10:30:15Z"));
    TestDateValue val = new TestDateValue();

    val.setValue("1800-01-01T10:30:15Z").setExists(true);
    assertEquals(date.toInstant(), ((DateValue) val).getDate().toInstant());
    assertTrue(((DateValue) val).exists());
  }

  @Test
  public void stringCastingTest() {
    TestDateValue val = new TestDateValue();

    val.setValue("1800-01-01T10:30:15Z").setExists(true);
    assertEquals("1800-01-01T10:30:15Z", ((StringValue) val).getString());
    assertTrue(((StringValue) val).exists());
  }

  @Test
  public void objectCastingTest() throws DateTimeParseException {
    Date date = Date.from(Instant.parse("1800-01-01T10:30:15Z"));
    TestDateValue val = new TestDateValue();

    val.setValue("1800-01-01T10:30:15Z").setExists(true);
    assertEquals(date.toInstant(), ((Date) ((AnalyticsValue) val).getObject()).toInstant());
    assertTrue(((AnalyticsValue) val).exists());
  }

  @Test
  public void dateStreamCastingTest() throws DateTimeParseException {
    Date date = Date.from(Instant.parse("1800-01-01T10:30:15Z"));
    TestDateValue val = new TestDateValue();

    // No values
    val.setExists(false);
    ((DateValueStream) val)
        .streamDates(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue("1800-01-01T10:30:15Z").setExists(true);
    Iterator<Date> values = List.of(date).iterator();
    ((DateValueStream) val)
        .streamDates(
            value -> {
              assertTrue(values.hasNext());
              assertEquals(values.next().toInstant(), value.toInstant());
            });
    assertFalse(values.hasNext());
  }

  @Test
  public void stringStreamCastingTest() {
    TestDateValue val = new TestDateValue();

    // No values
    val.setExists(false);
    ((StringValueStream) val)
        .streamStrings(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue("1800-01-01T10:30:15Z").setExists(true);
    Iterator<String> values = List.of("1800-01-01T10:30:15Z").iterator();
    ((StringValueStream) val)
        .streamStrings(
            value -> {
              assertTrue(values.hasNext());
              assertEquals(values.next(), value);
            });
    assertFalse(values.hasNext());
  }

  @Test
  public void objectStreamCastingTest() throws DateTimeParseException {
    Date date = Date.from(Instant.parse("1800-01-01T10:30:15Z"));
    TestDateValue val = new TestDateValue();

    // No values
    val.setExists(false);
    ((AnalyticsValueStream) val)
        .streamObjects(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValue("1800-01-01T10:30:15Z").setExists(true);
    Iterator<Object> values = List.<Object>of(date).iterator();
    ((AnalyticsValueStream) val)
        .streamObjects(
            value -> {
              assertTrue(values.hasNext());
              assertEquals(values.next(), value);
            });
    assertFalse(values.hasNext());
  }

  @Test
  public void constantConversionTest() throws DateTimeParseException {
    Date date = Date.from(Instant.parse("1800-01-01T10:30:15Z"));

    TestDateValue val = new TestDateValue(ExpressionType.CONST);
    val.setValue("1800-01-01T10:30:15Z").setExists(true);
    AnalyticsValueStream conv = val.convertToConstant();
    assertTrue(conv instanceof ConstantDateValue);
    assertEquals(date.toInstant(), ((ConstantDateValue) conv).getDate().toInstant());

    val = new TestDateValue(ExpressionType.FIELD);
    val.setValue("1800-01-01T10:30:15Z").setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestDateValue(ExpressionType.UNREDUCED_MAPPING);
    val.setValue("1800-01-01T10:30:15Z").setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestDateValue(ExpressionType.REDUCTION);
    val.setValue("1800-01-01T10:30:15Z").setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);

    val = new TestDateValue(ExpressionType.REDUCED_MAPPING);
    val.setValue("1800-01-01T10:30:15Z").setExists(true);
    conv = val.convertToConstant();
    assertSame(val, conv);
  }
}
