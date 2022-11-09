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

import java.util.Arrays;
import java.util.Iterator;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.value.FillableTestValue.TestFloatValueStream;
import org.junit.Test;

public class CastingFloatValueStreamTest extends SolrTestCaseJ4 {

  @Test
  public void doubleStreamCastingTest() {
    TestFloatValueStream val = new TestFloatValueStream();

    // No values
    val.setValues();
    ((DoubleValueStream) val)
        .streamDoubles(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValues(20F, -3.32F, 42.5F);
    Iterator<Double> values = Arrays.asList(20.0, -3.32, 42.5).iterator();
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
    TestFloatValueStream val = new TestFloatValueStream();

    // No values
    val.setValues();
    ((StringValueStream) val)
        .streamStrings(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValues(20F, -3.32F, 42.5F);
    Iterator<String> values = Arrays.asList("20.0", "-3.32", "42.5").iterator();
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
    TestFloatValueStream val = new TestFloatValueStream();

    // No values
    val.setValues();
    ((AnalyticsValueStream) val)
        .streamObjects(
            value -> {
              fail("There should be no values to stream");
            });

    // Multiple Values
    val.setValues(20F, -3.32F, 42.5F);
    Iterator<Object> values = Arrays.<Object>asList(20F, -3.32F, 42.5F).iterator();
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
    AnalyticsValueStream val = new TestFloatValueStream();
    assertSame(val, val.convertToConstant());
  }
}
