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
package org.apache.solr.client.solrj.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.MapWriter.EntryWriter;
import org.apache.solr.common.params.StreamParams;
import org.junit.Test;

public class TupleTest extends SolrTestCase {

  @Test
  public void putAllSetsEOFMarker() {
    final Map<String, Object> fields = new HashMap<>();
    fields.put("field-one", new Object());
    fields.put("field-two", new Object());
    fields.put(StreamParams.EOF, true);

    final Tuple tuple = new Tuple();
    tuple.putAll(fields);

    assertTrue(tuple.EOF);
  }

  @Test
  public void putAllSetsEXCEPTIONMarker() {
    final Map<String, Object> fields = new HashMap<>();
    fields.put("field-one", new Object());
    fields.put("field-two", new Object());
    fields.put(StreamParams.EXCEPTION, "exception");

    final Tuple tuple = new Tuple();
    tuple.putAll(fields);

    assertTrue(tuple.EXCEPTION);
  }

  @Test
  public void copyTest() {
    final Map<String, Object> fields = new HashMap<>();
    fields.put("field-one", new Object());
    fields.put("field-two", new Object());
    fields.put(StreamParams.EXCEPTION, "exception");
    fields.put(StreamParams.EOF, true);
    final Tuple original = new Tuple();
    original.putAll(fields);
    original.setFieldNames(new ArrayList<>(Arrays.asList("field-one", "field-two")));
    original.setFieldLabels(
        new HashMap<>(
            Map.ofEntries(
                Map.entry("field-one", "field one"), Map.entry("field-two", "field two"))));

    final Tuple copy = new Tuple(original);

    assertEquals(original.getFields().entrySet().size(), copy.getFields().entrySet().size());
    assertEquals(original.getFieldNames().size(), copy.getFieldNames().size());
    assertEquals(
        original.getFieldLabels().entrySet().size(), copy.getFieldLabels().entrySet().size());
    assertEquals(original.EOF, copy.EOF);
    assertEquals(original.EXCEPTION, copy.EXCEPTION);
  }

  @Test
  public void cloneTest() {
    final Map<String, Object> fields = new HashMap<>();
    fields.put("field-one", new Object());
    fields.put("field-two", new Object());
    fields.put(StreamParams.EXCEPTION, "exception");
    fields.put(StreamParams.EOF, true);
    final Tuple original = new Tuple();
    original.putAll(fields);
    original.setFieldNames(new ArrayList<>(Arrays.asList("field-one", "field-two")));
    original.setFieldLabels(
        new HashMap<>(
            Map.ofEntries(
                Map.entry("field-one", "field one"), Map.entry("field-two", "field two"))));

    final Tuple clone = original.clone();

    assertEquals(original.getFields().entrySet().size(), clone.getFields().entrySet().size());
    assertEquals(original.getFieldNames().size(), clone.getFieldNames().size());
    assertEquals(
        original.getFieldLabels().entrySet().size(), clone.getFieldLabels().entrySet().size());
    assertEquals(original.EOF, clone.EOF);
    assertEquals(original.EXCEPTION, clone.EXCEPTION);
  }

  @Test
  public void mergeTest() {
    final Map<String, Object> commonFields = new HashMap<>();
    commonFields.put("field-one", new Object());
    commonFields.put("field-two", new Object());
    commonFields.put("field-three", new Object());
    commonFields.put(StreamParams.EXCEPTION, "exception");
    commonFields.put(StreamParams.EOF, true);

    final Tuple tupleOne = new Tuple();
    tupleOne.putAll(commonFields);
    tupleOne.setFieldNames(
        new ArrayList<>(Arrays.asList("field-one-name", "field-two-name", "field-three-name")));
    tupleOne.setFieldLabels(
        new HashMap<>(
            Map.ofEntries(
                Map.entry("field-one-name", "field-one"),
                Map.entry("field-two-name", "field-two"),
                Map.entry("field-three-name", "field-three"))));

    final Tuple tupleTwo = new Tuple();
    tupleTwo.putAll(commonFields);
    tupleTwo.put("field-four", new Object());
    tupleTwo.put("new-field-two", new Object());
    tupleTwo.setFieldNames(
        new ArrayList<>(Arrays.asList("field-one-name", "field-two-name", "field-four-name")));
    tupleTwo.setFieldLabels(
        new HashMap<>(
            Map.ofEntries(
                Map.entry("field-one-name", "field-one"),
                Map.entry("field-two-name", "new-field-two"),
                Map.entry("field-four-name", "field-four"))));

    tupleOne.merge(tupleTwo);

    assertEquals(7, tupleOne.getFields().size());
    assertEquals(4, tupleOne.getFieldNames().size()); // fieldNames should contain no duplicates
    assertEquals(4, tupleOne.getFieldLabels().size());
    assertEquals("new-field-two", tupleOne.getFieldLabels().get("field-two-name"));
    assertTrue(tupleOne.EOF);
    assertTrue(tupleOne.EXCEPTION);
  }

  @Test
  public void writeMapTest() throws IOException {
    final Map<String, Object> commonFields = new HashMap<>();
    commonFields.put("field a", "1");
    commonFields.put("field b", "2");
    commonFields.put("field c", "3");

    final Tuple tupleOne = new Tuple(commonFields);
    // label all fields
    tupleOne.setFieldLabels(
        new HashMap<>(
            Map.ofEntries(
                Map.entry("field-one", "field a"),
                Map.entry("field-two", "field b"),
                Map.entry("field-three", "field c"))));
    // then choose a subset for serialisation
    tupleOne.setFieldNames(new ArrayList<>(Arrays.asList("field-two", "field-three")));
    {
      final TupleEntryWriter writer = new TupleEntryWriter();
      tupleOne.writeMap(writer);
      assertEquals(2, writer.tuple.getFields().size());
      assertEquals("2", writer.tuple.get("field b")); // field-two
      assertEquals("3", writer.tuple.get("field c")); // field-three
    }

    final Tuple tupleTwo = new Tuple(commonFields);
    tupleTwo.put("field d", "4");
    // label most fields (3 of 4)
    tupleTwo.setFieldLabels(
        new HashMap<>(
            Map.ofEntries(
                Map.entry("field-one", "field b"),
                Map.entry("field-two", "field a"),
                Map.entry("field-four", "field d"))));
    // then choose a subset for serialisation
    tupleTwo.setFieldNames(new ArrayList<>(Arrays.asList("field-two", "field-four")));
    {
      final TupleEntryWriter writer = new TupleEntryWriter();
      tupleTwo.writeMap(writer);
      assertEquals(2, writer.tuple.getFields().size());
      assertEquals("1", writer.tuple.get("field a")); // field-two
      assertEquals("4", writer.tuple.get("field d")); // field-four
    }

    // clone and merge
    final Tuple tupleThree = tupleOne.clone();
    tupleThree.merge(tupleTwo);
    assertEquals(4, tupleThree.getFieldLabels().size());
    assertEquals(3, tupleThree.getFieldNames().size());
    // serialise merged tuples
    {
      final TupleEntryWriter writer = new TupleEntryWriter();
      tupleThree.writeMap(writer);
      assertEquals(3, writer.tuple.getFields().size());
      // field-two label in tupleTwo replaced field-two label from tupleOne
      assertEquals("1", writer.tuple.get("field a"));
      assertEquals("3", writer.tuple.get("field c")); // field-three label from tupleOne
      assertEquals("4", writer.tuple.get("field d")); // field-four label from tupleTwo
    }

    tupleThree.setFieldNames(null);
    // full serialisation
    {
      final TupleEntryWriter writer = new TupleEntryWriter();
      tupleThree.writeMap(writer);
      assertEquals(4, writer.tuple.getFields().size());
      assertEquals("1", writer.tuple.get("field a"));
      assertEquals("2", writer.tuple.get("field b"));
      assertEquals("3", writer.tuple.get("field c"));
      assertEquals("4", writer.tuple.get("field d"));
    }
  }

  private static final class TupleEntryWriter implements EntryWriter {
    final Tuple tuple = new Tuple();

    @Override
    public EntryWriter put(CharSequence k, Object v) throws IOException {
      tuple.put(k.toString(), v);
      return this;
    }
  }
}
