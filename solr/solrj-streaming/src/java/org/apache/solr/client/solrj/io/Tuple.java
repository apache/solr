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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.params.StreamParams;
import org.apache.solr.common.util.CollectionUtil;

/**
 * A simple abstraction of a record containing key/value pairs. Convenience methods are provided for
 * returning single and multiValue String, Long and Double values. Note that ints and floats are
 * treated as longs and doubles respectively.
 */
public class Tuple implements Cloneable, MapWriter {

  /**
   * When EOF field is true the Tuple marks the end of the stream. The EOF Tuple will not contain a
   * record from the stream, but it may contain metrics/aggregates gathered by underlying streams.
   */
  public boolean EOF;

  /**
   * When EXCEPTION field is true the Tuple marks an exception in the stream and the corresponding
   * "EXCEPTION" field contains a related message.
   */
  public boolean EXCEPTION;

  private final Map<String, Object> fields = CollectionUtil.newHashMap(2);
  private List<String> fieldNames;
  private Map<String, String> fieldLabels;

  public Tuple() {
    // just an empty tuple
  }

  public Tuple(String k1, Object v1) {
    if (k1 != null) put(k1, v1);
  }

  public Tuple(String k1, Object v1, String k2, Object v2) {
    if (k1 != null) put(k1, v1);
    if (k2 != null) put(k2, v2);
  }

  /**
   * A copy constructor.
   *
   * @param fields map containing keys and values to be copied to this tuple
   */
  public Tuple(Map<String, ?> fields) {
    putAll(fields);
  }

  /**
   * A copy constructor
   *
   * @param original Tuple that will be copied
   */
  public Tuple(Tuple original) {
    this.putAll(original.fields);
    if (original.fieldNames != null) {
      this.fieldNames = new ArrayList<>(original.fieldNames);
    }
    if (original.fieldLabels != null) {
      this.fieldLabels = new HashMap<>(original.fieldLabels);
    }
  }

  public Object get(String key) {
    return this.fields.get(key);
  }

  public void put(String key, Object value) {
    this.fields.put(key, value);
    if (key.equals(StreamParams.EOF)) {
      EOF = true;
    } else if (key.equals(StreamParams.EXCEPTION)) {
      EXCEPTION = true;
    }
  }

  public void putAll(Map<String, ?> fields) {
    this.fields.putAll(fields);
    if (fields.containsKey(StreamParams.EOF)) {
      EOF = true;
    }
    if (fields.containsKey(StreamParams.EXCEPTION)) {
      EXCEPTION = true;
    }
  }

  public void remove(String key) {
    this.fields.remove(key);
  }

  public String getString(String key) {
    return String.valueOf(this.fields.get(key));
  }

  public String getException() {
    return (String) this.fields.get(StreamParams.EXCEPTION);
  }

  public Long getLong(String key) {
    Object o = this.fields.get(key);

    if (o == null) {
      return null;
    }

    if (o instanceof Long) {
      return (Long) o;
    } else if (o instanceof Number) {
      return ((Number) o).longValue();
    } else {
      // Attempt to parse the long
      return Long.parseLong(o.toString());
    }
  }

  // Convenience method since Booleans can be passed around as Strings.
  public Boolean getBool(String key) {
    Object o = this.fields.get(key);

    if (o == null) {
      return null;
    }

    if (o instanceof Boolean) {
      return (Boolean) o;
    } else {
      // Attempt to parse the Boolean
      return Boolean.parseBoolean(o.toString());
    }
  }

  @SuppressWarnings({"unchecked"})
  public List<Boolean> getBools(String key) {
    return (List<Boolean>) this.fields.get(key);
  }

  // Convenience methods since the dates are actually shipped around as Strings.
  public Date getDate(String key) {
    Object o = this.fields.get(key);

    if (o == null) {
      return null;
    }

    if (o instanceof Date) {
      return (Date) o;
    } else {
      // Attempt to parse the Date from a String
      return new Date(Instant.parse(o.toString()).toEpochMilli());
    }
  }

  @SuppressWarnings({"unchecked"})
  public List<Date> getDates(String key) {
    List<String> vals = (List<String>) this.fields.get(key);
    if (vals == null) return null;

    List<Date> ret = new ArrayList<>();
    for (String dateStr : (List<String>) this.fields.get(key)) {
      ret.add(new Date(Instant.parse(dateStr).toEpochMilli()));
    }
    return ret;
  }

  public Double getDouble(String key) {
    Object o = this.fields.get(key);

    if (o == null) {
      return null;
    }

    if (o instanceof Double) {
      return (Double) o;
    } else {
      // Attempt to parse the double
      return Double.parseDouble(o.toString());
    }
  }

  @SuppressWarnings({"unchecked"})
  public List<String> getStrings(String key) {
    return (List<String>) this.fields.get(key);
  }

  @SuppressWarnings({"unchecked"})
  public List<Long> getLongs(String key) {
    return (List<Long>) this.fields.get(key);
  }

  @SuppressWarnings({"unchecked"})
  public List<Double> getDoubles(String key) {
    return (List<Double>) this.fields.get(key);
  }

  /** Return all tuple fields and their values. */
  public Map<String, Object> getFields() {
    return this.fields;
  }

  /**
   * This represents the mapping of external field labels to the tuple's internal field names if
   * they are different from field names.
   *
   * @return field labels or null
   */
  public Map<String, String> getFieldLabels() {
    return fieldLabels;
  }

  public void setFieldLabels(Map<String, String> fieldLabels) {
    this.fieldLabels = fieldLabels;
  }

  /**
   * A list of field names to serialize. This list (together with the mapping in {@link
   * #getFieldLabels()} determines what tuple values are serialized and their external (serialized)
   * names.
   *
   * @return list of external field names or null
   */
  public List<String> getFieldNames() {
    return fieldNames;
  }

  public void setFieldNames(List<String> fieldNames) {
    this.fieldNames = fieldNames;
  }

  @SuppressWarnings({"unchecked"})
  public List<Map<?, ?>> getMaps(String key) {
    return (List<Map<?, ?>>) this.fields.get(key);
  }

  public void setMaps(String key, List<Map<?, ?>> maps) {
    this.fields.put(key, maps);
  }

  @SuppressWarnings({"unchecked"})
  public Map<String, Map<?, ?>> getMetrics() {
    return (Map<String, Map<?, ?>>) this.fields.get(StreamParams.METRICS);
  }

  public void setMetrics(Map<String, Map<?, ?>> metrics) {
    this.fields.put(StreamParams.METRICS, metrics);
  }

  @Override
  public Tuple clone() {
    return new Tuple(this);
  }

  /**
   * The other tuples fields and fieldLabels will be putAll'd directly to this's fields and
   * fieldLabels while other's fieldNames will be added such that duplicates aren't present.
   *
   * @param other Tuple to be merged into this.
   */
  public void merge(Tuple other) {
    this.putAll(other.getFields());
    if (other.fieldNames != null) {
      if (this.fieldNames != null) {
        this.fieldNames.addAll(
            other.fieldNames.stream()
                .filter(otherFieldName -> !this.fieldNames.contains(otherFieldName))
                .collect(Collectors.toList()));
      } else {
        this.fieldNames = new ArrayList<>(other.fieldNames);
      }
    }
    if (other.fieldLabels != null) {
      if (this.fieldLabels != null) {
        this.fieldLabels.putAll(other.fieldLabels);
      } else {
        this.fieldLabels = new HashMap<>(other.fieldLabels);
      }
    }
  }

  @Override
  public void writeMap(EntryWriter ew) throws IOException {
    if (fieldNames == null) {
      fields.forEach(
          (k, v) -> {
            try {
              ew.put(k, v);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
    } else {
      for (String fieldName : fieldNames) {
        String label = fieldLabels.get(fieldName);
        ew.put(label, fields.get(label));
      }
    }
  }

  /** Create a new empty tuple marked as EOF. */
  public static Tuple EOF() {
    Tuple tuple = new Tuple();
    tuple.put(StreamParams.EOF, true);
    return tuple;
  }

  /**
   * Create a new empty tuple marked as EXCEPTION, and optionally EOF.
   *
   * @param msg exception message
   * @param isEOF if true the tuple will be marked as EOF
   */
  public static Tuple EXCEPTION(String msg, boolean isEOF) {
    Tuple tuple = new Tuple();
    tuple.put(StreamParams.EXCEPTION, msg);
    if (isEOF) {
      tuple.put(StreamParams.EOF, true);
    }
    return tuple;
  }

  /**
   * Create a new empty tuple marked as EXCEPTION and optionally EOF.
   *
   * @param t exception - full stack trace will be used as an exception message
   * @param isEOF if true the tuple will be marked as EOF
   */
  public static Tuple EXCEPTION(Throwable t, boolean isEOF) {
    StringWriter sw = new StringWriter();
    t.printStackTrace(new PrintWriter(sw));
    return EXCEPTION(sw.toString(), isEOF);
  }
}
