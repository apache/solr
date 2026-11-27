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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.solr.common.ConfigNode;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.common.util.SimpleOrderedMap;

/**
 * Descriptive metadata attached to a schema, a field type, a field or a dynamic field. It is
 * persisted as a {@code <meta>} child element with typed children ({@code str}, {@code int}, {@code
 * long}, {@code float}, {@code double}, {@code bool}, {@code null}, and nested {@code lst} / {@code
 * arr}), and exposed as a {@code meta} property. Metadata never affects indexing, querying or
 * schema compatibility.
 *
 * <p>Instances are immutable. Keys are unique at every nesting level. Values are {@link String},
 * {@link Integer}, {@link Long}, {@link Float}, {@link Double}, {@link Boolean}, {@code null},
 * nested maps and lists of those.
 */
public final class SchemaMetadata {

  /** Element and property name used for metadata. */
  public static final String META = "meta";

  /** Attribute names that suggest metadata was meant; errors about them point to {@code <meta>}. */
  private static final Set<String> METADATA_LIKE_ATTRIBUTES =
      Set.of(META, "metadata", "description");

  /**
   * Hard limit on nesting while parsing, applied before validation so that deeply nested input
   * cannot exhaust the stack. {@link SchemaMetadataValidator} may apply a tighter supported limit.
   */
  static final int MAX_PARSE_DEPTH = 10;

  public static final SchemaMetadata EMPTY = new SchemaMetadata(Map.of());

  private final Map<String, Object> entries;

  /**
   * Creates schema metadata from the supplied entry map. Private constructor,
   * use one of the static factory methods for creation.
   *
   * @param entries unmodifiable map with the metadata entries to store
   */
  private SchemaMetadata(Map<String, Object> entries) {
    this.entries = entries;
  }

  /** Returns true if there are no entries. */
  public boolean isEmpty() {
    return entries.isEmpty();
  }

  /** Returns the value of a top-level entry, or null if absent. */
  public Object get(String key) {
    return entries.get(key);
  }

  /** Returns an unmodifiable view of the entries, with nested maps and lists also unmodifiable. */
  public Map<String, Object> getEntries() {
    return entries;
  }

  /**
   * Returns the entries as a new {@link SimpleOrderedMap}, with nested maps also converted to
   * {@link SimpleOrderedMap}, for use in property output and schema persistence.
   */
  public SimpleOrderedMap<Object> toMap() {
    return toSimpleOrderedMap(entries);
  }

  /**
   * Parses the {@code <meta>} child of a schema element. Rejects more than one {@code <meta>}
   * child, unknown tags, missing names and duplicate names, then validates the result.
   *
   * @param owner the element that may have a {@code <meta>} child, e.g. a {@code <field>}
   * @param context names the owning element in error messages, e.g. {@code field 'id'}
   */
  public static SchemaMetadata fromConfigNode(ConfigNode owner, String context) {
    List<ConfigNode> metaNodes = owner.getAll(META);
    if (metaNodes.isEmpty()) {
      return EMPTY;
    }
    if (metaNodes.size() > 1) {
      throw error(context, "more than one <" + META + "> element");
    }
    Map<String, Object> parsed = parseMap(metaNodes.get(0), context, META, 0);
    return of(parsed, context);
  }

  /**
   * Builds metadata from a property value: an existing {@link SchemaMetadata} or a {@link Map}
   * (e.g. parsed JSON). Integral numbers become {@link Integer} when they fit, otherwise {@link
   * Long}; other numbers keep their type, except {@link BigDecimal} which becomes {@link Double}.
   *
   * @param value the property value, may be null
   * @param context names the owning element in error messages, e.g. {@code field 'id'}
   */
  public static SchemaMetadata fromObject(Object value, String context) {
    if (value == null) {
      return EMPTY;
    }
    if (value instanceof SchemaMetadata metadata) {
      return metadata;
    }
    if (!(value instanceof Map)) {
      throw new SolrException(
          ErrorCode.BAD_REQUEST,
          "Invalid '" + META + "' for " + context + ": expected an object, got " + describe(value));
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> normalized = (Map<String, Object>) normalize(value, context, META, 0);
    return of(normalized, context);
  }

  private static SchemaMetadata of(Map<String, Object> entries, String context) {
    SchemaMetadataValidator.validate(entries, context);
    if (entries.isEmpty()) {
      return EMPTY;
    }
    return new SchemaMetadata(entries);
  }

  private static void checkNesting(String context, String path, int depth) {
    if (depth > MAX_PARSE_DEPTH) {
      throw error(context, "'" + path + "' is nested deeper than " + MAX_PARSE_DEPTH + " levels");
    }
  }

  private static Map<String, Object> parseMap(
      ConfigNode node, String context, String path, int depth) {
    checkNesting(context, path, depth);
    Map<String, Object> result = new LinkedHashMap<>();
    node.forEachChild(
        child -> {
          String tag = child.name();
          String name = child.attributes().get("name");
          if (name == null || name.isEmpty()) {
            throw error(context, "<" + tag + "> in " + path + " has no 'name'");
          }
          String childPath = path + "/" + name;
          if (result.containsKey(name)) {
            throw error(context, "duplicate name '" + childPath + "'");
          }
          result.put(name, parseValue(child, context, childPath, depth));
          return Boolean.TRUE;
        });
    return Collections.unmodifiableMap(result);
  }

  private static List<Object> parseList(ConfigNode node, String context, String path, int depth) {
    checkNesting(context, path, depth);
    List<Object> result = new ArrayList<>();
    node.forEachChild(
        child -> {
          result.add(parseValue(child, context, path + "[" + result.size() + "]", depth));
          return Boolean.TRUE;
        });
    return Collections.unmodifiableList(result);
  }

  private static Object parseValue(ConfigNode node, String context, String path, int depth) {
    String tag = node.name();
    if ("lst".equals(tag)) {
      return parseMap(node, context, path, depth + 1);
    }
    if ("arr".equals(tag)) {
      return parseList(node, context, path, depth + 1);
    }
    if (!DOMUtil.NL_TAGS.contains(tag)) {
      throw error(
          context,
          "unknown tag <"
              + tag
              + "> at '"
              + path
              + "', expected one of "
              + DOMUtil.NL_TAGS
              + ", lst or arr");
    }
    String text = node.txt();
    try {
      return DOMUtil.parseVal(tag, path, text == null ? "" : text);
    } catch (SolrException e) {
      throw error(context, "invalid <" + tag + "> value at '" + path + "': \"" + text + "\"", e);
    }
  }

  private static Object normalize(Object value, String context, String path, int depth) {
    if (value == null
        || value instanceof String
        || value instanceof Boolean
        || value instanceof Integer
        || value instanceof Float
        || value instanceof Double) {
      return value;
    }
    if (value instanceof Long l) {
      return narrow(l);
    }
    if (value instanceof BigInteger bigInteger) {
      try {
        return narrow(bigInteger.longValueExact());
      } catch (ArithmeticException e) {
        throw error(context, "number at '" + path + "' does not fit in a long: " + value);
      }
    }
    if (value instanceof BigDecimal bigDecimal) {
      return bigDecimal.doubleValue();
    }
    if (value instanceof Map<?, ?> map) {
      checkNesting(context, path, depth);
      Map<String, Object> result = new LinkedHashMap<>();
      for (Map.Entry<?, ?> e : map.entrySet()) {
        if (!(e.getKey() instanceof String key)) {
          throw error(context, "non-string key at '" + path + "': " + e.getKey());
        }
        result.put(key, normalize(e.getValue(), context, path + "/" + key, depth + 1));
      }
      return Collections.unmodifiableMap(result);
    }
    if (value instanceof List<?> list) {
      checkNesting(context, path, depth);
      List<Object> result = new ArrayList<>();
      for (Object item : list) {
        result.add(normalize(item, context, path + "[" + result.size() + "]", depth + 1));
      }
      return Collections.unmodifiableList(result);
    }
    throw error(context, "unsupported value at '" + path + "': " + describe(value));
  }

  private static Object narrow(long l) {
    return (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) ? (Object) (int) l : (Object) l;
  }

  private static SimpleOrderedMap<Object> toSimpleOrderedMap(Map<String, Object> map) {
    SimpleOrderedMap<Object> result = new SimpleOrderedMap<>(map.size());
    for (Map.Entry<String, Object> e : map.entrySet()) {
      result.add(e.getKey(), toOutput(e.getValue()));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private static Object toOutput(Object value) {
    if (value instanceof Map) {
      return toSimpleOrderedMap((Map<String, Object>) value);
    }
    if (value instanceof List<?> list) {
      List<Object> result = new ArrayList<>(list.size());
      for (Object item : list) {
        result.add(toOutput(item));
      }
      return result;
    }
    return value;
  }

  private static String describe(Object value) {
    return value.getClass().getSimpleName() + " (" + value + ")";
  }

  /**
   * Returns a hint to append to an "invalid attribute" error when any of the names looks like an
   * attempt to add metadata as an attribute, otherwise an empty string.
   */
  public static String attributeHint(Iterable<String> attributeNames) {
    for (String name : attributeNames) {
      if (METADATA_LIKE_ATTRIBUTES.contains(name)) {
        return ". '"
            + name
            + "' is not an attribute: put descriptive metadata in a <"
            + META
            + "> child element (JSON: a \""
            + META
            + "\" object)";
      }
    }
    return "";
  }

  static SolrException error(String context, String message) {
    return error(context, message, null);
  }

  static SolrException error(String context, String message, Throwable cause) {
    return new SolrException(
        ErrorCode.BAD_REQUEST, "Invalid '" + META + "' for " + context + ": " + message, cause);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof SchemaMetadata other && entries.equals(other.entries);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(entries);
  }

  @Override
  public String toString() {
    return entries.toString();
  }
}
