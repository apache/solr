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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.solr.common.SolrException;

/**
 * Checks {@link SchemaMetadata} entries before they enter a schema. Every path that creates
 * metadata (schema XML, field and field type properties) goes through {@link #validate(Map,
 * String)}.
 *
 * <p>This is a minimal set of structural rules with fixed limits.
 */
// TODO SOLR-18679: replace with the shared vocabulary-aware validator (possibly moved to solrj so
// clients can reuse it).
public final class SchemaMetadataValidator {

  // TODO SOLR-18679: vocabulary rules (recognized prefixes, reject unknown non-"ext:" keys).
  static final Pattern KEY_PATTERN = Pattern.compile("[A-Za-z][A-Za-z0-9_.:-]*");

  // TODO SOLR-18679: make the limits configurable through EnvUtils and agree on defaults.
  static final int MAX_KEY_LENGTH = 64;
  static final int MAX_STRING_LENGTH = 256;
  static final int MAX_ENTRIES = 64;
  static final int MAX_DEPTH = 3;

  // TODO SOLR-18679: per-key expected types (e.g. dc:description must be a string) and a cap on
  // the total metadata size per schema.

  private SchemaMetadataValidator() {}

  /**
   * Returns the first code point that cannot be stored in the schema XML and read back unchanged,
   * or -1. Allowed are tab, newline and the XML 1.0 character range from U+0020; carriage return is
   * rejected because XML parsing turns it into a newline.
   */
  static int firstUnsupportedChar(String s) {
    for (int i = 0; i < s.length(); ) {
      int cp = s.codePointAt(i);
      boolean ok =
          cp == 0x9
              || cp == 0xA
              || (cp >= 0x20 && cp <= 0xD7FF)
              || (cp >= 0xE000 && cp <= 0xFFFD)
              || (cp >= 0x10000 && cp <= 0x10FFFF);
      if (!ok) {
        return cp;
      }
      i += Character.charCount(cp);
    }
    return -1;
  }

  /**
   * Validates metadata entries.
   *
   * @param entries top-level entries, with nested values as maps and lists
   * @param context names the owning element in error messages, e.g. {@code field 'id'}
   * @throws SolrException with code 400 if a rule is broken
   */
  public static void validate(Map<String, ?> entries, String context) {
    int[] count = new int[1];
    validateMap(entries, context, SchemaMetadata.META, 0, count);
  }

  private static void validateMap(
      Map<String, ?> map, String context, String path, int depth, int[] count) {
    for (Map.Entry<String, ?> e : map.entrySet()) {
      String key = e.getKey();
      String childPath = path + "/" + key;
      if (key.length() > MAX_KEY_LENGTH) {
        throw SchemaMetadata.error(
            context, "key '" + childPath + "' is longer than " + MAX_KEY_LENGTH + " characters");
      }
      if (!KEY_PATTERN.matcher(key).matches()) {
        throw SchemaMetadata.error(
            context, "invalid key '" + childPath + "', keys must match " + KEY_PATTERN.pattern());
      }
      validateValue(e.getValue(), context, childPath, depth, count);
    }
  }

  @SuppressWarnings("unchecked")
  private static void validateValue(
      Object value, String context, String path, int depth, int[] count) {
    if (++count[0] > MAX_ENTRIES) {
      throw SchemaMetadata.error(context, "more than " + MAX_ENTRIES + " entries");
    }
    if (value instanceof Map || value instanceof List) {
      if (depth + 1 > MAX_DEPTH) {
        throw SchemaMetadata.error(
            context, "'" + path + "' is nested deeper than " + MAX_DEPTH + " levels");
      }
      if (value instanceof Map) {
        validateMap((Map<String, ?>) value, context, path, depth + 1, count);
      } else {
        List<?> list = (List<?>) value;
        for (int i = 0; i < list.size(); i++) {
          validateValue(list.get(i), context, path + "[" + i + "]", depth + 1, count);
        }
      }
    } else if (value instanceof String s) {
      if (s.length() > MAX_STRING_LENGTH) {
        throw SchemaMetadata.error(
            context, "value of '" + path + "' is longer than " + MAX_STRING_LENGTH + " characters");
      }
      int bad = firstUnsupportedChar(s);
      if (bad >= 0) {
        throw SchemaMetadata.error(
            context,
            "value of '"
                + path
                + "' contains an unsupported character U+"
                + String.format(Locale.ROOT, "%04X", bad)
                + " (control characters other than tab and newline, and characters not allowed"
                + " in XML, cannot be stored)");
      }
    } else if (value != null
        && !(value instanceof Integer)
        && !(value instanceof Long)
        && !(value instanceof Float)
        && !(value instanceof Double)
        && !(value instanceof Boolean)) {
      throw SchemaMetadata.error(
          context,
          "unsupported value type " + value.getClass().getSimpleName() + " at '" + path + "'");
    }
  }
}
