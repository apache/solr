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
package org.apache.solr.handler.admin.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.ByteArrayUtf8CharSequence;

/**
 * Converts {@link SolrDocument} field values into plain, JSON-serializable types.
 *
 * <p>{@code solr/api} response POJOs (e.g. {@code GetDocumentsResponse}) can't reference {@link
 * SolrDocument} directly since that module doesn't depend on {@code solrj}, so callers that
 * populate those POJOs from a {@link SolrDocument} need this conversion first.
 */
public final class SolrDocumentFieldConverter {

  private SolrDocumentFieldConverter() {}

  /** Converts a {@link SolrDocument}'s entries into a plain {@code Map<String, Object>}. */
  public static Map<String, Object> toFieldMap(SolrDocument doc) {
    final var docMap = new HashMap<String, Object>();
    for (var entry : doc.entrySet()) {
      docMap.put(entry.getKey(), convertFieldValue(entry.getValue()));
    }
    return docMap;
  }

  /**
   * Converts a field value from a SolrDocument to a JSON-serializable type. Handles IndexableField,
   * Utf8CharSequence, and Collection types.
   */
  private static Object convertFieldValue(Object value) {
    if (value == null) {
      return null;
    }

    // Handle Lucene IndexableField objects
    if (value instanceof IndexableField field) {
      // Try numeric value first
      Number numericValue = field.numericValue();
      if (numericValue != null) {
        return numericValue;
      }
      // Fall back to string value
      String stringValue = field.stringValue();
      if (stringValue != null) {
        return stringValue;
      }
      // If neither, try binary value
      BytesRef binaryValue = field.binaryValue();
      if (binaryValue != null) {
        return binaryValue.utf8ToString();
      }
      return null;
    }

    // Handle Utf8CharSequence
    value = ByteArrayUtf8CharSequence.convertCharSeq(value);

    // Recursively handle collections
    if (value instanceof Collection<?> collection) {
      List<Object> converted = new ArrayList<>(collection.size());
      for (Object item : collection) {
        converted.add(convertFieldValue(item));
      }
      return converted;
    }

    return value;
  }
}
