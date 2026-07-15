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
package org.apache.solr.common.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

/**
 * Converts a parsed response into the canonical shape the SolrJ response classes expect (the shape
 * the binary and XML parsers produce): nested JSON objects become {@link NamedList}s and a {@code
 * {numFound, docs}} object becomes a {@link SolrDocumentList}.
 *
 * <p>Only unambiguous, self-describing conversions are performed. It is a no-op for values already
 * in canonical form (so binary/XML responses pass through unchanged). It does not attempt to
 * interpret the ambiguous flat arrays produced by {@code json.nl=flat}; a typed JSON parser should
 * request {@code json.nl=map} for its own reads.
 */
public final class ResponseNormalizer {

  private ResponseNormalizer() {}

  /** Returns a normalized copy of the given response NamedList. */
  public static NamedList<Object> normalize(NamedList<Object> response) {
    if (response == null) {
      return null;
    }
    SimpleOrderedMap<Object> out = new SimpleOrderedMap<>(response.size());
    for (Map.Entry<String, Object> e : response) {
      out.add(e.getKey(), normalizeValue(e.getValue()));
    }
    return out;
  }

  @SuppressWarnings("unchecked")
  private static Object normalizeValue(Object val) {
    if (val instanceof SolrDocumentList || val instanceof SolrDocument) {
      // Already canonical (binary/XML produce these directly); leave untouched. Must precede the
      // List/Map branches since SolrDocumentList is a List and SolrDocument is a Map.
      return val;
    } else if (val instanceof NamedList) {
      // Already canonical (binary/XML), but its children may still need normalizing.
      NamedList<Object> in = (NamedList<Object>) val;
      SimpleOrderedMap<Object> out = new SimpleOrderedMap<>(in.size());
      for (Map.Entry<String, Object> e : in) {
        out.add(e.getKey(), normalizeValue(e.getValue()));
      }
      return out;
    } else if (val instanceof Map) {
      Map<String, Object> m = (Map<String, Object>) val;
      if (isDocList(m)) {
        return toDocList(m);
      }
      // SimpleOrderedMap (not plain NamedList): it is the canonical map type the binary parser
      // produces, and some response extractors cast to it specifically.
      SimpleOrderedMap<Object> out = new SimpleOrderedMap<>(m.size());
      for (Map.Entry<String, Object> e : m.entrySet()) {
        out.add(e.getKey(), normalizeValue(e.getValue()));
      }
      return out;
    } else if (val instanceof List) {
      List<Object> in = (List<Object>) val;
      List<Object> out = new ArrayList<>(in.size());
      for (Object item : in) {
        out.add(normalizeValue(item));
      }
      return out;
    }
    return val;
  }

  private static boolean isDocList(Map<String, Object> m) {
    return m.get("numFound") instanceof Number && m.get("docs") instanceof List;
  }

  @SuppressWarnings("unchecked")
  private static SolrDocumentList toDocList(Map<String, Object> m) {
    SolrDocumentList docs = new SolrDocumentList();
    docs.setNumFound(((Number) m.get("numFound")).longValue());
    if (m.get("start") instanceof Number start) {
      docs.setStart(start.longValue());
    }
    if (m.get("maxScore") instanceof Number maxScore) {
      docs.setMaxScore(maxScore.floatValue());
    }
    if (m.get("numFoundExact") instanceof Boolean exact) {
      docs.setNumFoundExact(exact);
    }
    for (Object d : (List<Object>) m.get("docs")) {
      docs.add(toDoc(d));
    }
    return docs;
  }

  @SuppressWarnings("unchecked")
  private static SolrDocument toDoc(Object o) {
    SolrDocument doc = new SolrDocument();
    if (o instanceof Map) {
      for (Map.Entry<String, Object> f : ((Map<String, Object>) o).entrySet()) {
        // setField (not addField): addField unwraps a Collection value into a plain list, which
        // would drop the type of a reconstructed SolrDocumentList held as a field value.
        doc.setField(f.getKey(), normalizeValue(f.getValue()));
      }
    }
    return doc;
  }
}
