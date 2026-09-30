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
package org.apache.solr.jersey;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link Map} that renders identically to a plain {@link LinkedHashMap} everywhere except
 * through Jackson.
 *
 * <p>{@code org.apache.solr.core.PluginInfo#writeMap} groups a plugin's unnamed children (e.g.
 * anonymous {@code <processor>} entries under an {@code updateRequestProcessorChain}) under a
 * {@code null} key. Solr's legacy (v1) JSON response writer tolerates that null key natively,
 * rendering it as {@code ""}. Jackson, however, rejects null Map keys outright and (by default)
 * omits null-valued entries entirely.
 *
 * <p>Wrap a config/plugin-derived map in this class to let the v2/JAX-RS (Jackson) response path
 * render a self-describing key in place of the null key and keep null values, without changing
 * what any non-Jackson consumer (e.g. a v1 handler reading the same map instance) sees.
 */
public class NullKeyTolerantMap extends LinkedHashMap<String, Object> implements JsonSerializable {

  private final String nullKeyReplacement;

  /**
   * @param nullKeyReplacement the JSON field name to substitute for a {@code null} Map key,
   *     wherever one is found (at any nesting depth of Maps/Lists reachable from this instance).
   */
  public NullKeyTolerantMap(String nullKeyReplacement) {
    this.nullKeyReplacement = nullKeyReplacement;
  }

  @Override
  public void serialize(JsonGenerator gen, SerializerProvider serializers) throws IOException {
    writeValue(this, gen, serializers);
  }

  @Override
  public void serializeWithType(
      JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer)
      throws IOException {
    serialize(gen, serializers);
  }

  private void writeValue(Object value, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    if (value instanceof Map<?, ?> m) {
      gen.writeStartObject();
      for (Map.Entry<?, ?> e : m.entrySet()) {
        Object key = e.getKey();
        gen.writeFieldName(key == null ? nullKeyReplacement : key.toString());
        writeValue(e.getValue(), gen, provider);
      }
      gen.writeEndObject();
    } else if (value instanceof List<?> l) {
      gen.writeStartArray();
      for (Object item : l) {
        writeValue(item, gen, provider);
      }
      gen.writeEndArray();
    } else if (value == null) {
      gen.writeNull();
    } else {
      provider.defaultSerializeValue(value, gen);
    }
  }
}
