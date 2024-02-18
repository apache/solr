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

package org.apache.solr.common;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import org.apache.solr.common.util.Utils;
import org.noggit.JSONWriter;

/**
 * Use this class to push all entries of a Map into an output. This avoids creating map instances
 * and is supposed to be memory efficient. If the entries are primitives, unnecessary boxing is also
 * avoided.
 */
public interface MapWriter extends MapSerializable, NavigableObject, JSONWriter.Writable {

  default String jsonStr() {
    return Utils.toJSONString(this);
  }

  @Override
  default Map<String, Object> toMap(Map<String, Object> map) {
    return Utils.convertToMap(this, map);
  }

  @Override
  default void write(JSONWriter writer) {
    writer.startObject();
    try {
      writeMap(
          new MapWriter.EntryWriter() {
            boolean first = true;

            @Override
            public MapWriter.EntryWriter put(CharSequence k, Object v) {
              if (first) {
                first = false;
              } else {
                writer.writeValueSeparator();
              }
              writer.indent();
              writer.writeString(k.toString());
              writer.writeNameSeparator();
              writer.write(v);
              return this;
            }
          });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    writer.endObject();
  }

  void writeMap(EntryWriter ew) throws IOException;

  default MapWriter append(MapWriter another) {
    MapWriter m = this;
    return ew -> {
      m.writeMap(ew);
      another.writeMap(ew);
    };
  }

  /**
   * An interface to push one entry at a time to the output. The order of the keys is not defined,
   * but we assume they are distinct -- don't call {@code put} more than once for the same key.
   */
  interface EntryWriter {

    /**
     * Writes a key value into the map
     *
     * @param k The key
     * @param v The value can be any supported object
     */
    EntryWriter put(CharSequence k, Object v) throws IOException;

    default EntryWriter putNoEx(CharSequence k, Object v) {
      try {
        put(k, v);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return this;
    }

    default EntryWriter put(CharSequence k, Object v, BiPredicate<CharSequence, Object> p)
        throws IOException {
      if (p.test(k, v)) put(k, v);
      return this;
    }

    default EntryWriter putIfNotNull(CharSequence k, Object v) throws IOException {
      if (v != null) put(k, v);
      return this;
    }

    default EntryWriter putIfNotNull(CharSequence k, Supplier<Object> v) throws IOException {
      Object val = v == null ? null : v.get();
      if (val != null) {
        putIfNotNull(k, val);
      }
      return this;
    }

    default EntryWriter put(CharSequence k, int v) throws IOException {
      put(k, (Integer) v);
      return this;
    }

    default EntryWriter put(CharSequence k, long v) throws IOException {
      put(k, (Long) v);
      return this;
    }

    default EntryWriter put(CharSequence k, float v) throws IOException {
      put(k, (Float) v);
      return this;
    }

    default EntryWriter put(CharSequence k, double v) throws IOException {
      put(k, (Double) v);
      return this;
    }

    default EntryWriter put(CharSequence k, boolean v) throws IOException {
      put(k, (Boolean) v);
      return this;
    }

    /** This is an optimization to avoid the instanceof checks. */
    default EntryWriter put(CharSequence k, CharSequence v) throws IOException {
      put(k, (Object) v);
      return this;
    }

    default BiConsumer<CharSequence, Object> getBiConsumer() {
      return (k, v) -> putNoEx(k, v);
    }
  }

  MapWriter EMPTY = new MapWriterMap(Collections.emptyMap());
}
