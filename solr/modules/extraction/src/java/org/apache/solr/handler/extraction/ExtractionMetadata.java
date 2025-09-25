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
package org.apache.solr.handler.extraction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Simple metadata bean */
public class ExtractionMetadata {
  private final Map<String, List<String>> map = new LinkedHashMap<>();

  public void add(String name, String value) {
    if (name == null || value == null) return;
    map.computeIfAbsent(name, k -> new ArrayList<>()).add(value);
  }

  public void addValues(String name, String[] values) {
    if (name == null || values == null || values.length == 0) return;
    map.computeIfAbsent(name, k -> new ArrayList<>()).addAll(List.of(values));
  }

  public void addIfNotNull(String resourceNameKey, String resourceName) {
    if (resourceName != null) {
      add(resourceNameKey, resourceName);
    }
  }

  public void putAll(Map<String, List<String>> map) {
    this.map.putAll(map);
  }

  public String[] getValues(String name) {
    List<String> vals = map.get(name);
    if (vals == null) return new String[0];
    return vals.toArray(new String[0]);
  }

  public String get(String name) {
    List<String> vals = map.get(name);
    if (vals == null || vals.isEmpty()) return null;
    return vals.get(0);
  }

  public String[] names() {
    return map.keySet().toArray(new String[0]);
  }

  public void remove(String name) {
    map.remove(name);
  }

  public Map<String, List<String>> asMap() {
    return map;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ExtractionMetadata{");
    boolean first = true;
    for (Map.Entry<String, List<String>> e : map.entrySet()) {
      if (!first) sb.append(", ");
      first = false;
      sb.append(e.getKey()).append('=').append(e.getValue());
    }
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof ExtractionMetadata)) return false;
    ExtractionMetadata that = (ExtractionMetadata) obj;
    return Objects.equals(this.map, that.map);
  }

  @Override
  public int hashCode() {
    return Objects.hash(map);
  }
}
