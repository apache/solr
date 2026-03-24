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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

/** A map of metadata name/value pairs. */
public class ExtractionMetadata extends LinkedHashMap<String, List<String>> {
  /**
   * Add a metadata value. If the name already exists, the value will be appended to the existing
   * list.
   */
  public void add(String name, String value) {
    if (name == null || value == null) return;
    computeIfAbsent(name, k -> new ArrayList<>()).add(value);
  }

  /** Add multiple metadata values. */
  public void add(String name, Collection<String> values) {
    if (name == null || values == null || values.isEmpty()) return;
    computeIfAbsent(name, k -> new ArrayList<>()).addAll(values);
  }

  /** Gets all metadata values for the given name. */
  public List<String> get(String name) {
    List<String> vals = super.get(name);
    return (vals == null) ? List.of() : vals;
  }

  /** Gets the first metadata value for the given name or null if not set. */
  public String getFirst(String name) {
    List<String> vals = super.get(name);
    if (vals == null || vals.isEmpty()) return null;
    return vals.getFirst();
  }
}
