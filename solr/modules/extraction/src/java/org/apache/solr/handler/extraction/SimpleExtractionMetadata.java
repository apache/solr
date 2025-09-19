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

/** Simple in-memory implementation of ExtractionMetadata. */
public class SimpleExtractionMetadata implements ExtractionMetadata {
  private final Map<String, List<String>> map = new LinkedHashMap<>();

  @Override
  public void add(String name, String value) {
    if (name == null || value == null) return;
    map.computeIfAbsent(name, k -> new ArrayList<>()).add(value);
  }

  @Override
  public String[] getValues(String name) {
    List<String> vals = map.get(name);
    if (vals == null) return new String[0];
    return vals.toArray(new String[0]);
  }

  @Override
  public String get(String name) {
    List<String> vals = map.get(name);
    if (vals == null || vals.isEmpty()) return null;
    return vals.get(0);
  }

  @Override
  public String[] names() {
    return map.keySet().toArray(new String[0]);
  }
}
