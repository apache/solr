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

package org.apache.solr.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.solr.common.ConfigNode;
import org.apache.solr.common.util.PropertiesUtil;

/**
 * ConfigNode impl that applies property substitutions on access.
 *
 * <p>This class wraps another {@link ConfigNode} and applies property substitution immediately when
 * the {@link #txt} or {@link #attributes} methods are called. Because property substition is based
 * on ThreadLocal values, These methods <em>MUST</em> be called while those variables are "active"
 *
 * @see ConfigNode#SUBSTITUTES
 * @see PropertiesUtil#substitute
 */
public class DataConfigNode implements ConfigNode {
  private final String name;
  private final Map<String, String> rawAttributes;
  private final Map<String, List<ConfigNode>> kids;
  private final String rawTextData;

  public DataConfigNode(ConfigNode root) {
    Map<String, List<ConfigNode>> kids = new LinkedHashMap<>();
    name = root.name();
    rawAttributes = root.attributes();
    rawTextData = root.txt();
    root.forEachChild(
        it -> {
          List<ConfigNode> nodes = kids.computeIfAbsent(it.name(), k -> new ArrayList<>());
          nodes.add(new DataConfigNode(it));
          return Boolean.TRUE;
        });
    for (Map.Entry<String, List<ConfigNode>> e : kids.entrySet()) {
      if (e.getValue() != null) {
        e.setValue(List.copyOf(e.getValue()));
      }
    }
    this.kids = Map.copyOf(kids);
  }

  private static String substituteVal(String s) {
    return PropertiesUtil.substitute(s, SUBSTITUTES.get());
  }

  @Override
  public String name() {
    return name;
  }

  /** Each call to this method returns a (new) copy of the original txt with substitions applied. */
  @Override
  public String txt() {
    return substituteVal(rawTextData);
  }

  /**
   * Each call to this method returns a (new) copy of the original Map with substitions applied to
   * the values.
   */
  @Override
  public Map<String, String> attributes() {
    if (rawAttributes.isEmpty()) return rawAttributes; // avoid unnecessary object creation

    // Note: using the the 4 arg toMap to force LinkedHashMap.
    // Duplicate keys should be impossible, but toMap makes us specify a mergeFunction
    return rawAttributes.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e -> {
                  return substituteVal(e.getValue());
                },
                (v, vv) -> {
                  throw new IllegalStateException();
                },
                LinkedHashMap::new));
  }

  @Override
  public ConfigNode child(String name) {
    List<ConfigNode> val = kids.get(name);
    return val == null || val.isEmpty() ? null : val.get(0);
  }

  @Override
  public List<ConfigNode> getAll(String name) {
    return kids.getOrDefault(name, List.of());
  }

  @Override
  public List<ConfigNode> getAll(Set<String> names, Predicate<ConfigNode> test) {
    if (names == null) {
      return ConfigNode.super.getAll(names, test);
    }
    // fast implementation based on our index on named children:
    List<ConfigNode> result = new ArrayList<>();
    for (String s : names) {
      List<ConfigNode> vals = kids.get(s);
      if (vals != null) {
        vals.forEach(
            it -> {
              if (test == null || test.test(it)) {
                result.add(it);
              }
            });
      }
    }
    return result;
  }

  @Override
  public void forEachChild(Function<ConfigNode, Boolean> fun) {
    kids.forEach(
        (s, configNodes) -> {
          if (configNodes != null) {
            configNodes.forEach(fun::apply);
          }
        });
  }
}
