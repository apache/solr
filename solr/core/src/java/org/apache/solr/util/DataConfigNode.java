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

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.solr.common.ConfigNode;
import org.apache.solr.common.util.PropertiesUtil;

/** ConfigNode impl that copies and maintains data internally from DOM */
public class DataConfigNode implements ConfigNode {
  public final String name;
  public final Map<String, String> attributes;
  public final Map<String, List<ConfigNode>> kids;
  public final String textData;

  public DataConfigNode(ConfigNode root) {
    Map<String, List<ConfigNode>> kids = new LinkedHashMap<>();
    name = root.name();
    attributes = wrapSubstituting(root.attributes());
    textData = root.txt();
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

  /** provides a substitute view, and read-only */
  private static Map<String, String> wrapSubstituting(Map<String, String> delegate) {
    if (delegate.size() == 0) return delegate; // avoid unnecessary object creation
    return new SubstitutingMap(delegate);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String txt() {
    return substituteVal(textData);
  }

  @Override
  public Map<String, String> attributes() {
    return attributes;
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

  private static class SubstitutingMap extends AbstractMap<String, String> {

    private final Map<String, String> delegate;

    SubstitutingMap(Map<String, String> delegate) {
      this.delegate = delegate;
    }

    @Override
    public String get(Object key) {
      return substituteVal(delegate.get(key));
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public Set<String> keySet() {
      return delegate.keySet();
    }

    @Override
    public void forEach(BiConsumer<? super String, ? super String> action) {
      delegate.forEach((k, v) -> action.accept(k, substituteVal(v)));
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
      return new AbstractSet<>() {
        @Override
        public Iterator<Entry<String, String>> iterator() {
          // using delegate, return an iterator using Streams
          return delegate.entrySet().stream()
              .map(entry -> (Entry<String, String>) new SubstitutingEntry(entry))
              .iterator();
        }

        @Override
        public int size() {
          return delegate.size();
        }
      };
    }

    private static class SubstitutingEntry implements Entry<String, String> {

      private final Entry<String, String> delegateEntry;

      SubstitutingEntry(Entry<String, String> delegateEntry) {
        this.delegateEntry = delegateEntry;
      }

      @Override
      public String getKey() {
        return delegateEntry.getKey();
      }

      @Override
      public String getValue() {
        return substituteVal(delegateEntry.getValue());
      }

      @Override
      public String setValue(String value) {
        throw new UnsupportedOperationException();
      }
    }
  }
}
