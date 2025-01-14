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

import static org.apache.solr.common.ConfigNode.Helpers._bool;
import static org.apache.solr.common.ConfigNode.Helpers._double;
import static org.apache.solr.common.ConfigNode.Helpers._int;
import static org.apache.solr.common.ConfigNode.Helpers._txt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.common.util.NamedList;

/**
 * A generic interface that represents a config file, mostly XML Please note that this is an
 * immutable, read-only object.
 */
public interface ConfigNode {
  ThreadLocal<Function<String, String>> SUBSTITUTES = new ThreadLocal<>();

  /** Name of the tag/element. */
  String name();

  /** Attributes of this node. Immutable shared instance. Not null. */
  Map<String, String> attributes();

  /** Mutable copy of {@link #attributes()}, excluding {@code exclusions} keys. */
  default Map<String, String> attributesExcept(String... exclusions) {
    assert exclusions.length < 5 : "non-performant exclusion list";
    final var attributes = attributes();
    Map<String, String> args = CollectionUtil.newHashMap(attributes.size());
    attributes.forEach(
        (k, v) -> {
          for (String ex : exclusions) if (ex.equals(k)) return;
          args.put(k, v);
        });
    return args;
  }

  /** Child by name */
  default ConfigNode child(String name) {
    return child(name, null);
  }

  /**
   * Child by name or return an empty node if null. If there are multiple values, it returns the
   * first element. This never returns a null.
   */
  default ConfigNode get(String name) {
    ConfigNode child = child(name, null);
    return child == null ? EMPTY : child;
  }

  default ConfigNode get(String name, Predicate<ConfigNode> test) {
    List<ConfigNode> children = getAll(Set.of(name), test);
    if (children.isEmpty()) return EMPTY;
    return children.get(0);
  }

  // @VisibleForTesting  Helps writing tests
  default ConfigNode get(String name, int idx) {
    List<ConfigNode> children = getAll(name);
    if (idx < children.size()) return children.get(idx);
    return EMPTY;
  }

  default ConfigNode childRequired(String name, Supplier<RuntimeException> err) {
    ConfigNode n = child(name);
    if (n == null) throw err.get();
    return n;
  }

  default boolean boolVal(boolean def) {
    return _bool(txt(), def);
  }

  default int intVal(int def) {
    return _int(txt(), def);
  }

  default String attr(String name, String def) {
    return _txt(attributes().get(name), def);
  }

  default String attr(String name) {
    return attributes().get(name);
  }

  /**
   * Like {@link #attr(String)} but throws an error (incorporating {@code missingErr}) if not found.
   */
  default String attrRequired(String name, String missingErr) {
    assert missingErr != null;
    String attr = attr(name);
    if (attr == null) {
      throw new RuntimeException(missingErr + ": missing mandatory attribute '" + name + "'");
    }
    return attr;
  }

  default int intAttr(String name, int def) {
    return _int(attr(name), def);
  }

  default boolean boolAttr(String name, boolean def) {
    return _bool(attr(name), def);
  }

  default String txt(String def) {
    return txt() == null ? def : txt();
  }

  String txt();

  default double doubleVal(double def) {
    return _double(txt(), def);
  }

  /** Iterate through child nodes with the tag/element name and return the first matching */
  default ConfigNode child(String name, Predicate<ConfigNode> test) {
    ConfigNode[] result = new ConfigNode[1];
    forEachChild(
        it -> {
          if (name != null && !name.equals(it.name())) return Boolean.TRUE;
          if (test == null || test.test(it)) {
            result[0] = it;
            return Boolean.FALSE;
          }
          return Boolean.TRUE;
        });
    return result[0];
  }

  /**
   * Iterate through child nodes with the names and return all the matching children
   *
   * @param names names of tags/elements to be returned. Null means all nodes.
   * @param test check for the nodes to be returned. Null means all nodes.
   */
  default List<ConfigNode> getAll(Set<String> names, Predicate<ConfigNode> test) {
    assert names == null || !names.isEmpty() : "Intended to pass null?";
    List<ConfigNode> result = new ArrayList<>();
    forEachChild(
        it -> {
          if ((names == null || names.contains(it.name())) && (test == null || test.test(it)))
            result.add(it);
          return Boolean.TRUE;
        });
    return result;
  }

  /** A list of all child nodes with the tag/element name. */
  default List<ConfigNode> getAll(String name) {
    return getAll(Set.of(name), null);
  }

  default boolean exists() {
    return true;
  }

  default boolean isNull() {
    return false;
  }

  /**
   * abortable iterate through children
   *
   * @param fun consume the node and return true to continue or false to abort
   */
  void forEachChild(Function<ConfigNode, Boolean> fun);

  default NamedList<Object> childNodesToNamedList() {
    NamedList<Object> result = new NamedList<>();
    forEachChild(
        it -> {
          String tag = it.name();
          String varName = it.attributes().get("name");
          if (DOMUtil.NL_TAGS.contains(tag)) {
            result.add(varName, DOMUtil.parseVal(tag, varName, it.txt()));
          }
          if ("lst".equals(tag)) {
            result.add(varName, it.childNodesToNamedList());
          } else if ("arr".equals(tag)) {
            List<Object> l = new ArrayList<>();
            result.add(varName, l);
            it.forEachChild(
                n -> {
                  if (DOMUtil.NL_TAGS.contains(n.name())) {
                    l.add(DOMUtil.parseVal(n.name(), null, n.txt()));
                  } else if ("lst".equals(n.name())) {
                    l.add(n.childNodesToNamedList());
                  }
                  return Boolean.TRUE;
                });
          }
          return Boolean.TRUE;
        });
    return result;
  }

  /** An empty node object. usually returned when the node is absent */
  ConfigNode EMPTY =
      new ConfigNode() {
        @Override
        public String name() {
          return null;
        }

        @Override
        public String txt() {
          return null;
        }

        @Override
        public Map<String, String> attributes() {
          return Map.of();
        }

        @Override
        public String attr(String name) {
          return null;
        }

        @Override
        public String attr(String name, String def) {
          return def;
        }

        @Override
        public ConfigNode child(String name) {
          return null;
        }

        @Override
        public ConfigNode get(String name) {
          return EMPTY;
        }

        @Override
        public boolean exists() {
          return false;
        }

        @Override
        public boolean isNull() {
          return true;
        }

        @Override
        public void forEachChild(Function<ConfigNode, Boolean> fun) {}
      };

  class Helpers {
    static boolean _bool(Object v, boolean def) {
      return v == null ? def : Boolean.parseBoolean(v.toString());
    }

    static String _txt(Object v, String def) {
      return v == null ? def : v.toString();
    }

    static int _int(Object v, int def) {
      return v == null ? def : Integer.parseInt(v.toString());
    }

    static double _double(Object v, double def) {
      return v == null ? def : Double.parseDouble(v.toString());
    }

    public static Predicate<ConfigNode> at(int i) {
      return new Predicate<>() {
        int index = 0;

        @Override
        public boolean test(ConfigNode node) {
          if (index == i) return true;
          index++;
          return false;
        }
      };
    }
  }
}
