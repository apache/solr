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

package org.apache.solr.cluster.placement.impl;

import java.util.Objects;
import java.util.function.Function;
import org.apache.solr.cluster.placement.NodeMetric;

/**
 * Node metric identifier, corresponding to a node-level metric registry and the internal metric
 * name.
 */
public abstract class NodeMetricImpl<T> extends MetricImpl<T> implements NodeMetric<T> {

  private final Registry registry;

  public NodeMetricImpl(String name, Registry registry, String internalName) {
    this(name, registry, internalName, null);
  }

  public NodeMetricImpl(
      String name, Registry registry, String internalName, Function<Object, T> converter) {
    super(name, internalName, converter);
    Objects.requireNonNull(registry);
    this.registry = registry;
  }

  public NodeMetricImpl(String key) {
    this(key, null);
  }

  public NodeMetricImpl(String key, Function<Object, T> converter) {
    super(key, key, converter);
    this.registry = Registry.UNSPECIFIED;
  }

  @Override
  public Registry getRegistry() {
    return registry;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof NodeMetricImpl)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    NodeMetricImpl<?> that = (NodeMetricImpl<?>) o;
    return registry == that.registry;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), registry);
  }

  @Override
  public String toString() {
    if (registry != null) {
      return "NodeMetricImpl{"
          + "name='"
          + name
          + '\''
          + ", internalName='"
          + internalName
          + '\''
          + ", converter="
          + converter
          + ", registry="
          + registry
          + '}';
    } else {
      return "NodeMetricImpl{key=" + internalName + "}";
    }
  }

  static class IntNodeMetricImpl extends NodeMetricImpl<Integer> {

    public IntNodeMetricImpl(String name, Registry registry, String internalName) {
      super(name, registry, internalName);
    }

    public IntNodeMetricImpl(
        String name, Registry registry, String internalName, Function<Object, Integer> converter) {
      super(name, registry, internalName, converter);
    }

    public IntNodeMetricImpl(String key) {
      super(key);
    }

    public IntNodeMetricImpl(String key, Function<Object, Integer> converter) {
      super(key, converter);
    }

    @Override
    public Integer increase(Integer a, Integer b) {
      if (b == null) {
        return a;
      } else if (a == null) {
        return b;
      } else {
        return a + b;
      }
    }

    @Override
    public Integer decrease(Integer a, Integer b) {
      if (b == null) {
        return a;
      } else if (a == null) {
        return b * -1;
      } else {
        return a - b;
      }
    }
  }

  static class DoubleNodeMetricImpl extends NodeMetricImpl<Double> {

    public DoubleNodeMetricImpl(String name, Registry registry, String internalName) {
      super(name, registry, internalName);
    }

    public DoubleNodeMetricImpl(
        String name, Registry registry, String internalName, Function<Object, Double> converter) {
      super(name, registry, internalName, converter);
    }

    public DoubleNodeMetricImpl(String key) {
      super(key);
    }

    public DoubleNodeMetricImpl(String key, Function<Object, Double> converter) {
      super(key, converter);
    }

    @Override
    public Double increase(Double a, Double b) {
      if (b == null) {
        return a;
      } else if (a == null) {
        return b;
      } else {
        return a + b;
      }
    }

    @Override
    public Double decrease(Double a, Double b) {
      if (b == null) {
        return a;
      } else if (a == null) {
        return b * -1;
      } else {
        return a - b;
      }
    }
  }

  static class StaticNodeMetricImpl<T> extends NodeMetricImpl<T> {

    public StaticNodeMetricImpl(String name, Registry registry, String internalName) {
      super(name, registry, internalName);
    }

    public StaticNodeMetricImpl(
        String name, Registry registry, String internalName, Function<Object, T> converter) {
      super(name, registry, internalName, converter);
    }

    public StaticNodeMetricImpl(String key) {
      super(key);
    }

    public StaticNodeMetricImpl(String key, Function<Object, T> converter) {
      super(key, converter);
    }

    @Override
    public T increase(T a, T b) {
      return a;
    }

    @Override
    public T decrease(T a, T b) {
      return a;
    }
  }
}
