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
package org.apache.solr.bench.generators;

import static org.apache.solr.bench.generators.SourceDSL.checkArguments;

import java.util.Map;
import org.apache.solr.bench.SolrGenerate;
import org.quicktheories.core.Gen;

/**
 * A Class for creating Map Sources that will produce Map objects of either fixed or bounded size.
 */
public class MapsDSL {

  /**
   * Creates a ListGeneratorBuilder.
   *
   * @param keys Gen of keys
   * @param values Gen of values
   * @param <K> key type to generate
   * @param <V> value type to generate
   * @return a MapGeneratorBuilder of type K,V
   */
  public <K, V> MapGeneratorBuilder<K, V> of(SolrGen<K> keys, SolrGen<V> values) {
    return new MapGeneratorBuilder<>(keys, values);
  }

  /**
   * MapGeneratorBuilder enables the creation of Sources for Maps of fixed and bounded size, where
   * no Collector is specified. A MapGeneratorBuilder can be used to create a
   * TypedListGeneratorBuilder, where the Collector is specified.
   *
   * @param <K> key type to generate
   * @param <V> value type to generate
   */
  public static class MapGeneratorBuilder<K, V> {
    final Gen<K> kg;
    final Gen<V> vg;

    public MapGeneratorBuilder(final Gen<K> kg, final Gen<V> vg) {
      this.kg = kg;
      this.vg = vg;
    }

    /**
     * Generates a Map of objects, where the size of the Map is fixed
     *
     * @param size size of lists to generate
     * @return a Source of Maps of type K,V
     */
    public Gen<Map<K, V>> ofSize(int size) {
      return ofSizeBetween(size, size);
    }

    /**
     * Generates a Map of objects, where the size of the Map is bounded by minimumSize and
     * maximumSize
     *
     * @param minSize inclusive minimum size of Map
     * @param maxSize inclusive maximum size of Map
     * @return a Source of Maps of type T
     */
    public Gen<Map<K, V>> ofSizeBetween(int minSize, int maxSize) {
      checkBoundedArguments(minSize, maxSize);
      return ofSizes(SolrGenerate.range(minSize, maxSize));
    }

    /**
     * Generates a Map of objects with sizes drawn from sizes gen
     *
     * @param sizes Sizes of maps to generate
     * @return A Source of Maps of Type T
     */
    public Gen<Map<K, V>> ofSizes(Gen<Integer> sizes) {
      return Maps.boundedMapsOf(kg, vg, sizes);
    }

    private static void checkBoundedArguments(int minSize, int maxSize) {
      checkArguments(
          minSize <= maxSize, "The minSize (%s) is longer than the maxSize(%s)", minSize, maxSize);
      checkSizeNotNegative(minSize);
    }
  }

  private static void checkSizeNotNegative(int size) {
    checkArguments(
        size >= 0, "The size of a Map cannot be negative; %s is not an accepted argument", size);
  }
}
