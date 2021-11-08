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

import static org.apache.solr.bench.generators.Lists.listsOf;
import static org.apache.solr.bench.generators.SourceDSL.checkArguments;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collector;
import org.apache.solr.bench.SolrGenerate;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.Lists;

/**
 * A Class for creating List Sources that will produce List objects of either fixed or bounded size.
 */
public class ListsDSL {

  /**
   * Creates an appropriate Collector for a type of List by specifying the Supplier used as a
   * parameter
   *
   * @param <T> type to generate
   * @param <A> list type
   * @param collectionFactory a supplier of A
   * @return a Collector
   */
  public <T, A extends List<T>> Collector<T, A, A> createListCollector(
      Supplier<A> collectionFactory) {
    return Lists.toList(collectionFactory);
  }

  /**
   * Creates a ListGeneratorBuilder.
   *
   * @param <T> type to generate
   * @param source a Source of type T for the items in the list
   * @return a ListGeneratorBuilder of type T
   */
  public <T> ListGeneratorBuilder<T> of(Gen<T> source) {
    return new ListGeneratorBuilder<>(source);
  }

  /**
   * ListGeneratorBuilder enables the creation of Sources for Lists of fixed and bounded size, where
   * no Collector is specified. A ListGeneratorBuilder can be used to create a
   * TypedListGeneratorBuilder, where the Collector is specified.
   *
   * @param <T> type to generate
   */
  public static class ListGeneratorBuilder<T> {

    protected final Gen<T> source;

    ListGeneratorBuilder(Gen<T> source) {
      this.source = source;
    }

    /**
     * Generates a List of objects, where the size of the List is fixed
     *
     * @param size size of lists to generate
     * @return a Source of Lists of type T
     */
    public Gen<List<T>> ofSize(int size) {
      return ofSizeBetween(size, size);
    }

    /**
     * Generates a List of objects, where the size of the List is bounded by minimumSize and
     * maximumSize
     *
     * @param minimumSize - inclusive minimum size of List
     * @param maximumSize - inclusive maximum size of List
     * @return a Source of Lists of type T
     */
    public Gen<List<T>> ofSizeBetween(int minimumSize, int maximumSize) {
      checkBoundedListArguments(minimumSize, maximumSize);
      return ofSizes(SolrGenerate.range(minimumSize, maximumSize));
    }

    public Gen<List<T>> ofSizes(Gen<Integer> sizes) {
      return listsOf(source, sizes);
    }

    /**
     * Determines how the Lists will be collected and returns an TypedListGeneratorBuilder with the
     * Collector specified
     *
     * @param collector collector to use to construct list
     * @return a TypedListGeneratorBuilder
     */
    public TypedListGeneratorBuilder<T> ofType(Collector<T, List<T>, List<T>> collector) {
      return new TypedListGeneratorBuilder<>(source, collector);
    }
  }

  /**
   * TypedListGeneratorBuilder enables the creation of Sources for Lists of fixed and bounded size,
   * where the Collector is fixed.
   */
  public static class TypedListGeneratorBuilder<T> {

    private final Gen<T> source;
    private final Collector<T, List<T>, List<T>> collector;

    TypedListGeneratorBuilder(Gen<T> source, Collector<T, List<T>, List<T>> collector) {
      this.source = source;
      this.collector = collector;
    }

    /**
     * Generates a List of objects, where the size of the List is fixed
     *
     * @param size size of lists to generate
     * @return a Source of Lists of type T
     */
    public Gen<List<T>> ofSize(int size) {
      return ofSizeBetween(size, size);
    }

    /**
     * Generates a List of objects, where the size of the List is bounded by minimumSize and
     * maximumSize
     *
     * @param minimumSize - inclusive minimum size of List
     * @param maximumSize - inclusive maximum size of List
     * @return a Source of Lists of type T
     */
    public Gen<List<T>> ofSizeBetween(int minimumSize, int maximumSize) {
      checkBoundedListArguments(minimumSize, maximumSize);
      return listsOf(source, collector, SolrGenerate.range(minimumSize, maximumSize));
    }
  }

  private static void checkBoundedListArguments(int minimumSize, int maximumSize) {
    checkArguments(
        minimumSize <= maximumSize,
        "The minimumSize (%s) is longer than the maximumSize(%s)",
        minimumSize,
        maximumSize);
    checkSizeNotNegative(minimumSize);
  }

  private static void checkSizeNotNegative(int size) {
    checkArguments(
        size >= 0, "The size of a List cannot be negative; %s is not an accepted argument", size);
  }

  public <T, A extends List<T>> Collector<T, List<T>, List<T>> arrayList() {
    return Lists.arrayList();
  }

  public <T, A extends List<T>> Collector<T, List<T>, List<T>> linkedList() {
    return Lists.linkedList();
  }
}
