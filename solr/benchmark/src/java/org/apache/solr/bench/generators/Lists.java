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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.quicktheories.api.AsString;
import org.quicktheories.core.Gen;

public final class Lists {

  private Lists() {}

  static <T> Gen<List<T>> listsOf(Gen<T> generator, Gen<Integer> sizes) {
    return listsOf(generator, arrayList(), sizes).mix(listsOf(generator, linkedList(), sizes));
  }

  public static <T, A extends List<T>> Collector<T, List<T>, List<T>> arrayList() {
    return toList(ArrayList::new);
  }

  public static <T, A extends List<T>> Collector<T, List<T>, List<T>> linkedList() {
    return toList(LinkedList::new);
  }

  public static <T, A extends List<T>> Collector<T, A, A> toList(Supplier<A> collectionFactory) {
    return Collector.of(
        collectionFactory,
        List::add,
        (left, right) -> {
          left.addAll(right);
          return left;
        });
  }

  static <T> Gen<List<T>> listsOf(
      Gen<T> values, Collector<T, List<T>, List<T>> collector, Gen<Integer> sizes) {

    Gen<List<T>> gen =
        prng -> {
          int size = sizes.generate(prng);
          return Stream.generate(() -> values.generate(prng)).limit(size).collect(collector);
        };
    return gen.describedAs(listDescriber(values::asString));
  }

  private static <T> AsString<List<T>> listDescriber(Function<T, String> valueDescriber) {
    return list -> list.stream().map(valueDescriber).collect(Collectors.joining(", ", "[", "]"));
  }
}
