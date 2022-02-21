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

import java.lang.reflect.Array;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.solr.bench.SolrGenerate;
import org.quicktheories.api.AsString;
import org.quicktheories.core.Gen;

public class ArraysDSL {

  /**
   * Creates an ArrayGeneratorBuilder of Integers that can be used to create an array Source
   *
   * @param source a Source of type Integer
   * @return an ArrayGeneratorBuilder of type Integer
   */
  public ArrayGeneratorBuilder<Integer> ofIntegers(Gen<Integer> source) {
    return new ArrayGeneratorBuilder<>(source, Integer.class);
  }

  /**
   * Creates an ArrayGeneratorBuilder of Characters that can be used to create an array Source
   *
   * @param source a Source of type Character
   * @return an ArrayGeneratorBuilder of type Character
   */
  public ArrayGeneratorBuilder<Character> ofCharacters(Gen<Character> source) {
    return new ArrayGeneratorBuilder<>(source, Character.class);
  }

  /**
   * Creates an ArrayGeneratorBuilder of Strings that can be used to create an array Source
   *
   * @param source a Source of type String
   * @return an ArrayGeneratorBuilder of type String
   */
  public ArrayGeneratorBuilder<String> ofStrings(Gen<String> source) {
    return new ArrayGeneratorBuilder<>(source, String.class);
  }

  /**
   * Creates an ArrayGeneratorBuilder of the given class that can be used to create an array Source
   *
   * @param <T> type of value to generate
   * @param source a Source of type T
   * @param c a Class of type T
   * @return an ArrayGeneratorBuilder of type T
   */
  public <T> ArrayGeneratorBuilder<T> ofClass(Gen<T> source, Class<T> c) {
    return new ArrayGeneratorBuilder<>(source, c);
  }

  public static class ArrayGeneratorBuilder<T> {

    private final Gen<T> source;
    private final Class<T> c;

    ArrayGeneratorBuilder(Gen<T> source, Class<T> c) {
      this.source = source;
      this.c = c;
    }

    /**
     * Generates arrays of specified type T of fixed length
     *
     * @param length - fixed length
     * @return a Source of type T[]
     */
    public Gen<T[]> withLength(int length) {
      return withLengthBetween(length, length);
    }

    @SuppressWarnings("unchecked")
    public Gen<T[]> withLengths(Gen<Integer> lengths) {
      return Lists.listsOf(source, Lists.arrayList(), lengths)
          .map(
              l -> l.toArray((T[]) Array.newInstance(c, 0)) // will generate
              // correct size if
              // zero is less than
              // the length of the
              // array
              )
          .describedAs(arrayDescriber(source::asString));
    }

    /**
     * Generates arrays of specified type T of length bounded inclusively between minimumSize and
     * maximumSize
     *
     * @param minLength - the inclusive minimum size of the array
     * @param maxLength - the inclusive maximum size of the array
     * @return a Source of type T[]
     */
    public Gen<T[]> withLengthBetween(int minLength, int maxLength) {
      checkArguments(
          minLength <= maxLength,
          "The minLength (%s) is longer than the maxLength(%s)",
          minLength,
          maxLength);
      checkArguments(
          minLength >= 0,
          "The length of an array cannot be negative; %s is not an accepted argument",
          minLength);
      return withLengths(SolrGenerate.range(minLength, maxLength));
    }

    private static <T> AsString<T[]> arrayDescriber(Function<T, String> valueDescriber) {
      return a ->
          java.util.Arrays.stream(a)
              .map(valueDescriber)
              .collect(Collectors.joining(", ", "[", "]"));
    }
  }
}
