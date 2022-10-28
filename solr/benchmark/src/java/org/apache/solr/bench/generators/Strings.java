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

import java.util.function.Function;
import org.apache.solr.bench.SolrGenerate;

/** The type Strings. */
final class Strings {

  /** The constant BOUNDED_NUMERIC_STRING. */
  public static final String BOUNDED_NUMERIC_STRING = "Bounded Numeric String";

  /** The constant POINTS_FROM_CODE_POINT_RANGE. */
  public static final String POINTS_FROM_CODE_POINT_RANGE = "PointsFromCodePointRange";

  /** The constant BOUNDED_LENGTH_STRING. */
  public static final String BOUNDED_LENGTH_STRING = "Bounded Length String";

  private Strings() {}

  /**
   * Bounded numeric strings solr gen.
   *
   * @param startInclusive the start inclusive
   * @param endInclusive the end inclusive
   * @return the solr gen
   */
  static SolrGen<String> boundedNumericStrings(int startInclusive, int endInclusive) {
    return new SolrGen<>(
            SolrGenerate.range(startInclusive, endInclusive).map(Object::toString), String.class)
        .describedAs(BOUNDED_NUMERIC_STRING);
  }

  /**
   * With code points solr gen.
   *
   * @param minCodePoint the min code point
   * @param maxCodePoint the max code point
   * @param numberOfCodePoints the number of code points
   * @return the solr gen
   */
  static SolrGen<String> withCodePoints(
      int minCodePoint, int maxCodePoint, SolrGen<Integer> numberOfCodePoints) {

    return new SolrGen<>(
            SolrGenerate.intArrays(
                    numberOfCodePoints, SolrGenerate.codePoints(minCodePoint, maxCodePoint))
                .map(is -> new String(is, 0, is.length)),
            String.class)
        .describedAs(POINTS_FROM_CODE_POINT_RANGE);
  }

  /**
   * Of bounded length strings solr gen.
   *
   * @param minCodePoint the min code point
   * @param maxCodePoint the max code point
   * @param minLength the min length
   * @param maxLength the max length
   * @return the solr gen
   */
  static SolrGen<String> ofBoundedLengthStrings(
      int minCodePoint, int maxCodePoint, int minLength, int maxLength) {

    // generate strings of fixed number of code points then modify any that exceed max length
    return new SolrGen<>(
            withCodePoints(minCodePoint, maxCodePoint, SolrGenerate.range(minLength, maxLength))
                .map(reduceToSize(maxLength)),
            String.class)
        .describedAs(BOUNDED_LENGTH_STRING);
  }

  private static Function<String, String> reduceToSize(int maxLength) {
    // Reduce size of string by removing characters from start
    return s -> {
      if (s.length() <= maxLength) {
        return s;
      }
      String t = s;
      while (true) {
        final int length = t.length();
        if (!(length > maxLength)) break;
        t = t.substring(1);
      }
      return t;
    };
  }
}
