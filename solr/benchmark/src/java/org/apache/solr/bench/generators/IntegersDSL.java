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

import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.bench.SolrGenerate;
import org.apache.solr.bench.SolrRandomnessSource;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.impl.SplittableRandomSource;

/** The type Integers dsl. */
public class IntegersDSL {

  /**
   * Constructs a IntegerDomainBuilder object with an inclusive lower bound
   *
   * @param startInclusive - lower bound of domain
   * @return an IntegerDomainBuilder
   */
  public IntegerDomainBuilder from(final int startInclusive) {
    return new IntegerDomainBuilder(startInclusive);
  }

  /**
   * Generates all possible integers in Java bounded below by Integer.MIN_VALUE and above by
   * Integer.MAX_VALUE.
   *
   * @return a Source of type Integer
   */
  public SolrGen<Integer> all() {
    return between(Integer.MIN_VALUE, Integer.MAX_VALUE).describedAs("All Ints");
  }

  /**
   * All with max cardinality solr gen.
   *
   * @param maxCardinality the max cardinality
   * @return the solr gen
   */
  public SolrGen<Integer> allWithMaxCardinality(int maxCardinality) {
    return between(Integer.MIN_VALUE, Integer.MAX_VALUE, maxCardinality)
        .describedAs("All Ints w/ Max Cardinality");
  }

  /**
   * Generates all possible positive integers in Java, bounded above by Integer.MAX_VALUE.
   *
   * @return a Source of type Integer
   */
  public SolrGen<Integer> allPositive() {
    return between(1, Integer.MAX_VALUE).describedAs("All Positive Ints");
  }

  /**
   * All positive with max cardinality solr gen.
   *
   * @param maxCardinality the max cardinality
   * @return the solr gen
   */
  public SolrGen<Integer> allPositiveWithMaxCardinality(int maxCardinality) {
    return between(1, Integer.MAX_VALUE, maxCardinality)
        .describedAs("All Positive Ints with Max Cardinality");
  }

  /**
   * Incrementing SolrGen. Always returns an integer greater than the previous one. You cannot count
   * on the increment being 1.
   *
   * @return a SolrGen that returns an int greater than the previous
   */
  public SolrGen<Integer> incrementing() {
    return new IncrementingIntegerSolrGen();
  }

  private static class IncrementingIntegerSolrGen extends SolrGen<Integer> {
    /** The Increment. */
    AtomicInteger increment = new AtomicInteger();

    /** Instantiates a new Incrementing integer solr gen. */
    public IncrementingIntegerSolrGen() {
      super(Integer.class);
      describedAs("Incrementing Int");
    }

    @Override
    public Integer generate(RandomnessSource in) {
      return increment.getAndIncrement();
    }

    @Override
    public Integer generate(SolrRandomnessSource in) {
      return increment.getAndIncrement();
    }
  }

  private static class IntegerMaxCardinalitySolrGen extends SolrGen<Integer> {
    private final int maxCardinality;
    private final Gen<Integer> integers;
    /** The Cardinality start. */
    Integer cardinalityStart;

    /**
     * Instantiates a new Integer max cardinality solr gen.
     *
     * @param maxCardinality the max cardinality
     * @param integers the integers
     */
    public IntegerMaxCardinalitySolrGen(int maxCardinality, Gen<Integer> integers) {
      this.maxCardinality = maxCardinality;
      this.integers = integers;
      this.describedAs("Max Cardinality Int");
    }

    @Override
    public Integer generate(SolrRandomnessSource in) {
      if (cardinalityStart == null) {
        cardinalityStart =
            SolrGenerate.range(0, Integer.MAX_VALUE - maxCardinality - 1).generate(in);
      }

      long seed =
          SolrGenerate.range(cardinalityStart, cardinalityStart + maxCardinality - 1).generate(in);
      return integers.generate(new SplittableRandomSource(new SplittableRandom(seed)));
    }
  }

  /** The type Integer domain builder. */
  public class IntegerDomainBuilder {

    private final int startInclusive;

    private int maxCardinality;

    private IntegerDomainBuilder(int startInclusive) {
      this.startInclusive = startInclusive;
    }

    /**
     * Max cardinality integer domain builder.
     *
     * @param max the max
     * @return the integer domain builder
     */
    public IntegerDomainBuilder maxCardinality(int max) {
      maxCardinality = max;
      return this;
    }

    /**
     * Generates integers within the interval specified with an inclusive lower and upper bound.
     *
     * @param endInclusive - inclusive upper bound of domain
     * @return a Source of type Integer
     */
    public SolrGen<Integer> upToAndIncluding(final int endInclusive) {
      return between(startInclusive, endInclusive, maxCardinality)
          .describedAs("Int UpToAndIncluding");
    }

    /**
     * Generates integers within the interval specified with an inclusive lower bound and exclusive
     * upper bound.
     *
     * @param endExclusive - exclusive upper bound of domain
     * @return a Source of type Integer
     */
    public SolrGen<Integer> upTo(final int endExclusive) {
      return between(startInclusive, endExclusive - 1, maxCardinality).describedAs("Int UpTo");
    }
  }

  /**
   * Generates Integers within the interval specified with an inclusive lower and upper bound.
   *
   * @param startInclusive - inclusive lower bound of domain
   * @param endInclusive - inclusive upper bound of domain
   * @return a Source of type Integer
   */
  public SolrGen<Integer> between(final int startInclusive, final int endInclusive) {
    return between(startInclusive, endInclusive, 0).describedAs("Int Between");
  }

  /**
   * Generates Integers within the interval specified with an inclusive lower and upper bound.
   *
   * @param startInclusive - inclusive lower bound of domain
   * @param endInclusive - inclusive upper bound of domain
   * @param maxCardinality the max cardinality
   * @return a Source of type Integer
   */
  public SolrGen<Integer> between(
      final int startInclusive, final int endInclusive, int maxCardinality) {
    checkArguments(
        startInclusive <= endInclusive,
        "There are no Integer values to be generated between (%s) and (%s)",
        startInclusive,
        endInclusive);
    Gen<Integer> integers = SolrGenerate.range(startInclusive, endInclusive);
    if (maxCardinality > 0) {
      return new SolrGen<>(
              new IntegerMaxCardinalitySolrGen(maxCardinality, integers), Integer.class)
          .describedAs("Int Between w/ MaxCardinality");
    } else {
      return new SolrGen<>(integers, Integer.class).describedAs("Int Between");
    }
  }
}
