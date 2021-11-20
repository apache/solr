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
package org.apache.solr.bench;

import static org.apache.solr.bench.generators.SourceDSL.checkArguments;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Predicate;
import org.apache.solr.bench.generators.SolrGen;
import org.quicktheories.api.Pair;
import org.quicktheories.core.DetatchedRandomnessSource;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;

/** The type Solr generate. */
public class SolrGenerate {

  private SolrGenerate() {}

  /**
   * Constant solr gen.
   *
   * @param <T> the type parameter
   * @param constant the constant
   * @return the solr gen
   */
  public static <T> SolrGen<T> constant(T constant) {
    return new SolrGen<>() {
      @Override
      public T generate(RandomnessSource in) {
        return constant;
      }

      @Override
      public T generate(SolrRandomnessSource in) {
        return constant;
      }
    };
  }

  /**
   * Range solr gen.
   *
   * @param startInclusive the start inclusive
   * @param endInclusive the end inclusive
   * @return the solr gen
   */
  public static SolrGen<Integer> range(final int startInclusive, final int endInclusive) {
    return new SolrGen<>() {
      {
        this.start = startInclusive;
        this.end = endInclusive;
      }

      @Override
      public Integer generate(RandomnessSource in) {
        return (int) ((SolrRandomnessSource) in).next(startInclusive, endInclusive);
      }

      @Override
      public Integer generate(SolrRandomnessSource in) {
        return (int) in.next(startInclusive, endInclusive);
      }
    };
  }

  /**
   * Long range solr gen.
   *
   * @param startInclusive the start inclusive
   * @param endInclusive the end inclusive
   * @return the solr gen
   */
  public static SolrGen<Long> longRange(final long startInclusive, final long endInclusive) {
    return new SolrGen<>() {
      {
        this.start = startInclusive;
        this.end = endInclusive;
      }

      @Override
      public Long generate(RandomnessSource in) {
        return ((SolrRandomnessSource) in).next(startInclusive, endInclusive);
      }

      @Override
      public Long generate(SolrRandomnessSource in) {
        return in.next(startInclusive, endInclusive);
      }
    };
  }

  /**
   * Int arrays solr gen.
   *
   * @param sizes the sizes
   * @param contents the contents
   * @return the solr gen
   */
  public static SolrGen<int[]> intArrays(SolrGen<Integer> sizes, SolrGen<Integer> contents) {
    return new SolrGen<>() {

      @Override
      public int[] generate(SolrRandomnessSource td) {
        int size = sizes.generate(td);
        int[] is = new int[size];
        for (int i = 0; i != size; i++) {
          is[i] = contents.generate(td);
        }
        return is;
      }
    };
  }

  /**
   * Code points solr gen.
   *
   * @param startInclusive the start inclusive
   * @param endInclusive the end inclusive
   * @return the solr gen
   */
  public static SolrGen<Integer> codePoints(int startInclusive, int endInclusive) {

    checkArguments(
        startInclusive >= Character.MIN_CODE_POINT,
        "(%s) is less than the minimum codepoint (%s)",
        startInclusive,
        Character.MIN_CODE_POINT);

    checkArguments(
        endInclusive <= Character.MAX_CODE_POINT,
        "%s is greater than the maximum codepoint (%s)",
        endInclusive,
        Character.MAX_CODE_POINT);

    return new Retry<>(SolrGenerate.range(startInclusive, endInclusive), Character::isDefined);
  }

  /**
   * Returns a generator that provides a value from a generator chosen with probability in
   * proportion to the weight supplied in the {@link Pair}. Shrinking is towards the first non-zero
   * weight in the list. At least one generator must have a positive weight and non-positive
   * generators will never be chosen.
   *
   * @param <T> Type to generate
   * @param mandatory Generator to sample from
   * @param others Other generators to sample
   * @return A gen of T
   */
  @SafeVarargs
  public static <T> SolrGen<T> frequency(
      Pair<Integer, SolrGen<T>> mandatory, Pair<Integer, SolrGen<T>>... others) {

    return frequency(SolrGenerate.FrequencyGen.makeGeneratorList(mandatory, others));
  }

  /**
   * Returns a generator that provides a value from a generator chosen with probability in
   * proportion to the weight supplied in the {@link Pair}. Shrinking is towards the first non-zero
   * weight in the list. At least one generator must have a positive weight and non-positive
   * generators will never be chosen.
   *
   * @param <T> Type to generate
   * @param weightedGens pairs of weight and generators to sample in proportion to their weighting
   * @return A gen of T
   */
  public static <T> SolrGen<T> frequency(List<Pair<Integer, SolrGen<T>>> weightedGens) {
    return SolrGenerate.FrequencyGen.fromList(weightedGens);
  }

  /**
   * Booleans solr gen.
   *
   * @return the solr gen
   */
  public static SolrGen<Boolean> booleans() {
    return SolrGenerate.pick(Arrays.asList(false, true));
  }

  /**
   * Randomly returns one of the supplied values
   *
   * @param <T> type of value to generate *
   * @param ts Values to pick from
   * @return A Gen of T
   */
  public static <T> SolrGen<T> pick(List<T> ts) {
    Gen<Integer> index = range(0, ts.size() - 1);
    return new SolrGen<>(prng -> ts.get(index.generate(prng)), null);
  }

  /**
   * Returns a generator that provides a value from a random generator provided.
   *
   * @param <T> Type to generate
   * @param mandatory Generator to sample from
   * @param others Other generators to sample from with equal weighting
   * @return A gen of T
   */
  @SafeVarargs
  public static <T> SolrGen<T> oneOf(SolrGen<T> mandatory, SolrGen<T>... others) {
    SolrGen<T>[] generators = Arrays.copyOf(others, others.length + 1);
    generators[generators.length - 1] = mandatory;
    SolrGen<Integer> index = range(0, generators.length - 1);
    return new SolrGen<>(prng -> generators[(index.generate(prng))].generate(prng), null);
  }

  /**
   * The type Frequency gen.
   *
   * @param <T> the type parameter
   */
  static class FrequencyGen<T> extends SolrGen<T> {
    private final NavigableMap<Integer, SolrGen<T>> weightedMap;
    private final SolrGen<Integer> indexGen;

    private FrequencyGen(SolrGen<Integer> indexGen, NavigableMap<Integer, SolrGen<T>> weightedMap) {
      this.weightedMap = weightedMap;
      this.indexGen = indexGen;
    }

    /**
     * Make generator list list.
     *
     * @param <T> the type parameter
     * @param mandatory the mandatory
     * @param others the others
     * @return the list
     */
    static <T> List<Pair<Integer, SolrGen<T>>> makeGeneratorList(
        Pair<Integer, SolrGen<T>> mandatory, Pair<Integer, SolrGen<T>>[] others) {
      List<Pair<Integer, SolrGen<T>>> ts = new ArrayList<>(others.length + 1);
      ts.add(mandatory);
      Collections.addAll(ts, others);
      return ts;
    }

    /**
     * From list solr generate . frequency gen.
     *
     * @param <T> the type parameter
     * @param ts the ts
     * @return the solr generate . frequency gen
     */
    /* First the generator normalizes the weights and their total by the greatest common factor
     * to keep integers small and improve the chance of moving to a new generator as the
     * index generator shrinks.
     *
     * Then assigns each generator a range of integers according to their normalized weight
     * For example, with three normalized weighted generators {3, g1}, {4, g2}, {5, g3},
     * total 12 it assigns [0, 2] to g1, [3, 6] to g2, [7, 11] to g3.
     *
     * At generation time, the generator picks an integer between [0, total weight) and finds
     * the generator responsible for the range.
     */
    static <T> SolrGenerate.FrequencyGen<T> fromList(List<Pair<Integer, SolrGen<T>>> ts) {
      if (ts.isEmpty()) {
        throw new IllegalArgumentException("List of generators must not be empty");
      }
      /* Calculate the total unadjusted weights, and the largest common factor
       * between all the weights and the total, so they can be reduced to the smallest.
       * It ignores non-positive weights to make it easy to disable generators while developing
       * properties.
       */
      long unadjustedTotalWeights = 0;
      long commonFactor = 0;
      for (Pair<Integer, SolrGen<T>> pair : ts) {
        int weight = pair._1;
        if (weight <= 0) continue;

        if (unadjustedTotalWeights == 0) {
          commonFactor = weight;
        } else {
          commonFactor = gcd(commonFactor, weight);
        }
        unadjustedTotalWeights += weight;
      }
      if (unadjustedTotalWeights == 0) {
        throw new IllegalArgumentException("At least one generator must have a positive weight");
      }
      commonFactor = gcd(commonFactor, unadjustedTotalWeights);

      /* Build a tree map with the key as the first integer assigned in the range,
       * floorEntry will pick the 'the greatest key less than or equal to the given key',
       * which will find the right generator for the range.
       */
      NavigableMap<Integer, SolrGen<T>> weightedMap = new TreeMap<>();
      int nextStart = 0;
      for (Pair<Integer, SolrGen<T>> pair : ts) {
        int weight = pair._1;
        if (weight <= 0) continue;
        try {
          weightedMap.put(nextStart, pair._2);
        } catch (ClassCastException e) {
          throw new ClassCastException(
              "Class cast exception for " + pair._1 + ',' + pair._2 + ' ' + e.getMessage());
        }
        nextStart += weight / commonFactor;
      }
      final int upperRange = (int) (unadjustedTotalWeights / commonFactor) - 1;

      SolrGen<Integer> indexGen = range(0, upperRange);

      return new SolrGenerate.FrequencyGen<>(indexGen, weightedMap);
    }

    @Override
    public T generate(SolrRandomnessSource prng) {
      return weightedMap.floorEntry(indexGen.generate(prng)).getValue().generate(prng);
    }

    @Override
    public String asString(T t) {
      return weightedMap.get(0).asString(t);
    }

    private static long gcd(long a, long b) {
      return BigInteger.valueOf(a).gcd(BigInteger.valueOf(b)).longValue();
    }
  }
}

/**
 * The type Retry.
 *
 * @param <T> the type parameter
 */
class Retry<T> extends SolrGen<T> {

  private final Predicate<T> assumption;

  /**
   * Instantiates a new Retry.
   *
   * @param child the child
   * @param assumption the assumption
   */
  Retry(SolrGen<T> child, Predicate<T> assumption) {
    super(child);
    this.assumption = assumption;
  }

  @Override
  public T generate(RandomnessSource in) {
    // danger of infinite loop here but space is densely populated enough for
    // this to be unlikely
    while (true) {
      DetatchedRandomnessSource detatched = in.detach();
      T t = Objects.requireNonNull(child).generate(detatched);
      if (assumption.test(t)) {
        detatched.commit();
        return t;
      }
    }
  }

  @Override
  public String asString(T t) {
    return Objects.requireNonNull(child).asString(t);
  }
}
