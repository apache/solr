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
package org.quicktheories.impl;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.solr.bench.SolrRandomnessSource;
import org.apache.solr.bench.generators.Distribution;
import org.quicktheories.core.DetatchedRandomnessSource;

/** The type Benchmark random source. */
public class BenchmarkRandomSource
    implements DetatchedRandomnessSource, ExtendedRandomnessSource, SolrRandomnessSource {
  private final RandomGenerator random;
  private final RandomDataGenerator rdg;

  private org.apache.solr.bench.generators.Distribution distribution = Distribution.UNIFORM;

  /**
   * Instantiates a new Benchmark random source.
   *
   * @param random the random
   */
  public BenchmarkRandomSource(RandomGenerator random) {
    this.random = random;
    rdg = new RandomDataGenerator(random);
  }

  private BenchmarkRandomSource(
      RandomGenerator random,
      RandomDataGenerator rdg,
      org.apache.solr.bench.generators.Distribution distribution) {
    this.random = random;
    this.rdg = rdg;
    this.distribution = distribution;
  }

  @Override
  public BenchmarkRandomSource withDistribution(Distribution distribution) {
    if (this.distribution == distribution) {
      return this;
    }
    return new BenchmarkRandomSource(random, rdg, distribution);
  }

  @Override
  public long next(long min, long max) {
    switch (distribution) {
      case UNIFORM:
        return rdg.nextLong(min, max);
      case ZIPFIAN:
        return rdg.nextZipf((int) (max - min), 2) + min - 1;
      case GAUSSIAN:
        return (int) BenchmarkRandomSource.normalize(rdg.nextGaussian(.5, .125), min, max - 1.0d);
      default:
        throw new IllegalStateException("Unknown distribution: " + distribution);
    }
  }

  @Override
  public long next(Constraint constraints) {
    if (constraints.min() == constraints.max()) {
      throw new RuntimeException(constraints.min() + " " + constraints.max());
    }
    return next(constraints.min(), constraints.max());
  }

  /**
   * Normalize double.
   *
   * @param value the value
   * @param normalizationLowerBound the normalization lower bound
   * @param normalizationUpperBound the normalization upper bound
   * @return the double
   */
  public static double normalize(
      double value, double normalizationLowerBound, double normalizationUpperBound) {
    double boundedValue = boundValue(value);
    // normalize boundedValue to new range
    double normalizedRange = normalizationUpperBound - normalizationLowerBound;
    return ((boundedValue - 0) * normalizedRange) + normalizationLowerBound;
  }

  private static double boundValue(double value) {
    double boundedValue = value;
    if (value < 0) {
      boundedValue = 0;
    }
    if (value > 1) {
      boundedValue = 1;
    }
    return boundedValue;
  }

  @Override
  public DetatchedRandomnessSource detach() {
    return this;
  }

  @Override
  public void registerFailedAssumption() {}

  @Override
  public long tryNext(Constraint constraints) {
    return next(constraints);
  }

  @Override
  public void add(Precursor other) {}

  @Override
  public void commit() {}
}
