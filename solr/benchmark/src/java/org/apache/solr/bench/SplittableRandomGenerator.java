/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.bench;

import java.util.SplittableRandom;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.RandomGeneratorFactory;
import org.apache.commons.math3.util.FastMath;

/** Extension of {@code java.util.SplittableRandom} to implement {@link RandomGenerator}. */
public class SplittableRandomGenerator implements RandomGenerator {

  /** Serializable version identifier. */
  private static final long serialVersionUID = -7745277476784028798L;

  private SplittableRandom random;
  private double nextGaussian;

  /**
   * Instantiates a new Splittable random generator.
   *
   * @param random the random
   */
  public SplittableRandomGenerator(SplittableRandom random) {
    this.random = random;
  }

  /**
   * Instantiates a new Splittable random generator.
   *
   * @param seed the seed
   */
  public SplittableRandomGenerator(long seed) {
    this.random = new SplittableRandom(seed);
  }

  @Override
  public void setSeed(int seed) {
    setSeed((long) seed);
  }

  @Override
  public void setSeed(int[] seed) {
    setSeed(RandomGeneratorFactory.convertToLong(seed));
  }

  @Override
  public void setSeed(long seed) {
    this.random = new SplittableRandom(seed);
  }

  @Override
  public void nextBytes(byte[] bytes) {
    random.nextBytes(bytes);
  }

  @Override
  public int nextInt() {
    return random.nextInt();
  }

  @Override
  public int nextInt(int n) {
    return random.nextInt(n);
  }

  @Override
  public long nextLong() {
    return random.nextLong();
  }

  @Override
  public boolean nextBoolean() {
    return random.nextBoolean();
  }

  @Override
  public float nextFloat() {
    return (float) random.nextDouble();
  }

  @Override
  public double nextDouble() {
    return random.nextDouble();
  }

  @Override
  public double nextGaussian() {

    final double rnd;
    if (Double.isNaN(nextGaussian)) {
      // generate a new pair of gaussian numbers
      final double x = nextDouble();
      final double y = nextDouble();
      final double alpha = 2 * FastMath.PI * x;
      final double r = FastMath.sqrt(-2 * FastMath.log(y));
      rnd = r * FastMath.cos(alpha);
      nextGaussian = r * FastMath.sin(alpha);
    } else {
      // use the second element of the pair already generated
      rnd = nextGaussian;
      nextGaussian = Double.NaN;
    }

    return rnd;
  }
}
