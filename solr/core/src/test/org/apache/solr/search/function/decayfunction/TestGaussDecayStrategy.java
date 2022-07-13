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


package org.apache.solr.search.function.decayfunction;

import org.junit.Assert;
import org.junit.Test;

public class TestGaussDecayStrategy {

  @Test
  public void testGaussDecayFunctionValueSourceParser() {
    DecayFunctionValueSourceParser parser = new GaussDecayFunctionValueSourceParser();
    DecayStrategy strategy = parser.getDecayStrategy();
    for (double scale = 0; scale < 100; scale += 0.1)
      for (double decay = 0; decay < 1; decay += 0.01)
        for (double value = 0; value < 100; value += 0.1) test(scale, decay, value, strategy);
  }

  private void test(double scale, double decay, double value, DecayStrategy strategy) {
    double s = strategy.scale(scale, decay);
    Assert.assertEquals(0.5 * Math.pow(scale, 2.0) / Math.log(decay), s, 0);
    double v = strategy.calculate(value, s);
    Assert.assertEquals(Math.exp(0.5 * Math.pow(value, 2.0) / s), v, 0);
  }
}
