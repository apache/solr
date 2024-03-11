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

public class ExponentialDecayFunctionValueSourceParser extends DecayFunctionValueSourceParser {
  @Override
  DecayStrategy getDecayStrategy() {
    return new ExponentialDecay();
  }

  @Override
  String name() {
    return "exp";
  }
}

final class ExponentialDecay implements DecayStrategy {

  @Override
  public double scale(double scale, double decay) {
    return Math.log(decay) / scale;
  }

  @Override
  public double calculate(double value, double scale) {
    return Math.exp(scale * value);
  }

  @Override
  public String explain(double scale) {
    return "exp(- <val> * " + -1 * scale + ")";
  }
}
