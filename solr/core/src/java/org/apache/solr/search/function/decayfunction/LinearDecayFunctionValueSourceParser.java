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

public class LinearDecayFunctionValueSourceParser extends DecayFunctionValueSourceParser {

  @Override
  DecayStrategy getDecayStrategy() {
    return new LinearDecay();
  }

  @Override
  String name() {
    return "linear";
  }
}

final class LinearDecay implements DecayStrategy {

  @Override
  public double scale(double scale, double decay) {
    return scale / (1.0 - decay);
  }

  @Override
  public double calculate(double value, double scale) {
    return Math.max(0.0, (scale - value) / scale);
  }

  @Override
  public String explain(double scale) {
    return "max(0.0, ((" + scale + " - <val>)/" + scale + ")";
  }
}
