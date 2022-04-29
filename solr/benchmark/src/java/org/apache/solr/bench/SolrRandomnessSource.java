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

import org.apache.solr.bench.generators.Distribution;
import org.quicktheories.core.RandomnessSource;

/** The interface Solr randomness source. */
public interface SolrRandomnessSource extends RandomnessSource {
  /**
   * Next long.
   *
   * @param min the min
   * @param max the max
   * @return the long
   */
  long next(long min, long max);

  /**
   * With distribution solr randomness source.
   *
   * @param distribution the distribution
   * @return the solr randomness source
   */
  SolrRandomnessSource withDistribution(Distribution distribution);
}
