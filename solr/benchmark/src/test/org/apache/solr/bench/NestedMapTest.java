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

import static org.apache.solr.bench.generators.SourceDSL.dates;
import static org.apache.solr.bench.generators.SourceDSL.doubles;
import static org.apache.solr.bench.generators.SourceDSL.floats;
import static org.apache.solr.bench.generators.SourceDSL.integers;
import static org.apache.solr.bench.generators.SourceDSL.longs;
import static org.apache.solr.bench.generators.SourceDSL.maps;
import static org.apache.solr.bench.generators.SourceDSL.strings;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.bench.generators.LazyGen;
import org.apache.solr.bench.generators.NamedListGen;
import org.apache.solr.bench.generators.SolrGen;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.quicktheories.api.Pair;
import org.quicktheories.core.Gen;
import org.quicktheories.impl.BenchmarkRandomSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NestedMapTest extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("random.counts", "true");
  }

  @AfterClass
  public static void afterClass() throws InterruptedException {
    SolrGen.countsReport().forEach(log::info);
    SolrGen.COUNTS.clear();
  }

  @Test
  public void testNestedMap() throws Exception {
    Gen<? extends Map<String, ?>> mapGen = maps().of(getKey(), getValue(10)).ofSizeBetween(1, 300);

    Map<String, ?> map =
        mapGen.generate(
            new BenchmarkRandomSource(
                new SplittableRandomGenerator(BaseBenchState.getRandomSeed())));
    //    if (log.isInfoEnabled()) {
    //      log.info("map={}", map);
    //    }
  }

  private static SolrGen<String> getKey() {
    return strings().betweenCodePoints('a', 'z' + 1).ofLengthBetween(1, 10);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static SolrGen<?> getValue(int depth) {
    if (depth == 0) {
      return integers().from(1).upToAndIncluding(5000);
    }
    List values = new ArrayList(4);
    values.add(
        Pair.of(
            5, maps().of(getKey(), new LazyGen(() -> getValue(depth - 1))).ofSizeBetween(1, 25)));
    values.add(
        Pair.of(
            5,
            new NamedListGen(
                maps().of(getKey(), new LazyGen(() -> getValue(depth - 1))).ofSizeBetween(1, 35))));
    values.add(Pair.of(15, integers().all()));
    values.add(Pair.of(14, longs().all()));
    values.add(Pair.of(13, doubles().all()));
    values.add(Pair.of(16, floats().all()));
    values.add(Pair.of(17, dates().all()));
    return SolrGenerate.frequency(values);
  }
}
