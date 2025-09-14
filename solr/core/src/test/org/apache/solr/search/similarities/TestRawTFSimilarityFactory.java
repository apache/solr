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
package org.apache.solr.search.similarities;

import org.apache.lucene.search.similarities.RawTFSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.junit.BeforeClass;

/** Tests {@link RawTFSimilarityFactory} */
public class TestRawTFSimilarityFactory extends BaseSimilarityTestCase {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml", "schema-rawtf.xml");
  }

  public void test() {
    Similarity sim = getSimilarity("text");
    assertEquals(RawTFSimilarity.class, sim.getClass());
    RawTFSimilarity rtfSim = (RawTFSimilarity) sim;
    assertTrue(rtfSim.getDiscountOverlaps());
  }

  public void testParameters0() {
    Similarity sim = getSimilarity("text_params_0");
    assertEquals(RawTFSimilarity.class, sim.getClass());
    RawTFSimilarity rtfSim = (RawTFSimilarity) sim;
    assertFalse(rtfSim.getDiscountOverlaps());
  }

  public void testParameters1() {
    Similarity sim = getSimilarity("text_params_1");
    assertEquals(RawTFSimilarity.class, sim.getClass());
    RawTFSimilarity rtfSim = (RawTFSimilarity) sim;
    assertTrue(rtfSim.getDiscountOverlaps());
  }
}
