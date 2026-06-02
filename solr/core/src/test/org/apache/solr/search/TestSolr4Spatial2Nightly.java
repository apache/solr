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
package org.apache.solr.search;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.apache.lucene.tests.util.LuceneTestCase.Nightly;
import org.junit.Test;

/**
 * Nightly variant of {@link TestSolr4Spatial2} that runs {@code
 * testLLPDecodeIsStableAndPrecise} the full 10 iterations.
 */
@Nightly
public class TestSolr4Spatial2Nightly extends TestSolr4Spatial2 {

  @Test
  @Repeat(iterations = 10)
  @Override
  public void testLLPDecodeIsStableAndPrecise() throws Exception {
    super.testLLPDecodeIsStableAndPrecise();
  }
}
