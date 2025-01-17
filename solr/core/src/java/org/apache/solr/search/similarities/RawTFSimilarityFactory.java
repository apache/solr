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
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.schema.SimilarityFactory;

/**
 * Factory for RawTFSimilarity.
 *
 * <p>Parameters:
 *
 * <ul>
 *   <li>discountOverlaps (bool): True if overlap tokens (tokens with a position of increment of
 *       zero) are discounted from the document's length. The default is <code>true</code>
 * </ul>
 *
 * @lucene.experimental
 * @since 9.9.0
 */
public class RawTFSimilarityFactory extends SimilarityFactory {
  private RawTFSimilarity similarity;

  @Override
  public void init(SolrParams params) {
    super.init(params);
    boolean discountOverlaps = params.getBool("discountOverlaps", true);
    similarity = new RawTFSimilarity(discountOverlaps);
  }

  @Override
  public Similarity getSimilarity() {
    return similarity;
  }
}
