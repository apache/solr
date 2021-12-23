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

import java.io.IOException;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.TopDocs;

/**
 * This class is a variant of Lucene's {@link Rescorer} class for use by the
 * {@link ReRankCollector} to rescore all results in a {@link TopDocs} object
 * from an original query.
 */
public abstract class ReRankRescorer extends Rescorer {

  /**
   * Rescore an initial first-pass {@link TopDocs}.
   *
   * @param searcher {@link IndexSearcher} used to produce the first pass topDocs
   * @param firstPassTopDocs Hits from the first pass search that are to be rescored.
   *     It's very important that these hits were produced by the provided searcher;
   *     otherwise the doc IDs will not match!
   */
  public abstract TopDocs rescore(IndexSearcher searcher, TopDocs firstPassTopDocs)
      throws IOException;

  /**
   * Throws an {@link UnsupportedOperationException} exception.
   * Use {@link #rescore(IndexSearcher, TopDocs)} instead.
   */
  final public TopDocs rescore(IndexSearcher searcher, TopDocs firstPassTopDocs, int topN)
      throws IOException
  {
    throw new UnsupportedOperationException();
  }

}
