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
package org.apache.solr.search.vector;

import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnSearchStrategy;

public class SolrKnnFloatVectorQuery extends KnnFloatVectorQuery {
  private final int topK;

  public SolrKnnFloatVectorQuery(
      String field, float[] target, int topK, int efSearch, Query filter) {
    // efSearch is used as 'k' to explore this many vectors in HNSW then topK results are returned
    // to the user
    super(field, target, efSearch, filter);
    this.topK = topK;
  }

  public SolrKnnFloatVectorQuery(
      String field,
      float[] target,
      int topK,
      int efSearch,
      Query filter,
      KnnSearchStrategy searchStrategy) {
    super(field, target, efSearch, filter, searchStrategy);
    this.topK = topK;
  }

  @Override
  protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
    return TopDocs.merge(topK, perLeafResults);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!super.equals(obj)) return false;
    if (getClass() != obj.getClass()) return false;
    SolrKnnFloatVectorQuery other = (SolrKnnFloatVectorQuery) obj;
    return this.topK == other.topK;
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + Integer.hashCode(topK);
  }
}
