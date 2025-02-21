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

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

/**
 * Detects whether a query can be run using a standard Lucene {@link
 * org.apache.lucene.search.IndexSearcher}
 *
 * <p>Some Solr {@link Query} implementations are written to assume access to a {@link
 * SolrIndexSearcher}. But these objects aren't always available: some code-paths (e.g. when
 * executing a "delete-by-query") execute the query using the standard {@link
 * org.apache.lucene.search.IndexSearcher} available in Lucene. This {@link QueryVisitor} allows
 * code to detect whether a given Query requires SolrIndexSearcher or not.
 *
 * <p>Instances should not be reused for multiple query-tree inspections.
 *
 * @see SolrSearcherRequirer
 * @lucene.experimental
 */
public class SolrSearcherRequirementDetector extends QueryVisitor {

  private boolean requiresSolrSearcher = false;

  @Override
  public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
    // This method is primarily intended to swap out visitors when descending through the Query
    // tree, but it's also the only place to put visiting logic for non-leaf nodes, since the
    // QueryVisitor interface largely assumes that only leaf-nodes are worth visiting.  See
    // LUCENE-????? for more details - this can be restructured if that ticket is ever addressed.
    if (parent instanceof SolrSearcherRequirer) {
      requiresSolrSearcher = true;
    }

    return this;
  }

  @Override
  public void visitLeaf(Query query) {
    if (query instanceof SolrSearcherRequirer) {
      requiresSolrSearcher = true;
    }
  }

  public boolean getRequiresSolrSearcher() {
    return requiresSolrSearcher;
  }
}
