/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.solr.monitor.search;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryVisitor;
import org.apache.solr.search.DelegatingCollector;
import org.apache.solr.search.ExtendedQueryBase;
import org.apache.solr.search.PostFilter;

class MonitorPostFilter extends ExtendedQueryBase implements PostFilter {

  private final SolrMonitorQueryCollector.CollectorContext collectorContext;

  MonitorPostFilter(SolrMonitorQueryCollector.CollectorContext collectorContext) {
    this.collectorContext = collectorContext;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
    return new SolrMonitorQueryCollector(collectorContext);
  }

  @Override
  public boolean getCache() {
    return false;
  }

  @Override
  public int getCost() {
    // cost has to be >= 100 since we only support post filtering
    return Math.max(super.getCost(), 111);
  }

  @Override
  public boolean equals(Object o) {
    return this == o;
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }
}
