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

package org.apache.solr.savedsearch;

import java.io.IOException;
import java.util.Map;
import org.apache.lucene.monitor.MonitorQuery;
import org.apache.lucene.monitor.QueryDecomposer;
import org.apache.lucene.monitor.Visitors.QCEVisitor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.savedsearch.search.ReverseSearchComponent;

public class SavedSearchDecoder {

  private final SolrCore core;
  private final QueryDecomposer queryDecomposer;

  public SavedSearchDecoder(SolrCore core) {
    this.core = core;
    ReverseSearchComponent rsc =
        (ReverseSearchComponent)
            core.getSearchComponents().get(ReverseSearchComponent.COMPONENT_NAME);
    this.queryDecomposer = rsc.getQueryDecomposer();
  }

  private MonitorQuery decode(SavedSearchDataValues savedSearchDataValues) throws IOException {
    String id = savedSearchDataValues.getQueryId();
    String queryStr = savedSearchDataValues.getMq();
    var query = SimpleQueryParser.parse(queryStr, core);
    return new MonitorQuery(id, query, queryStr, Map.of());
  }

  public QCEVisitor getComponent(SavedSearchDataValues dataValues, String cacheId)
      throws IOException {
    for (QCEVisitor qce : QCEVisitor.decompose(decode(dataValues), queryDecomposer)) {
      if (qce.getCacheId().equals(cacheId)) {
        return qce;
      }
    }
    throw new IllegalArgumentException("Corrupt monitorQuery value in index");
  }
}
