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
package org.apache.solr.response;

import org.apache.lucene.search.Query;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocList;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrIndexSearcher;

public class DistributedResultContext extends ResultContext {
  private ReturnFields returnFields;
  private SolrQueryRequest req;

  public DistributedResultContext(
      ReturnFields returnFields,
      SolrQueryRequest req) {
    this.returnFields = returnFields;
    this.req = req;
  }

  @Override
  public DocList getDocList() {
    return null;
  }

  @Override
  public ReturnFields getReturnFields() {
    return returnFields;
  }

  @Override
  public SolrIndexSearcher getSearcher() {
    return null;
  }

  @Override
  public Query getQuery() {
    return null;
  }

  @Override
  public SolrQueryRequest getRequest() {
    return req;
  }

  @Override
  public boolean wantsScores() {
    return getReturnFields().wantsScore();
  }
}
