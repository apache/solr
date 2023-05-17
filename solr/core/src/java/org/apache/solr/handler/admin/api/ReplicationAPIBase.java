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
package org.apache.solr.handler.admin.api;

import java.io.IOException;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/** A common parent for "replication" (i.e. replication-level) APIs. */
public abstract class ReplicationAPIBase extends JerseyResource {

  protected final SolrCore solrCore;
  protected final SolrQueryRequest solrQueryRequest;
  protected final SolrQueryResponse solrQueryResponse;

  public ReplicationAPIBase(
      SolrCore solrCore, SolrQueryRequest solrQueryRequest, SolrQueryResponse solrQueryResponse) {
    this.solrCore = solrCore;
    this.solrQueryRequest = solrQueryRequest;
    this.solrQueryResponse = solrQueryResponse;
  }

  protected CoreReplicationAPI.IndexVersionResponse doFetchIndexVersion() throws IOException {

    ReplicationHandler replicationHandler =
        (ReplicationHandler) solrCore.getRequestHandler(ReplicationHandler.PATH);

    return replicationHandler.getIndexVersionResponse();
  }
}
