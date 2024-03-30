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

package org.apache.solr.monitor;

import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.RTimerTree;

/**
 * Fake Solr request used only in some places which need Solr Requests, but not all methods are
 * needed.
 */
public class SimpleQueryParser implements SolrQueryRequest {

  private static final SolrParams EMPTY = new MapSolrParams(Map.of());

  private final SolrCore solrCore;
  private final Map<Object, Object> context;
  private final IndexSchema schema;
  private final RTimerTree timerTree;

  private SimpleQueryParser(SolrCore solrCore) {
    this.solrCore = solrCore;
    this.schema = solrCore.getLatestSchema();
    context = new HashMap<>();
    this.timerTree = new RTimerTree();
  }

  public static Query parse(String queryStr, SolrCore core) {
    try {
      return QParser.getParser(queryStr, new SimpleQueryParser(core)).parse();
    } catch (SyntaxError e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "could not parse query");
    }
  }

  @Override
  public SolrParams getParams() {
    return EMPTY;
  }

  @Override
  public void setParams(SolrParams params) {
    throw new UnsupportedOperationException("SolrQueryRequest:: setParams Not supported.");
  }

  @Override
  public Iterable<ContentStream> getContentStreams() {
    throw new UnsupportedOperationException("SolrQueryRequest::getContentStreams Not supported.");
  }

  @Override
  public SolrParams getOriginalParams() {
    return EMPTY;
  }

  @Override
  public Map<Object, Object> getContext() {
    return context;
  }

  @Override
  public void close() {
    /* no-op */
  }

  protected final long startTime = System.nanoTime();

  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public RTimerTree getRequestTimer() {
    return timerTree;
  }

  @Override
  public SolrIndexSearcher getSearcher() {
    return solrCore.getSearcher().get();
  }

  @Override
  public SolrCore getCore() {
    return solrCore;
  }

  @Override
  public IndexSchema getSchema() {
    return schema;
  }

  @Override
  public void updateSchemaToLatest() {
    throw new UnsupportedOperationException(
        "SolrQueryRequest::updateSchemaToLatest Not supported.");
  }

  @Override
  public String getParamString() {
    throw new UnsupportedOperationException("SolrQueryRequest::getParamString Not supported.");
  }

  @Override
  public Map<String, Object> getJSON() {
    throw new UnsupportedOperationException("SolrQueryRequest::getJSON Not supported.");
  }

  @Override
  public void setJSON(Map<String, Object> json) {
    throw new UnsupportedOperationException("SolrQueryRequest::setJSON Not supported.");
  }

  @Override
  public Principal getUserPrincipal() {
    throw new UnsupportedOperationException("SolrQueryRequest::getUserPrincipal Not supported.");
  }
}
