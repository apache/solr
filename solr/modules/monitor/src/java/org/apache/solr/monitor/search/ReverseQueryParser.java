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

import static org.apache.solr.monitor.MonitorConstants.DOCUMENT_BATCH_KEY;
import static org.apache.solr.monitor.MonitorConstants.SOLR_MONITOR_CACHE_NAME;

import java.util.function.BiPredicate;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.monitor.DocumentBatchVisitor;
import org.apache.lucene.monitor.Presearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.monitor.cache.MonitorQueryCache;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;

public class ReverseQueryParser extends QParser {

  private final Presearcher presearcher;

  public ReverseQueryParser(
      String qstr,
      SolrParams localParams,
      SolrParams params,
      SolrQueryRequest req,
      Presearcher presearcher) {
    super(qstr, localParams, params, req);
    this.presearcher = presearcher;
  }

  @Override
  public Query parse() throws SyntaxError {
    var obj = req.getContext().get(DOCUMENT_BATCH_KEY);
    if (!(obj instanceof DocumentBatchVisitor)) {
      throw new SyntaxError("Document Batch is of unexpected type");
    }
    var documentBatch = (DocumentBatchVisitor) obj;
    LeafReader queryIndexReader = documentBatch.get();
    return presearcher.buildQuery(queryIndexReader, getTermAcceptor());
  }

  private BiPredicate<String, BytesRef> getTermAcceptor() {
    var searcher = req.getSearcher();
    MonitorQueryCache cache = (MonitorQueryCache) searcher.getCache(SOLR_MONITOR_CACHE_NAME);
    if (cache == null) {
      return ReverseQueryParser::allTermAcceptor;
    }
    return cache::acceptTerm;
  }

  private static boolean allTermAcceptor(String __, BytesRef ___) {
    return true;
  }
}
