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
package org.apache.solr.response.transform;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.ResultContext;
import org.apache.solr.search.DocIterationInfo;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Add values from a ValueSource (function query etc)
 *
 * <p>NOT really sure how or if this could work...
 *
 * @since solr 4.0
 */
public class ValueSourceAugmenter extends DocTransformer {
  private static final Object NULL_SENTINEL = new Object();
  public final String name;
  public final QParser qparser;
  public final ValueSource valueSource;
  private final int maxPrefetchSize;

  public ValueSourceAugmenter(String name, QParser qparser, ValueSource valueSource) {
    this.name = name;
    this.qparser = qparser;
    this.valueSource = valueSource;
    String maxPrefetchSizeRaw = qparser.getParam("preFetchDocs");
    this.maxPrefetchSize = maxPrefetchSizeRaw != null ? Integer.parseInt(maxPrefetchSizeRaw) : 1000;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setContext(ResultContext context) {
    super.setContext(context);
    try {
      searcher = context.getSearcher();
      readerContexts = searcher.getIndexReader().leaves();
      fcontext = ValueSource.newContext(searcher);
      this.valueSource.createWeight(fcontext, searcher);
      final var docList = context.getDocList();
      if (docList == null) {
        return;
      }

      final int prefetchSize = Math.min(docList.size(), maxPrefetchSize);
      final int[] ids = new int[prefetchSize];
      int i = 0;
      var iter = docList.iterator();
      while (iter.hasNext() && i < prefetchSize) {
        ids[i++] = iter.nextDoc();
      }
      Arrays.sort(ids);
      cachedValuesById = new IntObjectHashMap<>(ids.length);

      FunctionValues values = null;
      int docBase = -1;
      int currentIdx = -1;
      for (int docid : ids) {
        int idx = ReaderUtil.subIndex(docid, readerContexts);
        if (currentIdx != idx) {
          currentIdx = idx;
          LeafReaderContext rcontext = readerContexts.get(idx);
          docBase = rcontext.docBase;
          values = valueSource.getValues(fcontext, rcontext);
        }
        int localId = docid - docBase;
        var value = values.objectVal(localId);
        cachedValuesById.put(docid, value != null ? value : NULL_SENTINEL);
      }
    } catch (IOException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "exception for valuesource " + valueSource, e);
    }
  }

  Map<Object, Object> fcontext;
  SolrIndexSearcher searcher;
  List<LeafReaderContext> readerContexts;
  IntObjectHashMap<Object> cachedValuesById;

  @Override
  public void transform(SolrDocument doc, int docid, DocIterationInfo docIterationInfo) {
    Object cacheValue = (cachedValuesById != null) ? cachedValuesById.get(docid) : null;
    if (cacheValue != null) {
      setValue(doc, cacheValue != NULL_SENTINEL ? cacheValue : null);
    } else {
      // Fallback to on-demand calculation for documents not in the pre-calculated set, RTG use case
      try {
        int idx = ReaderUtil.subIndex(docid, readerContexts);
        LeafReaderContext rcontext = readerContexts.get(idx);
        FunctionValues values = valueSource.getValues(fcontext, rcontext);
        int localId = docid - rcontext.docBase;
        setValue(doc, values.objectVal(localId));
      } catch (IOException e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "exception at docid " + docid + " for valuesource " + valueSource,
            e);
      }
    }
  }

  /** Always returns true */
  @Override
  public boolean needsSolrIndexSearcher() {
    return true;
  }

  protected void setValue(SolrDocument doc, Object val) {
    if (val != null) {
      doc.setField(name, val);
    }
  }
}
