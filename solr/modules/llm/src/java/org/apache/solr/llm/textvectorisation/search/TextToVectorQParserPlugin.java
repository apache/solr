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
package org.apache.solr.llm.textvectorisation.search;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.llm.textvectorisation.model.SolrTextToVectorModel;
import org.apache.solr.llm.textvectorisation.store.rest.ManagedTextToVectorModelStore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceObserver;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.neural.KnnQParser;

/**
 * A neural query parser that encode the query to a vector and then run K-nearest neighbors search
 * on Dense Vector fields. See Wiki page
 * https://solr.apache.org/guide/solr/latest/query-guide/dense-vector-search.html
 */
public class TextToVectorQParserPlugin extends QParserPlugin
    implements ResourceLoaderAware, ManagedResourceObserver {
  public static final String EMBEDDING_MODEL_PARAM = "model";
  private ManagedTextToVectorModelStore modelStore = null;

  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new TextToVectorQParser(qstr, localParams, params, req);
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    final SolrResourceLoader solrResourceLoader = (SolrResourceLoader) loader;
    ManagedTextToVectorModelStore.registerManagedTextToVectorModelStore(solrResourceLoader, this);
  }

  @Override
  public void onManagedResourceInitialized(NamedList<?> args, ManagedResource res)
      throws SolrException {
    if (res instanceof ManagedTextToVectorModelStore) {
      modelStore = (ManagedTextToVectorModelStore) res;
    }
    if (modelStore != null) {
      modelStore.loadStoredModels();
    }
  }

  public class TextToVectorQParser extends KnnQParser {

    public TextToVectorQParser(
        String queryString, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      super(queryString, localParams, params, req);
    }

    private void checkVectorEncoding() {
      final VectorEncoding vectorEncoding =
          getCheckedFieldType(req.getCore().getLatestSchema().getField(getFieldName()))
              .getVectorEncoding();

      if (vectorEncoding != VectorEncoding.FLOAT32) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Vector Encoding not supported : " + vectorEncoding);
      }
    }

    @Override
    public Query parse() throws SyntaxError {
      checkParam(qstr, "Query string is empty, nothing to vectorise");
      final String embeddingModelName = localParams.get(EMBEDDING_MODEL_PARAM);
      checkParam(embeddingModelName, "The 'model' parameter is missing");
      SolrTextToVectorModel textToVector = modelStore.getModel(embeddingModelName);

      if (textToVector != null) {
        checkVectorEncoding();
        super.vectorToSearch = Arrays.toString(textToVector.vectorise(qstr));
        return super.parse();
      } else {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "The model requested '"
                + embeddingModelName
                + "' can't be found in the store: "
                + ManagedTextToVectorModelStore.REST_END_POINT);
      }
    }
  }

  private void checkParam(String value, String message) {
    if (value == null || value.isBlank()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, message);
    }
  }
}
