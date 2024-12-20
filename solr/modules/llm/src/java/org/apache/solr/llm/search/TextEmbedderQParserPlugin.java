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
package org.apache.solr.llm.search;

import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.llm.embedding.SolrEmbeddingModel;
import org.apache.solr.llm.store.rest.ManagedEmbeddingModelStore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceObserver;
import org.apache.solr.schema.DenseVectorField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.neural.KnnQParser;

import java.io.IOException;


/**
 * A neural query parser to run K-nearest neighbors search on Dense Vector fields. See Wiki page
 * https://solr.apache.org/guide/solr/latest/query-guide/dense-vector-search.html
 */
public class TextEmbedderQParserPlugin extends QParserPlugin
        implements ResourceLoaderAware, ManagedResourceObserver {
  public static final String NAME = "embed";
  /** query parser plugin: the name of the attribute for setting the model */
  public static final String EMBEDDING_MODEL = "model";
  
  private ManagedEmbeddingModelStore modelStore = null;

  
  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new TextEmbedderQParser(qstr, localParams, params, req);
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    final SolrResourceLoader solrResourceLoader = (SolrResourceLoader) loader;
    ManagedEmbeddingModelStore.registerManagedEmbeddingModelStore(solrResourceLoader, this);
  }

  @Override
  public void onManagedResourceInitialized(NamedList<?> args, ManagedResource res)
          throws SolrException {
    if (res instanceof ManagedEmbeddingModelStore) {
      modelStore = (ManagedEmbeddingModelStore) res;
    }
    if (modelStore != null) {
      // now we can safely load the models
      modelStore.loadStoredModels();
    }
  }

  public class TextEmbedderQParser extends KnnQParser {

    public TextEmbedderQParser(
            String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      super(qstr, localParams, params, req);
    }

    @Override
    public Query parse() throws SyntaxError {
      final String embeddingModelName = localParams.get(EMBEDDING_MODEL);
      SolrEmbeddingModel embedder = modelStore.getModel(embeddingModelName);

      final SchemaField schemaField = req.getCore().getLatestSchema().getField(getFieldName());
      final DenseVectorField denseVectorType = getCheckedFieldType(schemaField);
      VectorEncoding vectorEncoding = denseVectorType.getVectorEncoding();
      final int topK = localParams.getInt(TOP_K, DEFAULT_TOP_K);

      switch (vectorEncoding) {
        case FLOAT32: {
          float[] vectorToSearch = embedder.floatVectorise(qstr);
          return denseVectorType.getKnnVectorQuery(
                  schemaField.getName(), vectorToSearch, topK, getFilterQuery());
        }
        case BYTE: {
          byte[] vectorToSearch = embedder.byteVectorise(qstr);
          return denseVectorType.getKnnVectorQuery(
                  schemaField.getName(), vectorToSearch, topK, getFilterQuery());
        }
        default:
          throw new SolrException(
                  SolrException.ErrorCode.SERVER_ERROR,
                  "Unexpected state. Vector Encoding: " + vectorEncoding);
      }


    }
  }
}
