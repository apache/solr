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

package org.apache.solr.llm.texttovector.update.processor;

import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.llm.texttovector.model.SolrTextToVectorModel;
import org.apache.solr.llm.texttovector.store.rest.ManagedTextToVectorModelStore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceObserver;
import org.apache.solr.schema.DenseVectorField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

/**
 * This class implements an UpdateProcessorFactory for the Text Embedder Update Processor. It takes
 * in input a series of parameter that will be necessary to instantiate and use the embedder
 *
 */
public class TextEmbedderUpdateProcessorFactory extends UpdateRequestProcessorFactory implements ResourceLoaderAware, ManagedResourceObserver {
    private static final String INPUT_FIELD_PARAM = "inputField";
  private static final String OUTPUT_FIELD_PARAM = "outputField";
  private static final String EMBEDDING_MODEl_NAME_PARAM = "model";

    private String  inputField;
  private String  outputField;
  private String  embeddingModelName;
  private ManagedTextToVectorModelStore modelStore = null;
  private SolrTextToVectorModel textToVector;
  private SolrParams params;

    @Override
    public void inform(ResourceLoader loader) {
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
    
  @Override
  public void init(final NamedList<?> args) {
      if (args != null) {
          params = args.toSolrParams();
          inputField = params.get(INPUT_FIELD_PARAM);
          checkNotNull(INPUT_FIELD_PARAM, inputField);

          outputField = params.get(OUTPUT_FIELD_PARAM);
          checkNotNull(OUTPUT_FIELD_PARAM, outputField);
          
          

          embeddingModelName = params.get(EMBEDDING_MODEl_NAME_PARAM);
          checkNotNull(EMBEDDING_MODEl_NAME_PARAM, embeddingModelName);

          textToVector = modelStore.getModel(embeddingModelName);
          if (textToVector == null) {
              throw new SolrException(
                      SolrException.ErrorCode.BAD_REQUEST,
                      "The model requested '"
                              + embeddingModelName
                              + "' can't be found in the store: "
                              + ManagedTextToVectorModelStore.REST_END_POINT);
          }
      }
  }

  private void checkNotNull(String paramName, Object param) {
    if (param == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Text Embedder UpdateProcessor '" + paramName + "' can not be null");
    }
  }

  @Override
  public UpdateRequestProcessor getInstance(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {

    final SchemaField outputFieldSchema = req.getCore().getLatestSchema().getField(outputField);
    assertIsDenseVectorField(outputFieldSchema);

    return new TextEmbedderUpdateProcessor(inputField, outputField, textToVector, next);
  }

    protected void assertIsDenseVectorField(SchemaField schemaField) {
        FieldType fieldType = schemaField.getType();
        if (!(fieldType instanceof DenseVectorField)) {
            throw new SolrException(
                    SolrException.ErrorCode.BAD_REQUEST,
                    "only DenseVectorField is compatible with Vector Query Parsers");
        }
    }

    public String getInputField() {
        return inputField;
    }

    public String getOutputField() {
        return outputField;
    }

    public SolrTextToVectorModel getTextToVector() {
        return textToVector;
    }
}
