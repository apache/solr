///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package java.org.apache.solr.llm.update.processor;
//
//import org.apache.lucene.index.IndexReader;
//import org.apache.lucene.search.Query;
//import org.apache.solr.common.SolrException;
//import org.apache.solr.common.cloud.SolrClassLoader;
//import org.apache.solr.common.params.SolrParams;
//import org.apache.solr.common.util.NamedList;
//import org.apache.solr.core.SolrCore;
//import org.apache.solr.core.SolrResourceLoader;
//import org.apache.solr.request.SolrQueryRequest;
//import org.apache.solr.response.SolrQueryResponse;
//import org.apache.solr.schema.DenseVectorField;
//import org.apache.solr.schema.FieldType;
//import org.apache.solr.schema.IndexSchema;
//import org.apache.solr.schema.SchemaField;
//import org.apache.solr.search.LuceneQParser;
//import org.apache.solr.search.SyntaxError;
//import org.apache.solr.update.processor.ClassificationUpdateProcessorParams;
//import org.apache.solr.update.processor.UpdateRequestProcessor;
//import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
//import org.apache.solr.util.plugin.SolrCoreAware;
//
//import java.io.IOException;
//import java.org.apache.solr.llm.embedding.EmbeddingModel;
//import java.util.Locale;
//
///**
// * This class implements an UpdateProcessorFactory for the Classification Update Processor. It takes
// * in input a series of parameter that will be necessary to instantiate and use the Classifier
// *
// * @since 6.1.0
// */
//public class TextEmbedderUpdateProcessorFactory extends UpdateRequestProcessorFactory implements SolrCoreAware {
//  private SolrResourceLoader solrResourceLoader = null;
//  private static final String INPUT_FIELD_PARAM = "inputField";
//  private static final String OUTPUT_FIELD_PARAM = "outputField";
//  private static final String MODEl_NAME_PARAM = "model";
//  
//  private String  inputField;
//  private final String  outputField;
//  private final String  modelName;
//
//  private EmbeddingModel embedder = null;
//  private SolrParams params;
//
//  @Override
//  public void init(final NamedList<?> args) {
//    if (args != null) {
//      params = args.toSolrParams();
//      inputField =
//          params.get(INPUT_FIELD_PARAM); // must be a comma separated list of fields
//      checkNotNull(INPUT_FIELD_PARAM, inputField);
//
//      outputField = (params.get(OUTPUT_FIELD_PARAM));
//      solrResourceLoader.getCoreContainer().
//      SolrClassLoader schemaLoader = solrResourceLoader.getSchemaLoader();
//      schemaLoader.
//      IndexSchema schema = req.getSchema();
//      SchemaField schemaField = schema.getField(outputField);
//      final DenseVectorField denseVectorType = checkFieldType(schemaField);
//
//      String modelName = params.get(MODEl_NAME_PARAM);
//      solrResourceLoader.getSolrConfig().
//    }
//  }
//
//  private void checkNotNull(String paramName, Object param) {
//    if (param == null) {
//      throw new SolrException(
//          SolrException.ErrorCode.SERVER_ERROR,
//          "Text Embedder UpdateProcessor '" + paramName + "' can not be null");
//    }
//  }
//
//  @Override
//  public UpdateRequestProcessor getInstance(
//      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
//
//    IndexSchema schema = req.getSchema();
//    IndexReader indexReader = req.getSearcher().getIndexReader();
//
//    return new TextEmbedderUpdateProcessor(inputField, outputField, embedder, next);
//  }
//
//
//  protected static DenseVectorField checkFieldType(SchemaField schemaField) {
//    FieldType fieldType = schemaField.getType();
//    if (!(fieldType instanceof DenseVectorField)) {
//      throw new SolrException(
//              SolrException.ErrorCode.BAD_REQUEST,
//              "only DenseVectorField is compatible to store vectors");
//    }
//    return (DenseVectorField) fieldType;
//  }
//
//  @Override
//  public void inform(SolrCore core) {
//    solrResourceLoader = core.getResourceLoader();
//  }
//}
