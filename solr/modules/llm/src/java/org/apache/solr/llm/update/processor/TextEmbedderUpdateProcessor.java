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
//import org.apache.lucene.analysis.Analyzer;
//import org.apache.lucene.document.Document;
//import org.apache.lucene.index.IndexReader;
//import org.apache.lucene.index.VectorEncoding;
//import org.apache.lucene.util.BytesRef;
//import org.apache.solr.common.SolrException;
//import org.apache.solr.common.SolrInputDocument;
//import org.apache.solr.schema.DenseVectorField;
//import org.apache.solr.schema.IndexSchema;
//import org.apache.solr.schema.SchemaField;
//import org.apache.solr.update.AddUpdateCommand;
//import org.apache.solr.update.DocumentBuilder;
//import org.apache.solr.update.processor.ClassificationUpdateProcessorFactory.Algorithm;
//import org.apache.solr.update.processor.ClassificationUpdateProcessorParams;
//import org.apache.solr.update.processor.UpdateRequestProcessor;
//
//import java.io.IOException;
//import java.org.apache.solr.llm.embedding.EmbeddingModel;
//import java.util.HashMap;
//import java.util.Map;
//
//class TextEmbedderUpdateProcessor extends UpdateRequestProcessor {
///*
//  private final String inputField;
//  private final String outputField;
//  private EmbeddingModel embedder;
//
//  /**
//   * Sole constructor
//   *
//   * @param classificationParams classification advanced params
//   * @param next next update processor in the chain
//   * @param indexReader index reader
//   * @param schema schema
//   */
//  public TextEmbedderUpdateProcessor(
//      String modelName,
//      String inputField,
//      String outputField,
//      UpdateRequestProcessor next,
//      IndexReader indexReader,
//      IndexSchema schema) {
//    super(next);
//    this.inputField = inputField;
//    this.outputField = outputField;
//    final SchemaField schemaField = schema.getField(outputField);
//    final DenseVectorField denseVectorType = getCheckedFieldType(schemaField);
//
//    embedder =  new EmbeddingModel();// retrieve the embedderModel by name
//    
//    final DenseVectorField denseVectorType = getCheckedFieldType(schemaField);
//    VectorEncoding vectorEncoding = denseVectorType.getVectorEncoding();
//  }
//
//  public TextEmbedderUpdateProcessor(String inputField, String outputField, EmbeddingModel embedder, UpdateRequestProcessor next) {
//    super();
//  }
//
//  /**
//   * @param cmd the update command in input containing the Document to classify
//   * @throws IOException If there is a low-level I/O error
//   */
//  @Override
//  public void processAdd(AddUpdateCommand cmd) throws IOException {
//    SolrInputDocument doc = cmd.getSolrInputDocument();
//    Object textToEmbed = doc.getFieldValue(inputField);
//    embedder.
//    doc.addField(outputField, assignedClass);
//
//    if (documentClass == null) {
//      Document luceneDocument =
//          DocumentBuilder.toDocument(doc, cmd.getReq().getSchema(), false, true);
//      List<ClassificationResult<BytesRef>> assignedClassifications =
//          classifier.getClasses(luceneDocument, maxOutputClasses);
//      if (assignedClassifications != null) {
//        for (ClassificationResult<BytesRef> singleClassification : assignedClassifications) {
//          String assignedClass = singleClassification.getAssignedClass().utf8ToString();
//          doc.addField(predictedClassField, assignedClass);
//        }
//      }
//    }
//    super.processAdd(cmd);
//  }
//  */
//}
