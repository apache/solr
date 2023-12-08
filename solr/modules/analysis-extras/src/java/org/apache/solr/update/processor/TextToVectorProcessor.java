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
package org.apache.solr.update.processor;

import ai.onnxruntime.OrtException;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import opennlp.dl.vectors.SentenceVectorsDL;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextToVectorProcessor extends UpdateRequestProcessor {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String INPUT_FIELD_PARAM = "inputField";
  private static final String OUTPUT_FIELD_PARAM = "outputField";
  private static final String MODEL_FILE_NAME_PARAM = "model";
  private static final String VOCAB_FILE_NAME_PARAM = "vocab";

  private static final String DEFAULT_INPUT_FIELDNAME = "name";
  private static final String DEFAULT_OUTPUT_FIELDNAME = "film_vector";
  private static final String DEFAULT_MODEL_FILE_NAME =
      "/Users/cpoerschke/opennlp-data/onnx/sentence-transformers/model.onnx";
  private static final String DEFAULT_VOCAB_FILE_NAME =
      "/Users/cpoerschke/opennlp-data/onnx/sentence-transformers/vocab.txt";

  private String inputFieldname = DEFAULT_INPUT_FIELDNAME;
  private String outputFieldname = DEFAULT_OUTPUT_FIELDNAME;
  private String modelFileName = DEFAULT_MODEL_FILE_NAME;
  private String vocabFileName = DEFAULT_VOCAB_FILE_NAME;

  private final SentenceVectorsDL sentenceVectorsDL;

  TextToVectorProcessor(
      SolrParams parameters,
      SolrQueryRequest request,
      SolrQueryResponse response,
      UpdateRequestProcessor nextProcessor) {
    super(nextProcessor);

    if (parameters != null) {
      this.modelFileName = parameters.get(MODEL_FILE_NAME_PARAM, DEFAULT_MODEL_FILE_NAME);
      this.vocabFileName = parameters.get(VOCAB_FILE_NAME_PARAM, DEFAULT_VOCAB_FILE_NAME);
    }

    SentenceVectorsDL sv = null;
    try {
      sv = new SentenceVectorsDL(new File(this.modelFileName), new File(this.vocabFileName));
    } catch (IOException ioe) {
      log.warn("SentenceVectorsDL initialisation failed", ioe);
    } catch (OrtException oe) {
      log.warn("SentenceVectorsDL initialisation failed", oe);
    }
    this.sentenceVectorsDL = sv;
  }

  @Override
  public void processAdd(AddUpdateCommand command) throws IOException {
    final SolrInputDocument document = command.getSolrInputDocument();
    if (document.containsKey(this.inputFieldname)) {
      final String inputText = (String) document.getFieldValue(this.inputFieldname);
      final float[] vectors = getVectors(inputText);
      if (vectors != null) {
        if (!document.containsKey(this.outputFieldname)) {
          if (log.isInfoEnabled()) {
            log.info(
                "for {}='{}' adding {}={}",
                this.inputFieldname,
                inputText,
                this.outputFieldname,
                vectors);
          }
          document.setField(this.outputFieldname, vectors);
        } else {
          if (log.isInfoEnabled()) {
            log.info(
                "for {}='{}' would have changed {} from {} to {}",
                this.inputFieldname,
                inputText,
                this.outputFieldname,
                document.getFieldValue(this.outputFieldname),
                vectors);
          }
        }
      }
    }
    super.processAdd(command);
  }

  private float[] getVectors(final String text) {
    try {
      return this.sentenceVectorsDL.getVectors(text);
    } catch (OrtException oe) {
      log.warn("SentenceVectorsDL.getVectors failed", oe);
      return null;
    }
  }
}
