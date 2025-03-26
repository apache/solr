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

package org.apache.solr.llm.textvectorisation.update.processor;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.llm.textvectorisation.model.SolrTextToVectorModel;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TextToVectorUpdateProcessor extends UpdateRequestProcessor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private IndexSchema schema;
  private final String inputField;
  private final String outputField;
  private SolrTextToVectorModel textToVector;

  public TextToVectorUpdateProcessor(
      String inputField,
      String outputField,
      SolrTextToVectorModel textToVector,
      SolrQueryRequest req,
      UpdateRequestProcessor next) {
    super(next);
    this.schema = req.getSchema();
    this.inputField = inputField;
    this.outputField = outputField;
    this.textToVector = textToVector;
  }

  /**
   * @param cmd the update command in input containing the Document to process
   * @throws IOException If there is a low-level I/O error
   */
  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    SolrInputDocument doc = cmd.getSolrInputDocument();
    SolrInputField inputFieldContent = doc.get(inputField);
    if (!isNullOrEmpty(inputFieldContent)) {
      try {
        String textToVectorise = inputFieldContent.getValue().toString();
        float[] vector = textToVector.vectorise(textToVectorise);
        List<Float> vectorAsList = new ArrayList<Float>(vector.length);
        for (float f : vector) {
          vectorAsList.add(f);
        }
        doc.addField(outputField, vectorAsList);
      } catch (RuntimeException vectorisationFailure) {
        if (log.isErrorEnabled()) {
          SchemaField uniqueKeyField = schema.getUniqueKeyField();
          String uniqueKeyFieldName = uniqueKeyField.getName();
          log.error(
              "Could not vectorise: {} for the document with {}: {}",
              inputField,
              uniqueKeyFieldName,
              doc.getFieldValue(uniqueKeyFieldName),
              vectorisationFailure);
        }
      }
    }
    super.processAdd(cmd);
  }

  protected boolean isNullOrEmpty(SolrInputField inputFieldContent) {
    return (inputFieldContent == null
        || inputFieldContent.getValue() == null
        || inputFieldContent.getValue().toString().isEmpty());
  }
}
