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

package org.apache.solr.languagemodels.documentenrichment.update.processor;

import dev.langchain4j.model.chat.request.ResponseFormat;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.languagemodels.documentenrichment.model.SolrFieldGenerationModel;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DocumentEnrichmentUpdateProcessor extends UpdateRequestProcessor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final IndexSchema schema;
  private final List<String> inputFields;
  private final String outputField;
  private final String prompt;
  private final SolrFieldGenerationModel fieldGenerationModel;
  private final boolean multiValued;
  private final ResponseFormat responseFormat;

  public DocumentEnrichmentUpdateProcessor(
      List<String> inputFields,
      String outputField,
      String prompt,
      SolrFieldGenerationModel fieldGenerationModel,
      boolean multiValued,
      ResponseFormat responseFormat,
      SolrQueryRequest req,
      UpdateRequestProcessor next) {
    super(next);
    this.schema = req.getSchema();
    this.inputFields = inputFields;
    this.outputField = outputField;
    this.prompt = prompt;
    this.fieldGenerationModel  = fieldGenerationModel;
    this.multiValued = multiValued;
    this.responseFormat = responseFormat;
  }

  /**
   * @param cmd the update command in input containing the Document to process
   * @throws IOException If there is a low-level I/O error
   */
  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    SolrInputDocument doc = cmd.getSolrInputDocument();

    // Collect all field values; skip enrichment if any declared field is null or empty
    String injectedPrompt = prompt;
    for (String fieldName : inputFields) {
      SolrInputField field = doc.get(fieldName);
      if (isNullOrEmpty(field)) {
        super.processAdd(cmd);
        return;
      }
      injectedPrompt = injectedPrompt.replace("{" + fieldName + "}", field.getValue().toString());
    }

    try {
      // as for now, only a plain text as prompt is sent to the model (no support for
      // tools/skills/agents)
      // chatModel.chat returns the parsed value from the structured JSON response
      Object generatedFieldValue = fieldGenerationModel.generateFieldValue(injectedPrompt, responseFormat);
      if (multiValued && generatedFieldValue instanceof List<?> generatedFieldValueList) {
        for (Object item : generatedFieldValueList) {
          doc.addField(outputField, item);
        }
      } else {
        doc.setField(outputField, generatedFieldValue);
      }
    } catch (RuntimeException fieldGenerationModelFailure) {
      if (log.isErrorEnabled()) {
        SchemaField uniqueKeyField = schema.getUniqueKeyField();
        String uniqueKeyFieldName = uniqueKeyField.getName();
        log.error(
            "Could not process fields {} for the document with {}: {}",
            inputFields,
            uniqueKeyFieldName,
            doc.getFieldValue(uniqueKeyFieldName),
            fieldGenerationModelFailure);
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
