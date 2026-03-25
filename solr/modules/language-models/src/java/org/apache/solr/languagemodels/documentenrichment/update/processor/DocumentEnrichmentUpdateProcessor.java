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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.languagemodels.documentenrichment.model.SolrChatModel;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DocumentEnrichmentUpdateProcessor extends UpdateRequestProcessor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private IndexSchema schema;
  private final String inputField;
  private final String outputField;
  private final String prompt;
  private SolrChatModel chatModel;

  public DocumentEnrichmentUpdateProcessor(
      String inputField,
      String outputField,
      String prompt,
      SolrChatModel chatModel,
      SolrQueryRequest req,
      UpdateRequestProcessor next) {
    super(next);
    this.schema = req.getSchema();
    // prompt must contain "{input}" where the user wants to inject the input data to populate outputField
    this.prompt = prompt;
    this.inputField = inputField;
    this.outputField = outputField;
    this.chatModel = chatModel;
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
        // as for now, only a plain text as prompt is sent to the model (no support for tools/skills/agents)
        String toInject = inputFieldContent.getValue().toString();
        String injectedPrompt = prompt.replace("{input}",  toInject);
        String response = chatModel.chat(injectedPrompt);
        /* TODO: check if the outputField is multivalued and adapt the code/llm call to deal with lists also, together
            with structured output support
        */
        doc.setField(outputField, response);
      } catch (RuntimeException chatModelFailure) {
        if (log.isErrorEnabled()) {
          SchemaField uniqueKeyField = schema.getUniqueKeyField();
          String uniqueKeyFieldName = uniqueKeyField.getName();
          log.error(
              "Could not process: {} for the document with {}: {}",
              inputField,
              uniqueKeyFieldName,
              doc.getFieldValue(uniqueKeyFieldName),
              chatModelFailure);
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
