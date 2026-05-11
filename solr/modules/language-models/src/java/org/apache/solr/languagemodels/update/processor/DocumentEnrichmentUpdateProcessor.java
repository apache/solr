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

package org.apache.solr.languagemodels.update.processor;

import dev.langchain4j.model.chat.request.ResponseFormat;
import dev.langchain4j.model.chat.request.json.JsonArraySchema;
import dev.langchain4j.model.chat.request.json.JsonBooleanSchema;
import dev.langchain4j.model.chat.request.json.JsonIntegerSchema;
import dev.langchain4j.model.chat.request.json.JsonNumberSchema;
import dev.langchain4j.model.chat.request.json.JsonObjectSchema;
import dev.langchain4j.model.chat.request.json.JsonSchemaElement;
import dev.langchain4j.model.chat.request.json.JsonStringSchema;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.Utils;
import org.apache.solr.languagemodels.model.SolrLargeLanguageModel;
import org.apache.solr.languagemodels.update.processor.factory.DocumentEnrichmentUpdateProcessorFactory;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentEnrichmentUpdateProcessor extends UpdateRequestProcessor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final IndexSchema schema;
  private final List<String> inputFields;
  private final String outputField;
  private final String prompt;
  private final SolrLargeLanguageModel largeLanguageModel;
  private final boolean multiValued;
  private final ResponseFormat responseFormat;

  public DocumentEnrichmentUpdateProcessor(
      List<String> inputFields,
      String outputField,
      String prompt,
      SolrLargeLanguageModel largeLanguageModel,
      boolean multiValued,
      ResponseFormat responseFormat,
      SolrQueryRequest req,
      UpdateRequestProcessor next) {
    super(next);
    this.schema = req.getSchema();
    this.inputFields = inputFields;
    this.outputField = outputField;
    this.prompt = prompt;
    this.largeLanguageModel = largeLanguageModel;
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
      Object returnValue =
          Utils.fromJSONString(largeLanguageModel.generate(injectedPrompt, responseFormat));
      // Guardrails for OllamaChatModel, since it doesn't support strict JSON structured output mode
      if (!(returnValue instanceof Map<?, ?> map)
          || !map.containsKey(DocumentEnrichmentUpdateProcessorFactory.JSON_FIELD_PROPERTY)) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "LLM was not able to format the response correctly: " + returnValue);
      }
      Object generatedFieldValue =
          map.get(DocumentEnrichmentUpdateProcessorFactory.JSON_FIELD_PROPERTY);
      JsonSchemaElement valueSchema =
          ((JsonObjectSchema) responseFormat.jsonSchema().rootElement())
              .properties()
              .get(DocumentEnrichmentUpdateProcessorFactory.JSON_FIELD_PROPERTY);
      boolean typeOk =
          switch (valueSchema) {
            case JsonStringSchema ignored -> generatedFieldValue instanceof String;
            case JsonIntegerSchema ignored -> generatedFieldValue instanceof Integer
                || generatedFieldValue instanceof Long;
            case JsonNumberSchema ignored -> generatedFieldValue instanceof Number;
            case JsonBooleanSchema ignored -> generatedFieldValue instanceof Boolean;
            case JsonArraySchema ignored -> generatedFieldValue instanceof List;
            default -> true;
          };
      if (!typeOk) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "LLM returned wrong value type: expected "
                + valueSchema.getClass().getSimpleName()
                + " but got "
                + generatedFieldValue.getClass().getSimpleName());
      }
      if (multiValued && generatedFieldValue instanceof List<?> generatedFieldValueList) {
        for (Object item : generatedFieldValueList) {
          doc.addField(outputField, item);
        }
      } else {
        doc.setField(outputField, generatedFieldValue);
      }
    } catch (RuntimeException largeLanguageModelFailure) {
      if (log.isErrorEnabled()) {
        SchemaField uniqueKeyField = schema.getUniqueKeyField();
        String uniqueKeyFieldName = uniqueKeyField.getName();
        log.error(
            "Could not process fields {} for the document with {}: {}",
            inputFields,
            uniqueKeyFieldName,
            doc.getFieldValue(uniqueKeyFieldName),
            largeLanguageModelFailure);
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
