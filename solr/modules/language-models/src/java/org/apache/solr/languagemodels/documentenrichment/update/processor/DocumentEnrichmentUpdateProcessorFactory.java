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
import dev.langchain4j.model.chat.request.ResponseFormatType;
import dev.langchain4j.model.chat.request.json.JsonArraySchema;
import dev.langchain4j.model.chat.request.json.JsonBooleanSchema;
import dev.langchain4j.model.chat.request.json.JsonIntegerSchema;
import dev.langchain4j.model.chat.request.json.JsonNumberSchema;
import dev.langchain4j.model.chat.request.json.JsonObjectSchema;
import dev.langchain4j.model.chat.request.json.JsonSchema;
import dev.langchain4j.model.chat.request.json.JsonSchemaElement;
import dev.langchain4j.model.chat.request.json.JsonStringSchema;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.RequiredSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.languagemodels.documentenrichment.model.SolrChatModel;
import org.apache.solr.languagemodels.documentenrichment.store.rest.ManagedChatModelStore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceObserver;
import org.apache.solr.schema.BoolField;
import org.apache.solr.schema.DatePointField;
import org.apache.solr.schema.DenseVectorField;
import org.apache.solr.schema.DoublePointField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.FloatPointField;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IntPointField;
import org.apache.solr.schema.LongPointField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.schema.TextField;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * Insert in an existing field the output of the model coming from one or more textual field values.
 *
 * <p>One or more {@code inputField} parameters specify the Solr fields to use as input. Each field
 * name must appear as a {@code {fieldName}} placeholder in the prompt. Exactly one of {@code
 * prompt} or {@code promptFile} must be provided.
 *
 * <pre class="prettyprint" >
 * &lt;processor class=&quot;solr.llm.documentenrichment.update.processor.DocumentEnrichmentUpdateProcessorFactory&quot;&gt;
 *   &lt;str name=&quot;inputField&quot;&gt;title_field&lt;/str&gt;
 *   &lt;str name=&quot;inputField&quot;&gt;body_field&lt;/str&gt;
 *   &lt;str name=&quot;outputField&quot;&gt;enriched_field&lt;/str&gt;
 *   &lt;str name=&quot;prompt&quot;&gt;Title: {title_field}. Body: {body_field}.&lt;/str&gt;
 *   &lt;str name=&quot;model&quot;&gt;ChatModel&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 *
 * <p>Alternatively, the prompt can be loaded from a text file using {@code promptFile}:
 *
 * <pre class="prettyprint" >
 * &lt;processor class=&quot;solr.llm.documentenrichment.update.processor.DocumentEnrichmentUpdateProcessorFactory&quot;&gt;
 *   &lt;str name=&quot;inputField&quot;&gt;title_field&lt;/str&gt;
 *   &lt;str name=&quot;outputField&quot;&gt;enriched_field&lt;/str&gt;
 *   &lt;str name=&quot;promptFile&quot;&gt;prompt.txt&lt;/str&gt;
 *   &lt;str name=&quot;model&quot;&gt;ChatModel&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 *
 * <p>Validation rules:
 *
 * <ul>
 *   <li>At least one {@code inputField} must be declared.
 *   <li>Exactly one of {@code prompt} or {@code promptFile} must be provided.
 *   <li>Every declared {@code inputField} must have a corresponding {@code {fieldName}} placeholder
 *       in the prompt.
 *   <li>Every {@code {placeholder}} in the prompt must correspond to a declared {@code inputField}.
 * </ul>
 */
public class DocumentEnrichmentUpdateProcessorFactory extends UpdateRequestProcessorFactory
    implements SolrCoreAware, ManagedResourceObserver {
  private static final String INPUT_FIELD_PARAM = "inputField";
  private static final String OUTPUT_FIELD_PARAM = "outputField";
  private static final String PROMPT = "prompt";
  private static final String PROMPT_FILE = "promptFile";
  private static final String MODEL_NAME = "model";
  private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\{([^}]+)\\}");

  private ManagedChatModelStore modelStore = null;

  private List<String> inputFields;
  private String outputField;
  private String prompt;
  private String promptFile;
  private String modelName;
  private SolrParams params;

  @Override
  public void init(final NamedList<?> args) {
    // removeConfigArgs handles both multiple <str name="inputField"> and <arr name="inputField">
    // and must be called before toSolrParams() since it mutates args in place
    Collection<String> fieldNames = args.removeConfigArgs(INPUT_FIELD_PARAM);
    if (fieldNames.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "At least one 'inputField' must be provided");
    }
    inputFields = List.copyOf(fieldNames);

    params = args.toSolrParams();
    RequiredSolrParams required = params.required();
    outputField = required.get(OUTPUT_FIELD_PARAM);
    modelName = required.get(MODEL_NAME);

    String inlinePrompt = params.get(PROMPT);
    String promptFilePath = params.get(PROMPT_FILE);

    if (inlinePrompt == null && promptFilePath == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Either 'prompt' or 'promptFile' must be provided");
    }
    if (inlinePrompt != null && promptFilePath != null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Only one of 'prompt' or 'promptFile' can be provided, not both");
    }
    if (inlinePrompt != null) {
      validatePromptPlaceholders(inlinePrompt, inputFields);
      this.prompt = inlinePrompt;
    }
    this.promptFile = promptFilePath;
  }

  @Override
  public void inform(SolrCore core) {
    final SolrResourceLoader solrResourceLoader = core.getResourceLoader();
    ManagedChatModelStore.registerManagedChatModelStore(solrResourceLoader, this);
    if (promptFile != null) {
      try (InputStream is = solrResourceLoader.openResource(promptFile)) {
        prompt = new String(is.readAllBytes(), StandardCharsets.UTF_8).trim();
      } catch (IOException e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Cannot read prompt file: " + promptFile,
            e);
      }
      validatePromptPlaceholders(prompt, inputFields);
    }
  }

  @Override
  public void onManagedResourceInitialized(NamedList<?> args, ManagedResource res)
      throws SolrException {
    if (res instanceof ManagedChatModelStore) {
      modelStore = (ManagedChatModelStore) res;
    }
    if (modelStore != null) {
      modelStore.loadStoredModels();
    }
  }

  @Override
  public UpdateRequestProcessor getInstance(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    IndexSchema latestSchema = req.getCore().getLatestSchema();

    for (String fieldName : inputFields) {
      if (!latestSchema.isDynamicField(fieldName) && !latestSchema.hasExplicitField(fieldName)) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "undefined field: \"" + fieldName + "\"");
      }
    }

    final SchemaField outputFieldSchema = latestSchema.getField(outputField);
    assertIsSupportedField(outputFieldSchema);

    ResponseFormat responseFormat = buildResponseFormat(outputFieldSchema);
    boolean multiValued = outputFieldSchema.multiValued();

    ManagedChatModelStore modelStore = ManagedChatModelStore.getManagedModelStore(req.getCore());
    SolrChatModel chatModel = modelStore.getModel(modelName);
    if (chatModel == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "The model configured in the Update Request Processor '"
              + modelName
              + "' can't be found in the store: "
              + ManagedChatModelStore.REST_END_POINT);
    }

    return new DocumentEnrichmentUpdateProcessor(
        inputFields, outputField, prompt, chatModel, multiValued, responseFormat, req, next);
  }

  /**
   * Validates that the output field type is supported. Supported types are: textual (Str, Text),
   * numeric (Int, Long, Float, Double), boolean and date. Vector and binary fields are not
   * supported.
   */
  protected void assertIsSupportedField(SchemaField schemaField) {
    try {
      toJsonSchemaElement(schemaField.getType());
    } catch (SolrException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "field type is not supported by Document Enrichment: " + schemaField.getName());
    }
  }

  /**
   * Builds a {@link ResponseFormat} that instructs the model to return a JSON object {@code
   * {"value": ...}} whose value type matches the Solr field type. For multivalued fields the value
   * is wrapped in a JSON array.
   */
  static ResponseFormat buildResponseFormat(SchemaField schemaField) {
    JsonSchemaElement valueElement = toJsonSchemaElement(schemaField.getType());
    JsonSchemaElement valueSchema =
        schemaField.multiValued()
            ? JsonArraySchema.builder().items(valueElement).build() // could be only supported by Gemini
            // (source: https://github.com/langchain4j/langchain4j/blob/main/docs/docs/tutorials/structured-outputs.md)
            // If not supported, we cannot support multivalued fields as outputField
            : valueElement;
    return ResponseFormat.builder()
        .type(ResponseFormatType.JSON)
        .jsonSchema(
            JsonSchema.builder()
                .name("output")
                .rootElement(
                    JsonObjectSchema.builder()
                        .addProperty("value", valueSchema)
                        .required("value")
                        .build())
                .build())
        .build();
  }

  private static JsonSchemaElement toJsonSchemaElement(FieldType fieldType) {
    // DenseVectorField extends FloatPointField, so it must be rejected before the numeric checks
    if (fieldType instanceof DenseVectorField) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "field type is not supported by Document Enrichment: "
              + fieldType.getClass().getSimpleName());
    }
    if (fieldType instanceof StrField
        || fieldType instanceof TextField
        || fieldType instanceof DatePointField) {
      return new JsonStringSchema();
    } else if (fieldType instanceof IntPointField || fieldType instanceof LongPointField) {
      return new JsonIntegerSchema();
    } else if (fieldType instanceof FloatPointField || fieldType instanceof DoublePointField) {
      return new JsonNumberSchema();
    } else if (fieldType instanceof BoolField) {
      return new JsonBooleanSchema();
    } else {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "field type is not supported by Document Enrichment: "
              + fieldType.getClass().getSimpleName());
    }
  }

  private static void validatePromptPlaceholders(String prompt, List<String> fieldNames) {
    Set<String> promptPlaceholders = new LinkedHashSet<>();
    Matcher m = PLACEHOLDER_PATTERN.matcher(prompt);
    while (m.find()) {
      promptPlaceholders.add(m.group(1));
    }

    Set<String> missingInPrompt = new LinkedHashSet<>(fieldNames);
    missingInPrompt.removeAll(promptPlaceholders);
    if (!missingInPrompt.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "prompt is missing placeholders for inputField(s): " + missingInPrompt);
    }

    Set<String> unknownInPrompt = new LinkedHashSet<>(promptPlaceholders);
    unknownInPrompt.removeAll(new HashSet<>(fieldNames));
    if (!unknownInPrompt.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "prompt contains placeholders not declared as inputField(s): " + unknownInPrompt);
    }
  }

  public List<String> getInputFields() {
    return inputFields;
  }

  public String getOutputField() {
    return outputField;
  }

  public String getPrompt() {
    return prompt;
  }

  public String getModelName() {
    return modelName;
  }

  public String getPromptFile() {
    return promptFile;
  }
}
