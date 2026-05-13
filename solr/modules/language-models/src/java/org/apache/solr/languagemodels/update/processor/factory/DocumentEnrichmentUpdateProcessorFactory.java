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

package org.apache.solr.languagemodels.update.processor.factory;

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
import org.apache.solr.languagemodels.model.SolrLargeLanguageModel;
import org.apache.solr.languagemodels.store.rest.LargeLanguageModelStore;
import org.apache.solr.languagemodels.update.processor.DocumentEnrichmentUpdateProcessor;
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
import org.apache.solr.schema.NestPathField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.schema.TextField;
import org.apache.solr.schema.UUIDField;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * Generate the content of {@code outputField} based on other fields specified as {@code
 * inputField}s.
 *
 * <p>The following validation rules are applied:
 *
 * <ul>
 *   <li>At least one {@code inputField} must be declared.
 *   <li>Exactly one of {@code prompt} or {@code promptFile} must be provided.
 *   <li>Every declared {@code inputField} must have a corresponding {@code {fieldName}} placeholder
 *       in the prompt.
 *   <li>Every {@code {placeholder}} in the prompt must correspond to a declared {@code inputField}.
 *   <li>One and only one {@code outputField} is allowed.
 * </ul>
 *
 * <pre class="prettyprint" >
 * &lt;processor class=&quot;solr.languagemodels.update.processor.factory.DocumentEnrichmentUpdateProcessorFactory&quot;&gt;
 *   &lt;str name=&quot;inputField&quot;&gt;title_field&lt;/str&gt;
 *   &lt;str name=&quot;inputField&quot;&gt;body_field&lt;/str&gt;
 *   &lt;str name=&quot;outputField&quot;&gt;enriched_field&lt;/str&gt;
 *   &lt;str name=&quot;prompt&quot;&gt;Title: {title_field}. Body: {body_field}.&lt;/str&gt; // or &lt;str name=&quot;promptFile&quot;&gt;prompt.txt&lt;/str&gt;
 *   &lt;str name=&quot;model&quot;&gt;model-name&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 *
 * <p>Multiple {@code inputField} values can also be declared as an array using {@code arr}:
 *
 * <pre class="prettyprint" >
 * &lt;arr name=&quot;inputField&quot;&gt;
 *   &lt;str&gt;title_field&lt;/str&gt;
 *   &lt;str&gt;body_field&lt;/str&gt;
 * &lt;/arr&gt;
 * </pre>
 */
public class DocumentEnrichmentUpdateProcessorFactory extends UpdateRequestProcessorFactory
    implements SolrCoreAware, ManagedResourceObserver {
  private static final String INPUT_FIELD_PARAM = "inputField";
  private static final String OUTPUT_FIELD_PARAM = "outputField";
  private static final String PROMPT = "prompt";
  private static final String PROMPT_FILE = "promptFile";
  private static final String MODEL_NAME = "model";
  private static final String JSON_SCHEMA_NAME = "output";
  public static final String JSON_FIELD_PROPERTY = "value";
  private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\{([^}]+)\\}");

  private List<String> inputFields;
  private String outputField;
  private String promptText;
  private String promptFile;
  private String modelName;

  @Override
  public void init(final NamedList<?> args) {
    // removeConfigArgs handles both multiple <str name="inputField"> and <arr name="inputField">
    // and must be called before toSolrParams() since it mutates args in place
    Collection<String> fieldNames = args.removeConfigArgs(INPUT_FIELD_PARAM);
    if (fieldNames.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "At least one 'inputField' must be provided");
    }
    inputFields = List.copyOf(fieldNames);

    Collection<String> outputFields = args.removeConfigArgs(OUTPUT_FIELD_PARAM);
    if (outputFields.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Exactly one 'outputField' must be provided");
    }
    if (outputFields.size() > 1) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Only one 'outputField' can be provided, but found: " + outputFields);
    }
    outputField = outputFields.iterator().next();

    SolrParams params = args.toSolrParams();
    RequiredSolrParams required = params.required();
    modelName = required.get(MODEL_NAME);

    String inlinePrompt = params.get(PROMPT);
    String promptFilePath = params.get(PROMPT_FILE);

    if (inlinePrompt == null && promptFilePath == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Either 'prompt' or 'promptFile' must be provided");
    }
    if (inlinePrompt != null && promptFilePath != null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Only one of 'prompt' or 'promptFile' can be provided, not both");
    }
    if (inlinePrompt != null) {
      validatePromptPlaceholders(inlinePrompt, inputFields);
      this.promptText = inlinePrompt;
    }
    this.promptFile = promptFilePath;
  }

  @Override
  public void inform(SolrCore core) {
    final SolrResourceLoader solrResourceLoader = core.getResourceLoader();
    LargeLanguageModelStore.registerManagedLargeLanguageModelStore(solrResourceLoader, this);
    if (promptFile != null) {
      try (InputStream is = solrResourceLoader.openResource(promptFile)) {
        promptText = new String(is.readAllBytes(), StandardCharsets.UTF_8).trim();
      } catch (IOException e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "Cannot read prompt file: " + promptFile, e);
      }
      validatePromptPlaceholders(promptText, inputFields);
    }
  }

  @Override
  public void onManagedResourceInitialized(NamedList<?> args, ManagedResource res)
      throws SolrException {
    if (res instanceof LargeLanguageModelStore store) {
      store.loadStoredModels();
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

    ResponseFormat responseFormat = getJsonSchema(outputFieldSchema);
    boolean multiValued = outputFieldSchema.multiValued();

    LargeLanguageModelStore store = LargeLanguageModelStore.getManagedModelStore(req.getCore());
    SolrLargeLanguageModel fieldGenerationModel = store.getModel(modelName);
    if (fieldGenerationModel == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "The model configured in the Update Request Processor '"
              + modelName
              + "' can't be found in the store: "
              + LargeLanguageModelStore.REST_END_POINT);
    }

    return new DocumentEnrichmentUpdateProcessor(
        inputFields,
        outputField,
        promptText,
        fieldGenerationModel,
        multiValued,
        responseFormat,
        req,
        next);
  }

  /**
   * Builds a {@link ResponseFormat} that instructs the model to return a JSON object {@code
   * {"value": ...}} whose value type matches the Solr field type. For multivalued fields the value
   * is wrapped in a {@link JsonArraySchema} nested inside the root {@link JsonObjectSchema}.
   *
   * <p>Nesting {@link JsonArraySchema} inside a {@link JsonObjectSchema} property is supported by
   * all langchain4j providers that implement structured outputs with {@link JsonObjectSchema}
   * (OpenAI, Azure OpenAI, Google AI, Gemini, Mistral, Ollama, Amazon Bedrock, Watsonx).
   */
  static ResponseFormat getJsonSchema(SchemaField schemaField) {
    JsonSchemaElement valueElement = toJsonSchemaElement(schemaField.getType());
    JsonSchemaElement valueSchema =
        schemaField.multiValued()
            ? JsonArraySchema.builder().items(valueElement).build()
            : valueElement;
    // estrai costanti output e value
    return ResponseFormat.builder()
        .type(ResponseFormatType.JSON)
        .jsonSchema(
            JsonSchema.builder()
                .name(JSON_SCHEMA_NAME)
                .rootElement(
                    JsonObjectSchema.builder()
                        .addProperty(JSON_FIELD_PROPERTY, valueSchema)
                        .required(JSON_FIELD_PROPERTY)
                        .build())
                .build())
        .build();
  }

  private static JsonSchemaElement toJsonSchemaElement(FieldType fieldType) {
    SolrException unsupportedFieldTypeException =
        new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "field type is not supported by Document Enrichment: "
                + fieldType.getClass().getSimpleName());

    return switch (fieldType) {
        // first check unsupported types and throw SolrException
      case DenseVectorField f -> throw unsupportedFieldTypeException;
      case UUIDField f -> throw unsupportedFieldTypeException;
      case NestPathField f -> throw unsupportedFieldTypeException;

        // build JsonSchemaElement for supported types
      case StrField f -> new JsonStringSchema();
      case TextField f -> new JsonStringSchema();
      case DatePointField f -> new JsonStringSchema();

      case IntPointField f -> new JsonIntegerSchema();
      case LongPointField f -> new JsonIntegerSchema();

      case FloatPointField f -> new JsonNumberSchema();
      case DoublePointField f -> new JsonNumberSchema();

      case BoolField f -> new JsonBooleanSchema();

        // fall-back to SolrException
      default -> throw unsupportedFieldTypeException;
    };
  }

  private static void validatePromptPlaceholders(String prompt, List<String> inputFields) {
    Set<String> promptPlaceholders = new HashSet<>();
    Matcher matcher = PLACEHOLDER_PATTERN.matcher(prompt);
    while (matcher.find()) {
      promptPlaceholders.add(matcher.group(1));
    }

    Set<String> fieldsWithoutPlaceholderInPrompt = new HashSet<>(inputFields);
    fieldsWithoutPlaceholderInPrompt.removeAll(promptPlaceholders);
    if (!fieldsWithoutPlaceholderInPrompt.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "prompt is missing placeholders for inputField(s): " + fieldsWithoutPlaceholderInPrompt);
    }

    Set<String> placeholdersInPromptWithoutField = new HashSet<>(promptPlaceholders);
    placeholdersInPromptWithoutField.removeAll(new HashSet<>(inputFields));
    if (!placeholdersInPromptWithoutField.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "prompt contains placeholders not declared as inputField(s): "
              + placeholdersInPromptWithoutField);
    }
  }

  public List<String> getInputFields() {
    return inputFields;
  }

  public String getOutputField() {
    return outputField;
  }

  public String getPrompt() {
    return promptText;
  }

  public String getModelName() {
    return modelName;
  }

  public String getPromptFile() {
    return promptFile;
  }
}
