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
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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
import org.apache.solr.schema.StrField;
import org.apache.solr.schema.TextField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * Insert in an existing field the output of the model coming from a textual field value.
 *
 * <p>The parameters supported are:
 *
 * <pre class="prettyprint" >
 * &lt;processor class=&quot;solr.llm.documentenrichment.update.processor.DocumentEnrichmentUpdateProcessorFactory&quot;&gt;
 *   &lt;str name=&quot;inputField&quot;&gt;textualField&lt;/str&gt;
 *   &lt;str name=&quot;outputField&quot;&gt;anotherTextualField&lt;/str&gt;
 *   &lt;str name=&quot;prompt&quot;&gt;Summarize: {input}&lt;/str&gt;
 *   &lt;str name=&quot;model&quot;&gt;ChatModel&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 *
 * <p>Alternatively, the prompt can be loaded from a text file using {@code promptFile}:
 *
 * <pre class="prettyprint" >
 * &lt;processor class=&quot;solr.llm.documentenrichment.update.processor.DocumentEnrichmentUpdateProcessorFactory&quot;&gt;
 *   &lt;str name=&quot;inputField&quot;&gt;textualField&lt;/str&gt;
 *   &lt;str name=&quot;outputField&quot;&gt;anotherTextualField&lt;/str&gt;
 *   &lt;str name=&quot;promptFile&quot;&gt;prompt.txt&lt;/str&gt;
 *   &lt;str name=&quot;model&quot;&gt;ChatModel&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 *
 * <p>Exactly one of {@code prompt} or {@code promptFile} must be provided. The prompt (from either
 * source) must contain the {@code {input}} placeholder.
 */
public class DocumentEnrichmentUpdateProcessorFactory extends UpdateRequestProcessorFactory
    implements SolrCoreAware, ManagedResourceObserver {
  private static final String INPUT_FIELD_PARAM = "inputField";
  private static final String OUTPUT_FIELD_PARAM = "outputField";
  private static final String PROMPT = "prompt";
  private static final String PROMPT_FILE = "promptFile";
  private static final String MODEL_NAME = "model";
  private ManagedChatModelStore modelStore = null;

  private String inputField; // TODO: change with a list of input fields (check how it's done in other UpdateProcessor that supports this behaviour)
  private String outputField;
  private String prompt;
  private String promptFile;
  private String modelName;
  private SolrParams params;

  @Override
  public void init(final NamedList<?> args) {
    params = args.toSolrParams();
    RequiredSolrParams required = params.required();
    inputField = required.get(INPUT_FIELD_PARAM);
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
      if (!inlinePrompt.contains("{input}")) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "prompt must contain {input} placeholder");
      }
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
      if (!prompt.contains("{input}")) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "prompt must contain {input} placeholder");
      }
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

    if (!latestSchema.isDynamicField(inputField) && !latestSchema.hasExplicitField(inputField)) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "undefined field: \"" + inputField + "\"");
    }

    final SchemaField outputFieldSchema = latestSchema.getField(outputField);
    assertIsTextualField(outputFieldSchema);

    ManagedChatModelStore modelStore =
        ManagedChatModelStore.getManagedModelStore(req.getCore());
    SolrChatModel chatModel = modelStore.getModel(modelName);
    if (chatModel == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "The model configured in the Update Request Processor '"
              + modelName
              + "' can't be found in the store: "
              + ManagedChatModelStore.REST_END_POINT);
    }

    return new DocumentEnrichmentUpdateProcessor(inputField, outputField, prompt, chatModel, req, next);
  }
  // This is used on the outputField. Now the support is limited. Can be changed with structured outputs.
  protected void assertIsTextualField(SchemaField schemaField) {
    FieldType fieldType = schemaField.getType();
    if (!(fieldType instanceof StrField) && !(fieldType instanceof TextField)) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "only textual fields are compatible with Document Enrichment: "
              + schemaField.getName());
    }
  }

  public String getInputField() {
    return inputField;
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
