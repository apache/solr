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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.RequiredSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.llm.textvectorisation.model.SolrTextToVectorModel;
import org.apache.solr.llm.textvectorisation.store.rest.ManagedTextToVectorModelStore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.DenseVectorField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

/**
 * Vectorises a textual field value and add the resulting vector to another field.
 *
 * <p>The parameters supported are:
 *
 * <pre class="prettyprint" >
 * &lt;processor class=&quot;solr.llm.textvectorisation.update.processor.TextToVectorUpdateProcessorFactory&quot;&gt;
 *   &lt;str name=&quot;inputField&quot;&gt;textualField&lt;/str&gt;
 *   &lt;str name=&quot;outputField&quot;&gt;vectorField&lt;/str&gt;
 *   &lt;str name=&quot;model&quot;&gt;textToVectorModel&lt;/str&gt;
 * &lt;/processor&gt;
 * </pre>
 *
 * *
 */
public class TextToVectorUpdateProcessorFactory extends UpdateRequestProcessorFactory {
  private static final String INPUT_FIELD_PARAM = "inputField";
  private static final String OUTPUT_FIELD_PARAM = "outputField";
  private static final String MODEL_NAME = "model";

  private String inputField;
  private String outputField;
  private String modelName;
  private SolrParams params;

  @Override
  public void init(final NamedList<?> args) {
    params = args.toSolrParams();
    RequiredSolrParams required = params.required();
    inputField = required.get(INPUT_FIELD_PARAM);
    outputField = required.get(OUTPUT_FIELD_PARAM);
    modelName = required.get(MODEL_NAME);
  }

  @Override
  public UpdateRequestProcessor getInstance(
      SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    IndexSchema latestSchema = req.getCore().getLatestSchema();
    if (!latestSchema.hasExplicitField(inputField)) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "undefined field: \"" + inputField + "\"");
    }
    if (!latestSchema.hasExplicitField(outputField)) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "undefined field: \"" + outputField + "\"");
    }

    final SchemaField outputFieldSchema = latestSchema.getField(outputField);
    assertIsDenseVectorField(outputFieldSchema);

    ManagedTextToVectorModelStore modelStore =
        ManagedTextToVectorModelStore.getManagedModelStore(req.getCore());
    SolrTextToVectorModel textToVector = modelStore.getModel(modelName);
    if (textToVector == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "The model configured in the Update Request Processor '"
              + modelName
              + "' can't be found in the store: "
              + ManagedTextToVectorModelStore.REST_END_POINT);
    }

    return new TextToVectorUpdateProcessor(inputField, outputField, textToVector, req, next);
  }

  protected void assertIsDenseVectorField(SchemaField schemaField) {
    FieldType fieldType = schemaField.getType();
    if (!(fieldType instanceof DenseVectorField)) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "only DenseVectorField is compatible with Vector Query Parsers: "
              + schemaField.getName());
    }
  }

  public String getInputField() {
    return inputField;
  }

  public String getOutputField() {
    return outputField;
  }

  public String getModelName() {
    return modelName;
  }
}
