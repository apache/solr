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

package org.apache.solr.llm.texttovector.update.processor;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.DenseVectorField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

/**
 * This class implements an UpdateProcessorFactory for the Text To Vector Update Processor.
 */
public class TextToVectorUpdateProcessorFactory extends UpdateRequestProcessorFactory {
    private static final String INPUT_FIELD_PARAM = "inputField";
    private static final String OUTPUT_FIELD_PARAM = "outputField";
    private static final String MODEL_NAME = "model";

    String inputField;
    String outputField;
    String modelName;
    SolrParams params;


    @Override
    public void init(final NamedList<?> args) {
        if (args != null) {
            params = args.toSolrParams();
            inputField = params.get(INPUT_FIELD_PARAM);
            checkNotNull(INPUT_FIELD_PARAM, inputField);

            outputField = params.get(OUTPUT_FIELD_PARAM);
            checkNotNull(OUTPUT_FIELD_PARAM, outputField);

            modelName = params.get(MODEL_NAME);
            checkNotNull(MODEL_NAME, modelName);
        }
    }

    private void checkNotNull(String paramName, Object param) {
        if (param == null) {
            throw new SolrException(
                    SolrException.ErrorCode.SERVER_ERROR,
                    "Text to Vector UpdateProcessor '" + paramName + "' can not be null");
        }
    }

    @Override
    public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
        req.getCore().getLatestSchema().getField(inputField);
        final SchemaField outputFieldSchema = req.getCore().getLatestSchema().getField(outputField);
        assertIsDenseVectorField(outputFieldSchema);

        return new TextToVectorUpdateProcessor(inputField, outputField, modelName, req, next);
    }

    protected void assertIsDenseVectorField(SchemaField schemaField) {
        FieldType fieldType = schemaField.getType();
        if (!(fieldType instanceof DenseVectorField)) {
            throw new SolrException(
                    SolrException.ErrorCode.SERVER_ERROR,
                    "only DenseVectorField is compatible with Vector Query Parsers: " + schemaField.getName());
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
