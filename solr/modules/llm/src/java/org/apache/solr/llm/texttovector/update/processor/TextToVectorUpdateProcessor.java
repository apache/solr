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
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.llm.texttovector.model.SolrTextToVectorModel;
import org.apache.solr.llm.texttovector.store.rest.ManagedTextToVectorModelStore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;


class TextToVectorUpdateProcessor extends UpdateRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final String inputField;
    private final String outputField;
    private final String model;
    private SolrTextToVectorModel textToVector;
    private ManagedTextToVectorModelStore modelStore = null;

    public TextToVectorUpdateProcessor(
            String inputField,
            String outputField,
            String model,
            SolrQueryRequest req,
            UpdateRequestProcessor next) {
        super(next);
        this.inputField = inputField;
        this.outputField = outputField;
        this.model = model;
        this.modelStore = ManagedTextToVectorModelStore.getManagedModelStore(req.getCore());
    }

    /**
     * @param cmd the update command in input containing the Document to process
     * @throws IOException If there is a low-level I/O error
     */
    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
        this.textToVector = modelStore.getModel(model);
        if (textToVector == null) {
            throw new SolrException(
                    SolrException.ErrorCode.BAD_REQUEST,
                    "The model requested '"
                            + model
                            + "' can't be found in the store: "
                            + ManagedTextToVectorModelStore.REST_END_POINT);
        }

        SolrInputDocument doc = cmd.getSolrInputDocument();
        SolrInputField inputFieldContent = doc.get(inputField);
        if (!isNullOrEmpty(inputFieldContent, doc, inputField)) {
            String textToVectorise = inputFieldContent.getValue().toString();//add null checks and
            float[] vector = textToVector.vectorise(textToVectorise);
            List<Float> vectorAsList = new ArrayList<Float>(vector.length);
            for (float f : vector) {
                vectorAsList.add(f);
            }
            doc.addField(outputField, vectorAsList);
        }
        super.processAdd(cmd);
    }

    protected boolean isNullOrEmpty(SolrInputField inputFieldContent, SolrInputDocument doc, String fieldName) {
        if (inputFieldContent == null || inputFieldContent.getValue() == null) {
            log.warn("the input field: " + fieldName + " is missing for the document: " + doc.toString());
            return true;
        } else if (inputFieldContent.getValue().toString().isEmpty()) {
            log.warn("the input field: " + fieldName + " is empty (string instance of zero length) for the document: " + doc.toString());
            return true;
        } else {
            return false;
        }
    }
}
