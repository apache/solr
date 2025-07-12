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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.llm.textvectorisation.model.SolrTextToVectorModel;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.zookeeper.common.StringUtils;
import org.eclipse.jetty.http2.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TextToVectorUpdateProcessor extends UpdateRequestProcessor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final IndexSchema schema;
  private final String inputField;
  private final String outputField;
  private final SolrTextToVectorModel textToVector;
  private final String[] additionalInputFields;

  public TextToVectorUpdateProcessor(
      String inputField,
      String[] additionalInputFields,
      String outputField,
      SolrTextToVectorModel textToVector,
      SolrQueryRequest req,
      UpdateRequestProcessor next) {
    super(next);
    this.schema = req.getSchema();
    this.inputField = inputField;
    this.outputField = outputField;
    this.textToVector = textToVector;

    this.additionalInputFields = additionalInputFields;
  }

  /**
   * @param cmd the update command in input containing the Document to process
   * @throws IOException If there is a low-level I/O error
   */
  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    SolrInputDocument newDoc = cmd.getSolrInputDocument();

    SolrInputField inputFieldContent = newDoc.get(inputField);

    ArrayList<String> allFields = new ArrayList<>(Arrays.asList(additionalInputFields));
    allFields.add(inputField);
    Collections.reverse(allFields);

    //todo do we really need to create a map?
    String contentFromNewDoc = concatenatedFieldContent(newDoc.toMap(new HashMap<>()), allFields);

    if (!isNullOrEmpty(inputFieldContent) || !StringUtils.isBlank(contentFromNewDoc)) {
      try {

        SolrDocument oldDoc =
            getCurrentSolrDocFromCore(
                cmd.getReq().getCore(), newDoc.getField("id").getValue().toString(), allFields);

        List<Float> targetVector = getFreshVectorIfChanged(oldDoc, allFields, contentFromNewDoc);

        newDoc.addField(outputField, targetVector);
      } catch (RuntimeException vectorisationFailure) {
        if (log.isErrorEnabled()) {
          SchemaField uniqueKeyField = schema.getUniqueKeyField();
          String uniqueKeyFieldName = uniqueKeyField.getName();
          log.error(
              "Could not vectorise: {} for the document with {}: {}",
              inputField,
              uniqueKeyFieldName,
              newDoc.getFieldValue(uniqueKeyFieldName),
              vectorisationFailure);
        }
      }
    }
    super.processAdd(cmd);
  }

  @SuppressWarnings("unchecked")
  private List<Float> getFreshVectorIfChanged(
      SolrDocument oldDoc, ArrayList<String> allFields, String contentFromNewDoc) {

    if (oldDoc != null && oldDoc.getFieldValue(outputField) != null) {

      String contentOldDoc = concatenatedFieldContent(oldDoc, allFields);
      //no need to create vector if old content is equals to new content
      if (contentFromNewDoc.equals(contentOldDoc)) {
        Object fieldValue = oldDoc.getFieldValue(outputField);
        return (List<Float>) fieldValue;
      }
    }
    return vectorize(contentFromNewDoc);
  }

  private List<Float> vectorize(String textToVectorise) {
    float[] vector = textToVector.vectorise(textToVectorise);
    List<Float> vectorAsList = new ArrayList<>(vector.length);
    for (float f : vector) {
      vectorAsList.add(f);
    }
    return vectorAsList;
  }

  private SolrDocument getCurrentSolrDocFromCore(SolrCore core, String id, List<String> allFields) {

    long t1 = System.currentTimeMillis();
    SolrClient solrClient = new EmbeddedSolrServer(core);
    log.info("create SolrClient took {}", System.currentTimeMillis() - t1);

    // we need to make a copy since we add an element to it
    allFields = new ArrayList<>(allFields);
    allFields.add(outputField);

    ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set("q", "id:" + id);
    queryParams.set("fl", allFields.toArray(new String[0]));

    t1 = System.currentTimeMillis();

    QueryResponse response;
    try {
      response = solrClient.query(queryParams);
    } catch (SolrServerException | IOException e) {
      log.error("error here");
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e.getMessage(), e);
    }

    log.info("create SolrClient took {}", System.currentTimeMillis() - t1);

    SolrDocumentList results = response.getResults();
    if (results.getNumFound() > 0) {
      return (results.getFirst());
    }
    return null;
  }

  private String concatenatedFieldContent(Map<String, Object> docFields, List<String> allFields) {
    if (additionalInputFields == null) {
      return "";
    }

    return allFields.stream()
        .map(docFields::get)
        .filter(Objects::nonNull)
        .map(Object::toString)
        .collect(Collectors.joining(" "));
  }

  protected boolean isNullOrEmpty(SolrInputField inputFieldContent) {
    return (inputFieldContent == null
        || inputFieldContent.getValue() == null
        || inputFieldContent.getValue().toString().isEmpty());
  }
}
