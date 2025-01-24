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

import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.llm.texttovector.model.SolrTextToVectorModel;
import org.apache.solr.llm.texttovector.store.rest.ManagedTextToVectorModelStore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceObserver;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


class TexToVectorUpdateProcessor extends UpdateRequestProcessor{
  private final String inputField;
  private final String outputField;
  private final String model;
  private SolrTextToVectorModel textToVector;
  private ManagedTextToVectorModelStore modelStore = null;
  
  public TexToVectorUpdateProcessor(
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
   * @param cmd the update command in input containing the Document to classify
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
    String textToEmbed = doc.get(inputField).getValue().toString();//add null checks and
   
    float[] vector = textToVector.vectorise(textToEmbed);
    List<Float> vectorAsList = new ArrayList<Float>(vector.length);
    for (float f : vector) {
      vectorAsList.add(f);
    }
    
    doc.addField(outputField, vectorAsList);
    super.processAdd(cmd);
  }
}
