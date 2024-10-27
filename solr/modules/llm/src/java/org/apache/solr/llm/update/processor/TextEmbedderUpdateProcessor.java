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

package org.apache.solr.llm.update.processor;

import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.llm.embedding.SolrEmbeddingModel;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;

import java.io.IOException;

class TextEmbedderUpdateProcessor extends UpdateRequestProcessor {
  private final String inputField;
  private final String outputField;
  private SolrEmbeddingModel embedder;

  
  public TextEmbedderUpdateProcessor(
      String inputField,
      String outputField,
      SolrEmbeddingModel embedder,
      UpdateRequestProcessor next) {
    super(next);
    this.inputField = inputField;
    this.outputField = outputField;
    this.embedder = embedder;
  }

  /**
   * @param cmd the update command in input containing the Document to classify
   * @throws IOException If there is a low-level I/O error
   */
  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    SolrInputDocument doc = cmd.getSolrInputDocument();
    String textToEmbed = doc.get(inputField).toString();
    float[] vector = embedder.vectorise(textToEmbed);
    doc.addField(outputField, vector);
    super.processAdd(cmd);
  }
}
