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
package org.apache.solr.llm.store;

import org.apache.solr.llm.embedding.SolrEmbeddingModel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class EmbeddingModelStore {

  private final Map<String, SolrEmbeddingModel> availableModels;

  public EmbeddingModelStore() {
    availableModels = new LinkedHashMap<>();
  }

  public synchronized SolrEmbeddingModel getModel(String name) {
    return availableModels.get(name);
  }

  public void clear() {
    availableModels.clear();
  }

  public List<SolrEmbeddingModel> getModels() {
    final List<SolrEmbeddingModel> availableModelsValues =
        new ArrayList<SolrEmbeddingModel>(availableModels.values());
    return Collections.unmodifiableList(availableModelsValues);
  }

  @Override
  public String toString() {
    return "ModelStore [availableModels=" + availableModels.keySet() + "]";
  }

  public SolrEmbeddingModel delete(String modelName) {
    return availableModels.remove(modelName);
  }

  public synchronized void addModel(SolrEmbeddingModel modeldata) throws EmbeddingModelException {
    final String name = modeldata.getName();
    if (availableModels.containsKey(name)) {
      throw new EmbeddingModelException("model '" + name + "' already exists. Please use a different name");
    }
    availableModels.put(modeldata.getName(), modeldata);
  }
}
