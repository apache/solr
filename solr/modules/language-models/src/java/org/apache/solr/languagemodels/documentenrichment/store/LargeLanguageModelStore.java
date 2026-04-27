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
package org.apache.solr.languagemodels.documentenrichment.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.languagemodels.documentenrichment.model.SolrLargeLanguageModel;

/** Simple store to manage CRUD operations on the {@link SolrLargeLanguageModel} */
public class LargeLanguageModelStore {

  private final Map<String, SolrLargeLanguageModel> availableModels;

  public LargeLanguageModelStore() {
    availableModels = Collections.synchronizedMap(new LinkedHashMap<>());
  }

  public SolrLargeLanguageModel getModel(String name) {
    return availableModels.get(name);
  }

  public void clear() {
    availableModels.clear();
  }

  public List<SolrLargeLanguageModel> getModels() {
    synchronized (availableModels) {
      final List<SolrLargeLanguageModel> availableModelsValues = new ArrayList<>(availableModels.values());
      return Collections.unmodifiableList(availableModelsValues);
    }
  }

  @Override
  public String toString() {
    return "LargeLanguageModelStore [availableModels=" + availableModels.keySet() + "]";
  }

  public SolrLargeLanguageModel delete(String modelName) {
    return availableModels.remove(modelName);
  }

  public void addModel(SolrLargeLanguageModel modeldata) throws LargeLanguageModelException {
    final String name = modeldata.getName();
    if (availableModels.putIfAbsent(modeldata.getName(), modeldata) != null) {
      throw new LargeLanguageModelException(
          "model '" + name + "' already exists. Please use a different name");
    }
  }
}
