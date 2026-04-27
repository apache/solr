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
package org.apache.solr.languagemodels.store.rest;

import java.lang.invoke.MethodHandles;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import net.jcip.annotations.ThreadSafe;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.languagemodels.model.SolrLanguageModel;
import org.apache.solr.languagemodels.store.LanguageModelException;
import org.apache.solr.languagemodels.store.LanguageModelStore;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.rest.BaseSolrResource;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for {@link ManagedResource} wrappers that expose a {@link LanguageModelStore}
 * via the REST API. Concrete subclasses supply the REST endpoint and the model instantiation logic.
 */
@ThreadSafe
public abstract class ManagedModelStore<M extends SolrLanguageModel> extends ManagedResource
    implements ManagedResource.ChildResourceSupport {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String MODELS_JSON_FIELD = "models";

  protected static final String CLASS_KEY = "class";
  protected static final String NAME_KEY = "name";
  protected static final String PARAMS_KEY = "params";

  private final LanguageModelStore<M> store;
  private Object managedData;

  protected ManagedModelStore(
      String resourceId, SolrResourceLoader loader, ManagedResourceStorage.StorageIO storageIO)
      throws SolrException {
    super(resourceId, loader, storageIO);
    store = new LanguageModelStore<>();
  }

  /**
   * Creates a model instance from the JSON map persisted in the managed resource storage.
   *
   * @param loader the resource loader for the current core
   * @param modelMap a map containing {@code "class"}, {@code "name"}, and {@code "params"} keys
   * @return the instantiated model
   */
  protected abstract M fromModelMap(SolrResourceLoader loader, Map<String, Object> modelMap);

  private static LinkedHashMap<String, Object> toModelMap(SolrLanguageModel model) {
    final LinkedHashMap<String, Object> modelMap = new LinkedHashMap<>(5, 1.0f);
    modelMap.put(NAME_KEY, model.getName());
    modelMap.put(CLASS_KEY, model.getModelClassName());
    modelMap.put(PARAMS_KEY, model.getParams());
    return modelMap;
  }

  @Override
  protected ManagedResourceStorage createStorage(
      ManagedResourceStorage.StorageIO storageIO, SolrResourceLoader loader) throws SolrException {
    return new ManagedResourceStorage.JsonStorage(storageIO, loader, -1);
  }

  @Override
  protected void onManagedDataLoadedFromStorage(NamedList<?> managedInitArgs, Object managedData)
      throws SolrException {
    store.clear();
    this.managedData = managedData;
  }

  public void loadStoredModels() {
    log.info("------ managed models ~ loading ------");
    if ((managedData != null) && (managedData instanceof List)) {
      @SuppressWarnings("unchecked")
      final List<Map<String, Object>> models = (List<Map<String, Object>>) managedData;
      for (final Map<String, Object> model : models) {
        addModelFromMap(model);
      }
    }
  }

  private void addModelFromMap(Map<String, Object> modelMap) {
    try {
      addModel(fromModelMap(solrResourceLoader, modelMap));
    } catch (final LanguageModelException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }

  public void addModel(M model) throws SolrException {
    try {
      if (log.isInfoEnabled()) {
        log.info("adding model {}", model.getName());
      }
      store.addModel(model);
    } catch (final LanguageModelException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Object applyUpdatesToManagedData(Object updates) {
    if (updates instanceof List) {
      final List<Map<String, Object>> models = (List<Map<String, Object>>) updates;
      for (final Map<String, Object> model : models) {
        addModelFromMap(model);
      }
    }
    if (updates instanceof Map) {
      addModelFromMap((Map<String, Object>) updates);
    }
    return modelsAsManagedResources(store.getModels());
  }

  @Override
  public void doDeleteChild(BaseSolrResource endpoint, String childId) {
    store.delete(childId);
    storeManagedData(applyUpdatesToManagedData(null));
  }

  @Override
  public void doGet(BaseSolrResource endpoint, String childId) {
    final SolrQueryResponse response = endpoint.getSolrResponse();
    response.add(MODELS_JSON_FIELD, modelsAsManagedResources(store.getModels()));
  }

  public M getModel(String modelName) {
    return store.getModel(modelName);
  }

  private static List<Object> modelsAsManagedResources(List<? extends SolrLanguageModel> models) {
    return models.stream().map(ManagedModelStore::toModelMap).collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " [store=" + store + "]";
  }
}
