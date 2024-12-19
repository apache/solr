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
package org.apache.solr.llm.texttovector.store.rest;

import java.lang.invoke.MethodHandles;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import net.jcip.annotations.ThreadSafe;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.llm.texttovector.model.SolrTextToVectorModel;
import org.apache.solr.llm.texttovector.store.TextToVectorModelException;
import org.apache.solr.llm.texttovector.store.TextToVectorModelStore;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.rest.BaseSolrResource;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceObserver;
import org.apache.solr.rest.ManagedResourceStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Managed Resource wrapper for the the {@link TextToVectorModelStore} to expose it via REST */
@ThreadSafe
public class ManagedTextToVectorModelStore extends ManagedResource
    implements ManagedResource.ChildResourceSupport {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** the model store rest endpoint */
  public static final String REST_END_POINT = "/schema/text-to-vector-model-store";

  /** Managed model store: the name of the attribute containing all the models of a model store */
  private static final String MODELS_JSON_FIELD = "models";

  /** name of the attribute containing a class */
  static final String CLASS_KEY = "class";

  /** name of the attribute containing a name */
  static final String NAME_KEY = "name";

  /** name of the attribute containing parameters */
  static final String PARAMS_KEY = "params";

  public static void registerManagedTextToVectorModelStore(
      SolrResourceLoader solrResourceLoader, ManagedResourceObserver managedResourceObserver) {
    solrResourceLoader
        .getManagedResourceRegistry()
        .registerManagedResource(
            REST_END_POINT, ManagedTextToVectorModelStore.class, managedResourceObserver);
  }

  public static ManagedTextToVectorModelStore getManagedModelStore(SolrCore core) {
    return (ManagedTextToVectorModelStore) core.getRestManager().getManagedResource(REST_END_POINT);
  }

  /**
   * Returns the available models as a list of Maps objects. After an update the managed resources
   * needs to return the resources in this format in order to store in json somewhere (zookeeper,
   * disk...)
   *
   * @return the available models as a list of Maps objects
   */
  private static List<Object> modelsAsManagedResources(List<SolrTextToVectorModel> models) {
    return models.stream()
        .map(ManagedTextToVectorModelStore::toModelMap)
        .collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  public static SolrTextToVectorModel fromModelMap(
      SolrResourceLoader solrResourceLoader, Map<String, Object> embeddingModel) {
    return SolrTextToVectorModel.getInstance(
        solrResourceLoader,
        (String) embeddingModel.get(CLASS_KEY), // modelClassName
        (String) embeddingModel.get(NAME_KEY), // modelName
        (Map<String, Object>) embeddingModel.get(PARAMS_KEY));
  }

  private static LinkedHashMap<String, Object> toModelMap(SolrTextToVectorModel model) {
    final LinkedHashMap<String, Object> modelMap = new LinkedHashMap<>(5, 1.0f);
    modelMap.put(NAME_KEY, model.getName());
    modelMap.put(CLASS_KEY, model.getEmbeddingModelClassName());
    modelMap.put(PARAMS_KEY, model.getParams());
    return modelMap;
  }

  private final TextToVectorModelStore store;
  private Object managedData;

  public ManagedTextToVectorModelStore(
      String resourceId, SolrResourceLoader loader, ManagedResourceStorage.StorageIO storageIO)
      throws SolrException {
    super(resourceId, loader, storageIO);
    store = new TextToVectorModelStore();
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
      @SuppressWarnings({"unchecked"})
      final List<Map<String, Object>> textToVectorModels = (List<Map<String, Object>>) managedData;
      for (final Map<String, Object> textToVectorModel : textToVectorModels) {
        addModelFromMap(textToVectorModel);
      }
    }
  }

  private void addModelFromMap(Map<String, Object> modelMap) {
    try {
      addModel(fromModelMap(solrResourceLoader, modelMap));
    } catch (final TextToVectorModelException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }

  public void addModel(SolrTextToVectorModel model) throws TextToVectorModelException {
    try {
      if (log.isInfoEnabled()) {
        log.info("adding model {}", model.getName());
      }
      store.addModel(model);
    } catch (final TextToVectorModelException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Object applyUpdatesToManagedData(Object updates) {
    if (updates instanceof List) {
      final List<Map<String, Object>> textToVectorModels = (List<Map<String, Object>>) updates;
      for (final Map<String, Object> textToVectorModel : textToVectorModels) {
        addModelFromMap(textToVectorModel);
      }
    }

    if (updates instanceof Map) {
      final Map<String, Object> map = (Map<String, Object>) updates;
      addModelFromMap(map);
    }

    return modelsAsManagedResources(store.getModels());
  }

  @Override
  public void doDeleteChild(BaseSolrResource endpoint, String childId) {
    store.delete(childId);
    storeManagedData(applyUpdatesToManagedData(null));
  }

  /**
   * Called to retrieve a named part (the given childId) of the resource at the given endpoint.
   * Note: since we have a unique child managed store we ignore the childId.
   */
  @Override
  public void doGet(BaseSolrResource endpoint, String childId) {
    final SolrQueryResponse response = endpoint.getSolrResponse();
    response.add(MODELS_JSON_FIELD, modelsAsManagedResources(store.getModels()));
  }

  public SolrTextToVectorModel getModel(String modelName) {
    return store.getModel(modelName);
  }

  @Override
  public String toString() {
    return "ManagedModelStore [store=" + store + "]";
  }
}
