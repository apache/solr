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
package org.apache.solr.languagemodels.textvectorisation.store.rest;

import java.util.Map;
import net.jcip.annotations.ThreadSafe;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.languagemodels.store.rest.ManagedLanguageModelStore;
import org.apache.solr.languagemodels.textvectorisation.model.SolrTextToVectorModel;
import org.apache.solr.rest.ManagedResourceObserver;
import org.apache.solr.rest.ManagedResourceStorage;

/** Managed Resource wrapper for the text-to-vector model store, exposed via REST */
@ThreadSafe
public class ManagedTextToVectorModelStore extends ManagedLanguageModelStore<SolrTextToVectorModel> {

  /** the model store rest endpoint */
  public static final String REST_END_POINT = "/schema/text-to-vector-model-store";

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

  @Override
  @SuppressWarnings("unchecked")
  protected SolrTextToVectorModel fromModelMap(
      SolrResourceLoader solrResourceLoader, Map<String, Object> embeddingModel) {
    return SolrTextToVectorModel.getInstance(
        solrResourceLoader,
        (String) embeddingModel.get(CLASS_KEY), // modelClassName
        (String) embeddingModel.get(NAME_KEY), // modelName
        (Map<String, Object>) embeddingModel.get(PARAMS_KEY));
  }

  public ManagedTextToVectorModelStore(
      String resourceId, SolrResourceLoader loader, ManagedResourceStorage.StorageIO storageIO)
      throws SolrException {
    super(resourceId, loader, storageIO);
  }
}
