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

import java.util.Map;
import net.jcip.annotations.ThreadSafe;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.languagemodels.model.SolrLargeLanguageModel;
import org.apache.solr.rest.ManagedResourceObserver;
import org.apache.solr.rest.ManagedResourceStorage;

/** Managed Resource wrapper for the large language model store, exposed via REST */
@ThreadSafe
public class ManagedLargeLanguageModelStore
    extends ManagedLanguageModelStore<SolrLargeLanguageModel> {
  /** the model store rest endpoint */
  public static final String REST_END_POINT = "/schema/large-language-model-store";

  public static void registerManagedLargeLanguageModelStore(
      SolrResourceLoader solrResourceLoader, ManagedResourceObserver managedResourceObserver) {
    solrResourceLoader
        .getManagedResourceRegistry()
        .registerManagedResource(
            REST_END_POINT, ManagedLargeLanguageModelStore.class, managedResourceObserver);
  }

  public static ManagedLargeLanguageModelStore getManagedModelStore(SolrCore core) {
    return (ManagedLargeLanguageModelStore)
        core.getRestManager().getManagedResource(REST_END_POINT);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected SolrLargeLanguageModel fromModelMap(
      SolrResourceLoader solrResourceLoader, Map<String, Object> modelMap) {
    return SolrLargeLanguageModel.getInstance(
        solrResourceLoader,
        (String) modelMap.get(CLASS_KEY),
        (String) modelMap.get(NAME_KEY),
        (Map<String, Object>) modelMap.get(PARAMS_KEY));
  }

  public ManagedLargeLanguageModelStore(
      String resourceId, SolrResourceLoader loader, ManagedResourceStorage.StorageIO storageIO)
      throws SolrException {
    super(resourceId, loader, storageIO);
  }
}
