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

package org.apache.solr.handler.api;

import org.apache.solr.api.ApiBag;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.api.AddReplicaAPI;
import org.apache.solr.handler.admin.api.AddReplicaPropertyAPI;
import org.apache.solr.handler.admin.api.BalanceShardUniqueAPI;
import org.apache.solr.handler.admin.api.CollectionStatusAPI;
import org.apache.solr.handler.admin.api.CreateShardAPI;
import org.apache.solr.handler.admin.api.DeleteCollectionAPI;
import org.apache.solr.handler.admin.api.DeleteReplicaAPI;
import org.apache.solr.handler.admin.api.DeleteReplicaPropertyAPI;
import org.apache.solr.handler.admin.api.DeleteShardAPI;
import org.apache.solr.handler.admin.api.ForceLeaderAPI;
import org.apache.solr.handler.admin.api.MigrateDocsAPI;
import org.apache.solr.handler.admin.api.ModifyCollectionAPI;
import org.apache.solr.handler.admin.api.MoveReplicaAPI;
import org.apache.solr.handler.admin.api.RebalanceLeadersAPI;
import org.apache.solr.handler.admin.api.ReloadCollectionAPI;
import org.apache.solr.handler.admin.api.SetCollectionPropertyAPI;
import org.apache.solr.handler.admin.api.SplitShardAPI;
import org.apache.solr.handler.admin.api.SyncShardAPI;
import org.apache.solr.handler.configsets.CreateConfigSetAPI;
import org.apache.solr.handler.configsets.DeleteConfigSetAPI;
import org.apache.solr.handler.configsets.ListConfigSetsAPI;
import org.apache.solr.handler.configsets.UploadConfigSetAPI;
import org.apache.solr.handler.configsets.UploadConfigSetFileAPI;

/**
 * Registers annotation-based V2 APIs with an {@link ApiBag}
 *
 * <p>Historically these APIs were registered directly by code in {@link
 * org.apache.solr.core.CoreContainer}, but as the number of annotation-based v2 APIs grew this
 * became increasingly unwieldy. So {@link ApiRegistrar} serves as a single place where all APIs
 * under a particular path can be registered together.
 */
public class ApiRegistrar {

  public static void registerCollectionApis(ApiBag apiBag, CollectionsHandler collectionsHandler) {
    apiBag.registerObject(new AddReplicaPropertyAPI(collectionsHandler));
    apiBag.registerObject(new BalanceShardUniqueAPI(collectionsHandler));
    apiBag.registerObject(new DeleteCollectionAPI(collectionsHandler));
    apiBag.registerObject(new DeleteReplicaPropertyAPI(collectionsHandler));
    apiBag.registerObject(new MigrateDocsAPI(collectionsHandler));
    apiBag.registerObject(new ModifyCollectionAPI(collectionsHandler));
    apiBag.registerObject(new MoveReplicaAPI(collectionsHandler));
    apiBag.registerObject(new RebalanceLeadersAPI(collectionsHandler));
    apiBag.registerObject(new ReloadCollectionAPI(collectionsHandler));
    apiBag.registerObject(new SetCollectionPropertyAPI(collectionsHandler));
    apiBag.registerObject(new CollectionStatusAPI(collectionsHandler));
  }

  public static void registerShardApis(ApiBag apiBag, CollectionsHandler collectionsHandler) {
    apiBag.registerObject(new SplitShardAPI(collectionsHandler));
    apiBag.registerObject(new CreateShardAPI(collectionsHandler));
    apiBag.registerObject(new AddReplicaAPI(collectionsHandler));
    apiBag.registerObject(new DeleteShardAPI(collectionsHandler));
    apiBag.registerObject(new SyncShardAPI(collectionsHandler));
    apiBag.registerObject(new ForceLeaderAPI(collectionsHandler));
    // really this is a replica API, but since there's only 1 API on the replica path, it's included
    // here for simplicity.
    apiBag.registerObject(new DeleteReplicaAPI(collectionsHandler));
  }

  public static void registerConfigsetApis(ApiBag apiBag, CoreContainer container) {
    apiBag.registerObject(new CreateConfigSetAPI(container));
    apiBag.registerObject(new DeleteConfigSetAPI(container));
    apiBag.registerObject(new ListConfigSetsAPI(container));
    apiBag.registerObject(new UploadConfigSetAPI(container));
    apiBag.registerObject(new UploadConfigSetFileAPI(container));
  }
}
