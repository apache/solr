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
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.api.*;

/**
 * Registers annotation-based V2 APIs with an {@link ApiBag}
 *
 * Historically these APIs were registered directly by code in {@link org.apache.solr.core.CoreContainer}, but as the
 * number of annotation-based v2 APIs grew this became increasingly unwieldy.  So {@link ApiRegistrar} serves as a single
 * place where all APIs under a particular path can be registered together.
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
  }
}
