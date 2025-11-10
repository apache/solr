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
package org.apache.solr.handler.admin.api;

import static org.apache.solr.security.PermissionNameProvider.Name.COLL_READ_PERM;

import jakarta.inject.Inject;
import java.util.Collection;
import java.util.Map;
import org.apache.solr.client.api.endpoint.CollectionSnapshotApis;
import org.apache.solr.client.api.model.ListCollectionSnapshotsResponse;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData;
import org.apache.solr.core.snapshots.SolrSnapshotManager;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/** V2 API implementation for Listing Collection Snapshots. */
public class ListCollectionSnapshots extends AdminAPIBase implements CollectionSnapshotApis.List {

  @Inject
  public ListCollectionSnapshots(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  /** This API is analogous to V1's (POST /solr/admin/collections?action=LISTSNAPSHOTS) */
  @Override
  @PermissionName(COLL_READ_PERM)
  public ListCollectionSnapshotsResponse listSnapshots(String collName) throws Exception {

    final ListCollectionSnapshotsResponse response =
        instantiateJerseyResponse(ListCollectionSnapshotsResponse.class);
    final CoreContainer coreContainer = fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(collName, solrQueryRequest);

    final String collectionName = resolveCollectionName(collName, true);

    SolrZkClient client = coreContainer.getZkController().getZkClient();
    Collection<CollectionSnapshotMetaData> m =
        SolrSnapshotManager.listSnapshots(client, collectionName);

    final Map<String, Object> snapshots = CollectionUtil.newHashMap(m.size());
    for (CollectionSnapshotMetaData metaData : m) {
      snapshots.put(metaData.getName(), metaData);
    }
    response.snapshots = snapshots;

    return response;
  }
}
