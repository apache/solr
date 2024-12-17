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

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;

import jakarta.inject.Inject;
import org.apache.solr.client.api.endpoint.CollectionStatusApi;
import org.apache.solr.client.api.model.CollectionStatusResponse;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.ColStatus;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJacksonMapper;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;

/** V2 API implementation for {@link CollectionStatusApi}. */
public class CollectionStatus extends AdminAPIBase implements CollectionStatusApi {

  @Inject
  public CollectionStatus(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.COLL_READ_PERM)
  public CollectionStatusResponse getCollectionStatus(
      String collectionName,
      Boolean coreInfo,
      Boolean segments,
      Boolean fieldInfo,
      Boolean rawSize,
      Boolean rawSizeSummary,
      Boolean rawSizeDetails,
      Float rawSizeSamplingPercent,
      Boolean sizeInfo)
      throws Exception {
    recordCollectionForLogAndTracing(collectionName, solrQueryRequest);

    final var params = new ModifiableSolrParams();
    params.add(COLLECTION_PROP, collectionName);
    params.setNonNull(ColStatus.CORE_INFO_PROP, coreInfo);
    params.setNonNull(ColStatus.SEGMENTS_PROP, segments);
    params.setNonNull(ColStatus.FIELD_INFO_PROP, fieldInfo);
    params.setNonNull(ColStatus.RAW_SIZE_PROP, rawSize);
    params.setNonNull(ColStatus.RAW_SIZE_SUMMARY_PROP, rawSizeSummary);
    params.setNonNull(ColStatus.RAW_SIZE_DETAILS_PROP, rawSizeDetails);
    params.setNonNull(ColStatus.RAW_SIZE_SAMPLING_PERCENT_PROP, rawSizeSamplingPercent);
    params.setNonNull(ColStatus.SIZE_INFO_PROP, sizeInfo);

    // TODO Push CollectionStatusResponse down into ColStatus and avoid the intermediate NL, if all
    // usages can be switched over.
    final var nlResponse = new NamedList<>();
    new ColStatus(
            coreContainer.getSolrClientCache(),
            coreContainer.getZkController().getZkStateReader().getClusterState(),
            new ZkNodeProps(params))
        .getColStatus(nlResponse);
    // collName is prop/key for a nested NL returned by ColStatus - extract the inner NL and
    // manually add name to resp
    final var colStatusResponse =
        SolrJacksonMapper.getObjectMapper()
            .convertValue(nlResponse.get(collectionName), CollectionStatusResponse.class);
    colStatusResponse.name = collectionName;
    return colStatusResponse;
  }
}
