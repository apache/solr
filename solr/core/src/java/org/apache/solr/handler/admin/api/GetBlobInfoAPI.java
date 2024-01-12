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

import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.security.PermissionNameProvider.Name.READ_PERM;

import org.apache.solr.api.EndPoint;
import org.apache.solr.handler.BlobHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 APIs for fetching blob(s) and their metadata
 *
 * <p>These APIs (GET /v2/collections/.system/blob/*) is analogous to the v1 GET
 * /solr/.system/blob/* APIs.
 */
public class GetBlobInfoAPI {
  private BlobHandler blobHandler;

  public GetBlobInfoAPI(BlobHandler blobHandler) {
    this.blobHandler = blobHandler;
  }

  @EndPoint(
      path = {"/blob"},
      method = GET,
      permission = READ_PERM)
  public void getAllBlobs(SolrQueryRequest req, SolrQueryResponse rsp) {
    blobHandler.handleRequest(req, rsp);
  }

  @EndPoint(
      path = {"/blob/{blobName}"},
      method = GET,
      permission = READ_PERM)
  public void getBlobByName(SolrQueryRequest req, SolrQueryResponse rsp) {
    blobHandler.handleRequest(req, rsp);
  }

  @EndPoint(
      path = {"/blob/{blobName}/{blobVersion}"},
      method = GET,
      permission = READ_PERM)
  public void getVersionedBlobByName(SolrQueryRequest req, SolrQueryResponse rsp) {
    blobHandler.handleRequest(req, rsp);
  }
}
