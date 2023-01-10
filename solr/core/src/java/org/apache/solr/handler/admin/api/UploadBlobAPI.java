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

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.security.PermissionNameProvider.Name.UPDATE_PERM;

import org.apache.solr.api.EndPoint;
import org.apache.solr.handler.BlobHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for uploading blobs into Solr's .system blobstore
 *
 * <p>This API (POST /v2/collections/.system/blob/blobName) is analogous to the v1 POST
 * /solr/.system/blob/blobName API.
 */
public class UploadBlobAPI {
  private final BlobHandler blobHandler;

  public UploadBlobAPI(BlobHandler blobHandler) {
    this.blobHandler = blobHandler;
  }

  @EndPoint(
      path = {"/blob/{blobName}"},
      method = POST,
      permission = UPDATE_PERM)
  public void uploadBlob(SolrQueryRequest req, SolrQueryResponse rsp) {
    blobHandler.handleRequest(req, rsp);
  }
}
