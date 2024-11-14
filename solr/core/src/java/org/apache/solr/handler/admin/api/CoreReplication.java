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

import static org.apache.solr.security.PermissionNameProvider.Name.CORE_READ_PERM;

import jakarta.inject.Inject;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.IOException;
import org.apache.solr.client.api.endpoint.ReplicationApis;
import org.apache.solr.client.api.model.FileListResponse;
import org.apache.solr.client.api.model.IndexVersionResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API implementation of {@link ReplicationApis}
 *
 * <p>These APIs are analogous to the v1 /coreName/replication APIs.
 */
public class CoreReplication extends ReplicationAPIBase implements ReplicationApis {

  @Inject
  public CoreReplication(SolrCore solrCore, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(solrCore, req, rsp);
  }

  @Override
  @PermissionName(CORE_READ_PERM)
  public IndexVersionResponse fetchIndexVersion() throws IOException {
    return doFetchIndexVersion();
  }

  @Override
  @PermissionName(CORE_READ_PERM)
  public FileListResponse fetchFileList(long gen) {
    return doFetchFileList(gen);
  }

  @Override
  @PermissionName(CORE_READ_PERM)
  public StreamingOutput fetchFile(
      String filePath,
      String dirType,
      String offset,
      String len,
      Boolean compression,
      Boolean checksum,
      double maxWriteMBPerSec,
      Long gen) {
    if (dirType == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Must provide a dirType ");
    }
    return doFetchFile(
        filePath, dirType, offset, len, compression, checksum, maxWriteMBPerSec, gen);
  }
}
