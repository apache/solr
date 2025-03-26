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
package org.apache.solr.filestore;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.solr.security.PermissionNameProvider.Name.FILESTORE_READ_PERM;

import jakarta.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.NodeFileStoreApis;
import org.apache.solr.client.api.model.FileStoreJsonFileResponse;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation for {@link NodeFileStoreApis} */
public class NodeFileStore extends JerseyResource implements NodeFileStoreApis {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CoreContainer coreContainer;
  private final SolrQueryRequest req;
  private final SolrQueryResponse rsp;
  private final FileStore fileStore;

  @Inject
  public NodeFileStore(
      CoreContainer coreContainer,
      FileStore fileStore,
      SolrQueryRequest req,
      SolrQueryResponse rsp) {
    this.coreContainer = coreContainer;
    this.req = req;
    this.rsp = rsp;
    this.fileStore = fileStore;
  }

  // Has been replaced by a number of distinct ClusterFileStore endpoints
  /**
   * @deprecated use {@link ClusterFileStore} methods instead
   */
  @Override
  @PermissionName(FILESTORE_READ_PERM)
  @Deprecated
  public SolrJerseyResponse getFile(String path, Boolean sync, String getFrom, Boolean meta) {
    final var clusterFileApi = new ClusterFileStore(coreContainer, fileStore, req, rsp);

    if (Boolean.TRUE.equals(sync)) {
      return clusterFileApi.syncFile(path);
    }

    if (path == null) {
      path = "";
    }
    final var pathCopy = path;
    if (getFrom != null) {
      return clusterFileApi.fetchFile(path, getFrom);
    }

    FileStore.FileType type = fileStore.getType(path, false);
    if (type == FileStore.FileType.NOFILE
        || type == FileStore.FileType.DIRECTORY
        || (Boolean.TRUE.equals(meta) && type == FileStore.FileType.FILE)) {
      return clusterFileApi.getMetadata(path);
    }

    // Support for JSON-ified response removed in 10.0, but here for the remainder of 9.x
    if ("json".equals(req.getParams().get(CommonParams.WT))) {
      final var jsonResponse = instantiateJerseyResponse(FileStoreJsonFileResponse.class);
      try {
        fileStore.get(
            pathCopy,
            it -> {
              try {
                InputStream inputStream = it.getInputStream();
                if (inputStream != null) {
                  jsonResponse.response = new String(inputStream.readAllBytes(), UTF_8);
                }
              } catch (IOException e) {
                throw new SolrException(
                    SolrException.ErrorCode.SERVER_ERROR, "Error reading file " + pathCopy);
              }
            },
            false);
        return jsonResponse;
      } catch (IOException e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "Error getting file from path " + path);
      }
    } else {
      return clusterFileApi.getFile(path);
    }
  }
}
