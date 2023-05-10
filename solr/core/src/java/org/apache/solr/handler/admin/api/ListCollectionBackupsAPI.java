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

import static org.apache.solr.client.solrj.impl.BinaryResponseParser.BINARY_CONTENT_TYPE_V2;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_ID;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_LOCATION;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_REPOSITORY;
import static org.apache.solr.common.params.CoreAdminParams.NAME;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.backup.BackupFilePaths;
import org.apache.solr.core.backup.BackupId;
import org.apache.solr.core.backup.BackupProperties;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJacksonMapper;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API definitions for collection-backup "listing".
 *
 * <p>These APIs are equivalent to the v1 '/admin/collections?action=LISTBACKUP' command.
 */
public class ListCollectionBackupsAPI extends BackupAPIBase {

  private final ObjectMapper objectMapper;

  @Inject
  public ListCollectionBackupsAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);

    this.objectMapper = SolrJacksonMapper.getObjectMapper();
  }

  public static void invokeFromV1Params(
      CoreContainer coreContainer, SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final SolrParams v1Params = req.getParams();
    v1Params.required().check(CommonParams.NAME);

    final var listApi = new ListCollectionBackupsAPI(coreContainer, req, rsp);
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(
        rsp,
        listApi.listBackupsAtLocation(
            v1Params.get(NAME), v1Params.get(BACKUP_LOCATION), v1Params.get(BACKUP_REPOSITORY)));
  }

  @Path("/backups/{backupName}/versions")
  @GET
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(COLL_EDIT_PERM)
  public ListCollectionBackupsResponse listBackupsAtLocation(
      @PathParam("backupName") String backupName,
      @QueryParam(BACKUP_LOCATION) String location,
      @QueryParam(BACKUP_REPOSITORY) String repositoryName)
      throws IOException {
    final var response = instantiateJerseyResponse(ListCollectionBackupsResponse.class);
    recordCollectionForLogAndTracing(null, solrQueryRequest);

    ensureRequiredParameterProvided(NAME, backupName);
    location = getAndValidateIncrementalBackupLocation(repositoryName, location, backupName);

    try (final var repository = createBackupRepository(repositoryName)) {
      final URI locationURI = repository.createDirectoryURI(location);
      final var backupLocation =
          BackupFilePaths.buildExistingBackupLocationURI(repository, locationURI, backupName);

      String[] subFiles = repository.listAllOrEmpty(backupLocation);
      List<BackupId> propsFiles = BackupFilePaths.findAllBackupIdsFromFileListing(subFiles);

      response.backups = new ArrayList<>();
      for (BackupId backupId : propsFiles) {
        BackupProperties properties =
            BackupProperties.readFrom(
                repository, backupLocation, BackupFilePaths.getBackupPropsName(backupId));
        if (response.collection == null) {
          response.collection = properties.getCollection();
        }

        // TODO Make BackupProperties itself Jackson-aware to avoid the additional conversion here?
        Map<String, Object> details = properties.getDetails();
        details.put(BACKUP_ID, backupId.id);
        response.backups.add(objectMapper.convertValue(details, CollectionBackupDetails.class));
      }
    }
    return response;
  }

  public static class ListCollectionBackupsResponse extends SolrJerseyResponse {
    @JsonProperty public String collection;
    @JsonProperty public List<CollectionBackupDetails> backups;
  }

  // TODO Merge with CreateCollectionBackupAPI.CollectionBackupDetails, which seems very
  // conceptually similar...
  public static class CollectionBackupDetails implements JacksonReflectMapWriter {
    @JsonProperty public Integer backupId;
    @JsonProperty public String indexVersion;
    @JsonProperty public String startTime;
    @JsonProperty public String endTime;
    @JsonProperty public Integer indexFileCount;
    @JsonProperty public Double indexSizeMB;

    @JsonProperty public Map<String, String> shardBackupIds;

    @JsonProperty(COLL_CONF)
    public String configsetName;

    @JsonProperty public String collectionAlias;
  }
}
