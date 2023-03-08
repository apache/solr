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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkShardTerms;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.handler.RestoreCore;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.solr.client.solrj.impl.BinaryResponseParser.BINARY_CONTENT_TYPE_V2;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

@Path("/cores/{coreName}/install")
public class InstallCoreDataAPI extends CoreAdminAPIBase {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public InstallCoreDataAPI(CoreContainer coreContainer, CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker, SolrQueryRequest req, SolrQueryResponse rsp) {
        super(coreContainer, coreAdminAsyncTracker, req, rsp);
    }

    @POST
    @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
    @PermissionName(CORE_EDIT_PERM)
    public SolrJerseyResponse installCoreData(@PathParam("coreName") String coreName, InstallCoreDataRequestBody requestBody) throws Exception {
        log.info("JEGERLOW: In install-core-data v2 API with coreName {}", coreName);
        final SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);

        // TODO Actual implementation (look at RESTORECORE for example)
        final ZkController zkController = coreContainer.getZkController();
        if (zkController == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Only valid for SolrCloud");
        }

        try (BackupRepository repository = coreContainer.newBackupRepository(requestBody.repository); SolrCore core = coreContainer.getCore(coreName)) {
            String location = repository.getBackupLocation(requestBody.location);
            if (location == null) {
                throw new SolrException(
                        SolrException.ErrorCode.BAD_REQUEST,
                        "'location' is not specified as a query"
                                + " parameter or as a default repository property");
            }

            URI locationUri = repository.createDirectoryURI(location);
            CloudDescriptor cd = core.getCoreDescriptor().getCloudDescriptor();
            // this core must be the only replica in its shard otherwise
            // we cannot guarantee consistency between replicas because when we add data (or restore
            // index) to this replica
            Slice slice =
                    zkController
                            .getClusterState()
                            .getCollection(cd.getCollectionName())
                            .getSlice(cd.getShardId());
            if (slice.getReplicas().size() != 1 && !core.readOnly) {
                throw new SolrException(
                        SolrException.ErrorCode.SERVER_ERROR,
                        "Failed to restore core="
                                + core.getName()
                                + ", the core must be the only replica in its shard or it must be read only");
            }

            // TODO RestoreCore.create expects a backup 'name' that it appends to the Uri via 'BackupRepository#resolve'...how does this handle null?
            final RestoreCore restoreCore = RestoreCore.create(repository, core, locationUri, "");
            boolean success = restoreCore.doRestore();
            if (!success) {
                throw new SolrException(
                        SolrException.ErrorCode.SERVER_ERROR, "Failed to install data to core=" + core.getName());
            }

            final Set<String> nonLeaderCoreNames = slice.getReplicas()
                    .stream()
                    .filter(r -> !r.isLeader())
                    .map(r -> r.getName())
                    .collect(Collectors.toSet());
            log.info("JEGERLOW Non leader core names are: {} and leader is {}", nonLeaderCoreNames, coreName);

            final ZkShardTerms zkShardTerms = zkController
                    .getShardTerms(cd.getCollectionName(), cd.getShardId());
            log.info("JEGERLOW ZkShardTerm keys are: {}", zkShardTerms);
            zkShardTerms.ensureTermsIsHigher(slice.getLeader().getName(), nonLeaderCoreNames);
        }

        return response;
    }

    public static class InstallCoreDataRequestBody implements JacksonReflectMapWriter {
        @JsonProperty("location")
        public String location;

        @JsonProperty("repository")
        public String repository;

        @JsonProperty("async")
        public String asyncId;
    }
}
