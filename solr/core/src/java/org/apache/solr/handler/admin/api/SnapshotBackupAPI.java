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

import static org.apache.solr.handler.ReplicationHandler.ERR_STATUS;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

import io.swagger.v3.oas.annotations.parameters.RequestBody;
import jakarta.inject.Inject;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.function.Consumer;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.ReplicationBackupApis;
import org.apache.solr.client.api.model.ReplicationBackupRequestBody;
import org.apache.solr.client.api.model.ReplicationBackupResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.handler.ReplicationHandler.ReplicationHandlerConfig;
import org.apache.solr.jersey.PermissionName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * v2 API implementation for replication-handler based backup creation.
 *
 * <p>This is the main backup functionality available to 'standalone' users.
 */
public class SnapshotBackupAPI extends JerseyResource implements ReplicationBackupApis {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final SolrCore solrCore;
  private final ReplicationHandlerConfig replicationHandlerConfig;

  @Inject
  public SnapshotBackupAPI(SolrCore solrCore, ReplicationHandlerConfig replicationHandlerConfig) {
    this.solrCore = solrCore;
    this.replicationHandlerConfig = replicationHandlerConfig;
  }

  /**
   * This API (POST /api/cores/coreName/replication/backups {...}) is analogous to the v1
   * /solr/coreName/replication?command=backup
   */
  @Override
  @PermissionName(CORE_EDIT_PERM)
  public ReplicationBackupResponse createBackup(
      @RequestBody ReplicationBackupRequestBody backupReplicationPayload) {
    ensureRequiredRequestBodyProvided(backupReplicationPayload);
    return doBackup(backupReplicationPayload);
  }

  private ReplicationBackupResponse doBackup(
      ReplicationBackupRequestBody backupReplicationPayload) {
    ReplicationBackupResponse response = instantiateJerseyResponse(ReplicationBackupResponse.class);
    int numberToKeep = backupReplicationPayload.numberToKeep;
    int numberBackupsToKeep = replicationHandlerConfig.getNumberBackupsToKeep();
    String location = backupReplicationPayload.location;
    String repoName = backupReplicationPayload.repository;
    String commitName = backupReplicationPayload.commitName;
    String name = backupReplicationPayload.name;
    Consumer<NamedList<?>> resultConsumer = result -> response.result = result;
    try {
      doSnapShoot(
          numberToKeep,
          numberBackupsToKeep,
          location,
          repoName,
          commitName,
          name,
          solrCore,
          resultConsumer);
      response.status = ReplicationHandler.OK_STATUS;
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      log.error("Exception while creating a snapshot", e);
      reportErrorOnResponse(
          response, "Error encountered while creating a snapshot: " + e.getMessage(), e);
    }
    return response;
  }

  /** Separate method helps with testing */
  protected void doSnapShoot(
      int numberToKeep,
      int numberBackupsToKeep,
      String location,
      String repoName,
      String commitName,
      String name,
      SolrCore solrCore,
      Consumer<NamedList<?>> resultConsumer)
      throws IOException {
    ReplicationHandler.doSnapShoot(
        numberToKeep,
        numberBackupsToKeep,
        location,
        repoName,
        commitName,
        name,
        solrCore,
        resultConsumer);
  }

  private static void reportErrorOnResponse(
      ReplicationBackupResponse response, String message, Exception e) {
    response.status = ERR_STATUS;
    response.message = message;
    if (e != null) {
      response.exception = e;
    }
  }
}
