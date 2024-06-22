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

import java.io.IOException;
import java.net.URI;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.backup.BackupFilePaths;
import org.apache.solr.core.backup.BackupManager;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/** Base class that facilitates reuse of common validation logic for collection-backup APIs. */
public abstract class BackupAPIBase extends AdminAPIBase {

  public BackupAPIBase(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  protected String getAndValidateBackupLocation(String repositoryName, String location)
      throws IOException {
    try (final var repository = createBackupRepository(repositoryName)) {
      return getAndValidateBackupLocation(repository, location);
    }
  }

  protected String getAndValidateIncrementalBackupLocation(
      String repositoryName, String location, String backupName) throws IOException {
    try (final var repository = createBackupRepository(repositoryName)) {
      final String fetchedLocation = getAndValidateBackupLocation(repository, location);
      ensureBackupLocationIsIncremental(repository, fetchedLocation, backupName);
      return fetchedLocation;
    }
  }

  protected BackupRepository createBackupRepository(String repositoryName) {
    return coreContainer.newBackupRepository(repositoryName);
  }

  private String getLocation(BackupRepository repository, String location) throws IOException {
    location = repository.getBackupLocation(location);
    if (location != null) {
      return location;
    }

    // Refresh the cluster property file to make sure the value set for location is the
    // latest. Check if the location is specified in the cluster property.
    location =
        new ClusterProperties(coreContainer.getZkController().getZkClient())
            .getClusterProperty(CoreAdminParams.BACKUP_LOCATION, null);
    if (location != null) {
      return location;
    }

    throw new SolrException(
        SolrException.ErrorCode.BAD_REQUEST,
        "'location' is not specified as a query"
            + " parameter or as a default repository property or as a cluster property.");
  }

  private void ensureBackupLocationExists(BackupRepository repository, String location) {
    final URI uri = repository.createDirectoryURI(location);
    try {
      if (!repository.exists(uri)) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "specified location " + uri + " does not exist.");
      }
    } catch (IOException ex) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Failed to check the existence of " + uri + ". Is it valid?",
          ex);
    }
  }

  private String getAndValidateBackupLocation(BackupRepository backupRepository, String location)
      throws IOException {
    location = getLocation(backupRepository, location);
    ensureBackupLocationExists(backupRepository, location);
    return location;
  }

  private void ensureBackupLocationIsIncremental(
      BackupRepository repository, String location, String backupName) throws IOException {
    final URI locationURI = repository.createDirectoryURI(location);
    final var locationAndNameUri =
        BackupFilePaths.buildExistingBackupLocationURI(repository, locationURI, backupName);
    if (repository.exists(
        repository.resolve(locationAndNameUri, BackupManager.TRADITIONAL_BACKUP_PROPS_FILE))) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "The backup name ["
              + backupName
              + "] at "
              + "location ["
              + location
              + "] holds a non-incremental (legacy) backup, but "
              + "backup-listing is only supported on incremental backups");
    }
  }
}
