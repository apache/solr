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
package org.apache.solr.handler.configsets;

import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;

import jakarta.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.client.api.endpoint.ConfigsetsApi;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.solrj.util.SolrIdentifierValidator;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.FileTypeMagicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * V2 API implementation for uploading a configset as a zip file.
 *
 * <p>This API (GET /v2/configsets) is analogous to the v1 /admin/configs?action=UPLOAD command.
 */
public class UploadConfigSet extends ConfigSetAPIBase
    implements ConfigsetsApi.Upload, ConfigsetsApi.PutFile {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Inject
  public UploadConfigSet(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(CONFIG_EDIT_PERM)
  public SolrJerseyResponse uploadConfigSet(
      String configSetName,
      Boolean overwrite,
      Boolean cleanup,
      String repository,
      String location,
      InputStream requestBody)
      throws IOException {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureConfigSetUploadEnabled();
    final boolean fromRepository = isFromRepository(repository, location);
    ensureSingleSource(fromRepository, requestBody);
    if (!fromRepository && requestBody == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "A configset upload must provide either a ZIP request body or a repository and location.");
    }
    SolrIdentifierValidator.validateConfigSetName(configSetName);

    final boolean overwritesExisting = configSetService.checkConfigExists(configSetName);
    if (overwritesExisting && overwrite != null && !overwrite) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "The configuration " + configSetName + " already exists");
    }
    final List<String> filesToDelete =
        overwritesExisting && cleanup != null && cleanup
            ? configSetService.getAllConfigFiles(configSetName)
            : new ArrayList<>();

    if (fromRepository) {
      uploadFromRepository(configSetName, repository, location, filesToDelete);
    } else {
      uploadZip(configSetName, requestBody, filesToDelete);
    }
    deleteUnusedFiles(configSetService, configSetName, filesToDelete);

    return response;
  }

  private void uploadZip(String configSetName, InputStream requestBody, List<String> filesToDelete)
      throws IOException {
    // Write the request body to a temp file so we can use ZipFile, which reads the central
    // directory and correctly handles entries that use the STORED method with an EXT (data
    // descriptor) flag — a combination that ZipInputStream cannot process.  This allows
    // zero-byte files (e.g. created with `touch`) to be included in the uploaded configset.
    final Path tempZip = Files.createTempFile("solr-configset-upload-", ".zip");
    try {
      Files.copy(requestBody, tempZip, StandardCopyOption.REPLACE_EXISTING);
      try (ZipFile zipFile = new ZipFile(tempZip.toFile())) {
        boolean hasEntry = false;
        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        while (entries.hasMoreElements()) {
          ZipEntry zipEntry = entries.nextElement();
          hasEntry = true;
          String filePath = zipEntry.getName();
          filesToDelete.remove(filePath);
          if (!zipEntry.isDirectory()) {
            try (InputStream entryStream = zipFile.getInputStream(zipEntry)) {
              configSetService.uploadFileToConfig(
                  configSetName, filePath, entryStream.readAllBytes(), true);
            }
          }
        }
        if (!hasEntry) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Either empty zipped data, or non-zipped data was uploaded. In order to upload a configSet, you must zip a non-empty directory to upload.");
        }
      } catch (ZipException e) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Failed to read the uploaded zip file: " + e.getMessage(),
            e);
      }
    } finally {
      Files.deleteIfExists(tempZip);
    }
  }

  private void uploadFromRepository(
      String configSetName,
      String repositoryName,
      String requestedLocation,
      List<String> filesToDelete)
      throws IOException {
    try (BackupRepository repository = coreContainer.newBackupRepository(repositoryName)) {
      final String location = resolveRepositoryLocation(repository, requestedLocation);
      final URI source = repository.createDirectoryURI(location);
      if (!repository.exists(source)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "The location " + source + " does not exist in the backup repository");
      }

      // Read everything before writing so a failure doesn't leave a half-uploaded configset.
      final Map<String, byte[]> files = new LinkedHashMap<>();
      readConfigFilesFromRepository(repository, source, "", files);
      if (files.isEmpty()) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "No configset files were found at " + source + " in the backup repository");
      }

      for (var file : files.entrySet()) {
        filesToDelete.remove(file.getKey());
        configSetService.uploadFileToConfig(configSetName, file.getKey(), file.getValue(), true);
      }
    }
  }

  /** Whether the upload reads from a backup repository; 'repository' and 'location' go together. */
  private static boolean isFromRepository(String repository, String location) {
    if ((repository == null) != (location == null)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "The 'repository' and 'location' parameters must be provided together.");
    }
    return repository != null;
  }

  private static void ensureSingleSource(boolean fromRepository, InputStream requestBody)
      throws IOException {
    if (fromRepository && requestBody != null && requestBody.available() > 0) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "An upload must provide either a request body or a repository location, not both.");
    }
  }

  private static void readConfigFilesFromRepository(
      BackupRepository repository, URI dir, String prefix, Map<String, byte[]> files)
      throws IOException {
    for (String name : repository.listAll(dir)) {
      final String filePath = prefix + name;
      switch (repository.getPathType(repository.resolve(dir, name))) {
        case DIRECTORY:
          if (!name.startsWith(".")) {
            readConfigFilesFromRepository(
                repository, repository.resolveDirectory(dir, name), filePath + "/", files);
          }
          break;
        case FILE:
          if (ConfigSetService.isFileForbiddenInConfigSets(filePath)) {
            log.warn("Not uploading file from repository, as it is a forbidden type: {}", filePath);
            break;
          }
          final byte[] data = readRepositoryFile(repository, dir, name);
          if (FileTypeMagicUtil.isFileForbiddenInConfigset(data)) {
            log.warn(
                "Not uploading file {} from repository, as it matched the MAGIC signature of a forbidden mime type",
                filePath);
          } else {
            files.put(filePath, data);
          }
          break;
      }
    }
  }

  private String resolveRepositoryLocation(BackupRepository repository, String requestedLocation) {
    final String location = repository.getBackupLocation(requestedLocation);
    if (location == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "'location' is not specified as a parameter or as a default repository property");
    }
    if ("file".equals(repository.createURI("x").getScheme())) {
      coreContainer.assertPathAllowed(Path.of(location));
    }
    return location;
  }

  private static byte[] readRepositoryFile(BackupRepository repository, URI dir, String name)
      throws IOException {
    try (IndexInput in = repository.openInput(dir, name, IOContext.DEFAULT)) {
      final byte[] data = new byte[(int) in.length()];
      in.readBytes(data, 0, data.length);
      return data;
    }
  }

  @Override
  @PermissionName(CONFIG_EDIT_PERM)
  public SolrJerseyResponse uploadConfigSetFile(
      String configSetName,
      String filePath,
      String repository,
      String location,
      InputStream requestBody)
      throws IOException {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureConfigSetUploadEnabled();
    final boolean fromRepository = isFromRepository(repository, location);
    ensureSingleSource(fromRepository, requestBody);
    SolrIdentifierValidator.validateConfigSetName(configSetName);
    ensureValidFilePath(filePath);
    storeFile(
        configSetName,
        filePath,
        fromRepository
            ? readSingleFileFromRepository(repository, location)
            : requestBody.readAllBytes());
    return response;
  }

  private static String ensureValidFilePath(String filePath) {
    final String singleFilePath = filePath != null ? filePath : "";
    final String fixedSingleFilePath =
        !singleFilePath.isEmpty() && singleFilePath.charAt(0) == '/'
            ? singleFilePath.substring(1)
            : singleFilePath;
    if (fixedSingleFilePath.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "The file path provided for upload, '" + singleFilePath + "', is not valid.");
    }
    return fixedSingleFilePath;
  }

  private void storeFile(String configSetName, String filePath, byte[] data) throws IOException {
    final String fixedSingleFilePath = ensureValidFilePath(filePath);
    if (ConfigSetService.isFileForbiddenInConfigSets(fixedSingleFilePath)
        || FileTypeMagicUtil.isFileForbiddenInConfigset(data)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "The file type provided for upload, '"
              + filePath
              + "', is forbidden for use in configSets.");
    }
    configSetService.uploadFileToConfig(configSetName, fixedSingleFilePath, data, true);
  }

  private byte[] readSingleFileFromRepository(String repositoryName, String requestedLocation)
      throws IOException {
    try (BackupRepository repository = coreContainer.newBackupRepository(repositoryName)) {
      final String location = resolveRepositoryLocation(repository, requestedLocation);
      final String trimmed =
          location.endsWith("/") ? location.substring(0, location.length() - 1) : location;
      final int slash = trimmed.lastIndexOf('/');
      if (slash < 0 || slash == trimmed.length() - 1) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "The location '" + location + "' must be the path of a file in the backup repository");
      }
      final URI dir = repository.createDirectoryURI(slash == 0 ? "/" : trimmed.substring(0, slash));
      final String name = trimmed.substring(slash + 1);
      final URI file = repository.resolve(dir, name);
      if (!repository.exists(file)
          || repository.getPathType(file) != BackupRepository.PathType.FILE) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "The location " + file + " is not a file in the backup repository");
      }
      return readRepositoryFile(repository, dir, name);
    }
  }

  private void deleteUnusedFiles(
      ConfigSetService configSetService, String configName, List<String> filesToDelete)
      throws IOException {
    if (!filesToDelete.isEmpty()) {
      if (log.isInfoEnabled()) {
        log.info("Cleaning up {} unused files", filesToDelete.size());
      }
      if (log.isDebugEnabled()) {
        log.debug("Cleaning up unused files: {}", filesToDelete);
      }
      configSetService.deleteFilesFromConfig(configName, filesToDelete);
    }
  }
}
