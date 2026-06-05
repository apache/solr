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

import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_READ_PERM;

import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.client.api.endpoint.ConfigsetsApi;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/** V2 API implementation for downloading a configset as a zip file. */
public class DownloadConfigSet extends ConfigSetAPIBase implements ConfigsetsApi.Download {

  @Inject
  public DownloadConfigSet(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public Response downloadConfigSet(String configSetName) throws Exception {
    if (StrUtils.isNullOrEmpty(configSetName)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "No configset name provided to download");
    }
    if (!configSetService.checkConfigExists(configSetName)) {
      throw new SolrException(
          SolrException.ErrorCode.NOT_FOUND, "ConfigSet " + configSetName + " not found!");
    }
    return buildZipResponse(configSetService, configSetName);
  }

  /**
   * Build a ZIP download {@link Response} for the given configset.
   *
   * @param configSetService the service to use for downloading the configset files
   * @param configSetName the name of the configset to download
   */
  public static Response buildZipResponse(ConfigSetService configSetService, String configSetName)
      throws IOException {
    final byte[] zipBytes = zipConfigSet(configSetService, configSetName);
    return Response.ok((StreamingOutput) outputStream -> outputStream.write(zipBytes))
        .type("application/zip")
        .build();
  }

  /**
   * Download the named configset from {@link ConfigSetService} and return its contents as a ZIP
   * archive byte array.
   */
  public static byte[] zipConfigSet(ConfigSetService configSetService, String configSetName)
      throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Path tmpDirectory = Files.createTempDirectory("configset-download-");
    try {
      configSetService.downloadConfig(configSetName, tmpDirectory);
      try (ZipOutputStream zipOut = new ZipOutputStream(baos)) {
        Files.walkFileTree(
            tmpDirectory,
            new SimpleFileVisitor<>() {
              @Override
              public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                  throws IOException {
                if (Files.isHidden(dir)) {
                  return FileVisitResult.SKIP_SUBTREE;
                }
                String dirName = tmpDirectory.relativize(dir).toString();
                if (!dirName.isEmpty()) {
                  if (!dirName.endsWith("/")) {
                    dirName += "/";
                  }
                  zipOut.putNextEntry(new ZipEntry(dirName));
                  zipOut.closeEntry();
                }
                return FileVisitResult.CONTINUE;
              }

              @Override
              public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                  throws IOException {
                if (!Files.isHidden(file)) {
                  try (InputStream fis = Files.newInputStream(file)) {
                    ZipEntry zipEntry = new ZipEntry(tmpDirectory.relativize(file).toString());
                    zipOut.putNextEntry(zipEntry);
                    fis.transferTo(zipOut);
                  }
                }
                return FileVisitResult.CONTINUE;
              }
            });
      }
    } finally {
      PathUtils.deleteDirectory(tmpDirectory);
    }
    return baos.toByteArray();
  }
}
