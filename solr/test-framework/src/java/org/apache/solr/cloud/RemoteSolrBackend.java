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
package org.apache.solr.cloud;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.solr.SolrBackend;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.URLUtil;
import org.apache.solr.core.CoreContainer;

/**
 * {@link SolrBackend} that connects to a pre-existing remote SolrCloud cluster. The caller supplies
 * the HTTP connection string at construction time (e.g. {@code http://localhost:8983/solr}).
 */
public class RemoteSolrBackend implements SolrBackend {

  private final CloudSolrClient adminClient;

  public RemoteSolrBackend(String baseUrl) {
    this.adminClient = new CloudSolrClient.Builder(List.of(baseUrl)).build();
  }

  @Override
  public CoreContainer getCoreContainer() {
    return null;
  }

  @Override
  public SolrClient newClient(String collection) {
    String urlScheme = adminClient.getClusterStateProvider().getUrlScheme();
    var urls =
        adminClient.getClusterStateProvider().getLiveNodes().stream()
            .map(liveNode -> URLUtil.getBaseUrlForNodeName(liveNode, urlScheme))
            .toList();
    return new CloudSolrClient.Builder(urls).withDefaultCollection(collection).build();
  }

  @Override
  public SolrClient getAdminClient() {
    return adminClient;
  }

  @Override
  public void uploadConfigSet(Path configDir, String name) throws SolrServerException, IOException {
    Path tempZip = Files.createTempFile("configset-", ".zip");
    try {
      zipDirectory(configDir, tempZip);
      new ConfigSetAdminRequest.Upload()
          .setConfigSetName(name)
          .setUploadFile(tempZip, "application/zip")
          .process(adminClient);
    } finally {
      Files.deleteIfExists(tempZip);
    }
  }

  @Override
  public boolean hasConfigSet(String name) throws SolrServerException, IOException {
    return new ConfigSetAdminRequest.List().process(adminClient).getConfigSets().contains(name);
  }

  /** Packages {@code sourceDir} contents into a zip file. */
  private static void zipDirectory(Path sourceDir, Path targetZip) throws IOException {
    try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(targetZip))) {
      Files.walk(sourceDir)
          .filter(Files::isRegularFile)
          .forEach(
              file -> {
                try {
                  zos.putNextEntry(new ZipEntry(sourceDir.relativize(file).toString()));
                  Files.copy(file, zos);
                  zos.closeEntry();
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    }
  }

  @Override
  public void createCollection(CollectionAdminRequest.Create create)
      throws SolrServerException, IOException {
    create.process(adminClient);
  }

  @Override
  public boolean hasCollection(String name) throws SolrServerException, IOException {
    return CollectionAdminRequest.listCollections(adminClient).contains(name);
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(adminClient);
  }
}
