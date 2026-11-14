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

import java.net.URI;
import java.nio.file.Path;
import org.apache.solr.cloud.api.collections.AbstractConfigSetUploadFromRepositoryTest;
import org.apache.solr.cloud.api.collections.AbstractInstallShardTest;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.StringRequestContent;
import org.eclipse.jetty.http.HttpMethod;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests uploading configsets from a backup repository with {@link
 * org.apache.solr.core.backup.repository.LocalFileSystemRepository}.
 */
public class LocalFSConfigSetUploadFromRepositoryTest
    extends AbstractConfigSetUploadFromRepositoryTest {

  private static final String REPOSITORY_NAME = "local";

  private static final String SOLR_XML =
      AbstractInstallShardTest.defaultSolrXmlTextWithBackupRepository(
          "<str name=\"allowPaths\">${solr.security.allow.paths:}</str>\n"
              + "<backup>\n"
              + "  <repository name=\""
              + REPOSITORY_NAME
              + "\" class=\"org.apache.solr.core.backup.repository.LocalFileSystemRepository\"/>\n"
              + "</backup>\n");

  private static Path repositoryDir;

  @BeforeClass
  public static void setupClass() throws Exception {
    System.setProperty("solr.security.allow.paths", "*");
    repositoryDir = createTempDir("repository");
    configureCluster(1).withSolrXml(SOLR_XML).configure();
  }

  @Override
  protected String repositoryName() {
    return REPOSITORY_NAME;
  }

  @Override
  protected String baseLocation() {
    return repositoryDir.toString();
  }

  @Override
  protected String locationParam(URI uri) {
    return Path.of(uri).toString();
  }

  @Test
  public void testRawBodiesOfAnyContentTypeAreStillStored() throws Exception {
    // A JSON config file is stored as-is
    final var jsonFile = put("rawjson/files/params.json", "application/json", "{\"a\":1}");
    assertEquals(jsonFile.getContentAsString(), 200, jsonFile.getStatus());
    assertTrue(cluster.getZkClient().exists("/configs/rawjson/params.json"));

    // Clients such as curl --data-binary send this content type by default
    final var form =
        put("rawform/files/solrconfig.xml", "application/x-www-form-urlencoded", "<config/>");
    assertEquals(form.getContentAsString(), 200, form.getStatus());
    assertTrue(cluster.getZkClient().exists("/configs/rawform/solrconfig.xml"));
  }

  private ContentResponse put(String path, String contentType, String body) throws Exception {
    final var jetty = cluster.getJettySolrRunners().getFirst();
    return jetty
        .getSolrClient()
        .getHttpClient()
        .newRequest(
            jetty.getBaseUrl().toString().replaceAll("/solr$", "") + "/api/configsets/" + path)
        .method(HttpMethod.PUT)
        .body(new StringRequestContent(contentType, body))
        .send();
  }
}
