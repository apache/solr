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
package org.apache.solr.cloud.api.collections;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.UUID;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.request.ConfigsetsApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.junit.Test;

/**
 * Base class for testing configset uploads from a backup repository with various repository types.
 *
 * <p>Subclasses bootstrap a Solr cluster with a single configured backup repository, and tell this
 * class its name and where in it test data can be written. This class stages the files through the
 * cluster's own {@link BackupRepository}, so it works for any repository implementation.
 *
 * @see org.apache.solr.handler.configsets.UploadConfigSet
 */
public abstract class AbstractConfigSetUploadFromRepositoryTest extends SolrCloudTestCase {

  /** The name of the backup repository, as configured in solr.xml, to read from. */
  protected abstract String repositoryName();

  /** A location in the repository under which each test stages its own files. */
  protected abstract String baseLocation();

  /** The 'location' parameter that refers to {@code uri}; some repositories want a plain path. */
  protected String locationParam(URI uri) {
    return uri.toString();
  }

  /** Writes a file, and any parent directories, under the test's own directory. */
  protected URI stageFile(URI testDir, String relativePath, String content) throws Exception {
    try (BackupRepository repository = newRepository()) {
      URI dir = testDir;
      repository.createDirectory(dir);
      final String[] parts = relativePath.split("/");
      for (int i = 0; i < parts.length - 1; i++) {
        dir = repository.resolveDirectory(dir, parts[i]);
        repository.createDirectory(dir);
      }
      final URI file = repository.resolve(dir, parts[parts.length - 1]);
      try (OutputStream os = repository.createOutput(file)) {
        os.write(content.getBytes(UTF_8));
      }
      return file;
    }
  }

  /** A fresh, not yet created, directory in the repository for the current test to stage into. */
  protected URI newTestDir() throws Exception {
    try (BackupRepository repository = newRepository()) {
      return repository.resolveDirectory(
          repository.createDirectoryURI(baseLocation()), UUID.randomUUID().toString());
    }
  }

  private BackupRepository newRepository() {
    return cluster.getJettySolrRunner(0).getCoreContainer().newBackupRepository(repositoryName());
  }

  private ConfigsetsApi.UploadConfigSet uploadRequest(String configSetName, URI source) {
    final var request = new ConfigsetsApi.UploadConfigSet(configSetName, emptyBody());
    request.setRepository(repositoryName());
    request.setLocation(locationParam(source));
    return request;
  }

  private ConfigsetsApi.UploadConfigSetFile uploadFileRequest(
      String configSetName, String filePath, URI source) {
    final var request = new ConfigsetsApi.UploadConfigSetFile(configSetName, filePath, emptyBody());
    request.setRepository(repositoryName());
    request.setLocation(locationParam(source));
    return request;
  }

  private static InputStream emptyBody() {
    return new ByteArrayInputStream(new byte[0]);
  }

  private static String uniqueName(String prefix) {
    return prefix + "-" + UUID.randomUUID();
  }

  private static boolean exists(String configSetName, String filePath) throws Exception {
    return cluster.getZkClient().exists("/configs/" + configSetName + "/" + filePath);
  }

  private static String read(String configSetName, String filePath) throws Exception {
    return new String(
        cluster.getZkClient().getData("/configs/" + configSetName + "/" + filePath, null, null),
        UTF_8);
  }

  @Test
  public void testUploadConfigSetFromRepository() throws Exception {
    final URI dir = newTestDir();
    stageFile(dir, "solrconfig.xml", "<config/>");
    stageFile(dir, "lang/stopwords.txt", "a");
    final String name = uniqueName("fromrepo");

    uploadRequest(name, dir).process(cluster.getSolrClient());

    assertEquals("<config/>", read(name, "solrconfig.xml"));
    assertEquals("a", read(name, "lang/stopwords.txt"));
  }

  @Test
  public void testOverwriteFalseRejectsExistingConfigSet() throws Exception {
    final URI dir = newTestDir();
    stageFile(dir, "solrconfig.xml", "<new/>");
    final String name = uniqueName("existing");
    uploadRequest(name, dir).process(cluster.getSolrClient());
    stageFile(dir, "solrconfig.xml", "<newer/>");

    final var request = uploadRequest(name, dir);
    request.setOverwrite(false);
    final var e =
        expectThrows(RemoteSolrException.class, () -> request.process(cluster.getSolrClient()));

    assertEquals(400, e.code());
    assertEquals("<new/>", read(name, "solrconfig.xml"));
  }

  @Test
  public void testCleanupRemovesFilesNotInRepository() throws Exception {
    final URI firstDir = newTestDir();
    stageFile(firstDir, "solrconfig.xml", "<old/>");
    stageFile(firstDir, "stale.txt", "stale");
    final String name = uniqueName("cleanup");
    uploadRequest(name, firstDir).process(cluster.getSolrClient());
    final URI secondDir = newTestDir();
    stageFile(secondDir, "solrconfig.xml", "<new/>");

    final var request = uploadRequest(name, secondDir);
    request.setCleanup(true);
    request.process(cluster.getSolrClient());

    assertEquals("<new/>", read(name, "solrconfig.xml"));
    assertFalse(exists(name, "stale.txt"));
  }

  @Test
  public void testMissingLocationIsRejected() throws Exception {
    final String name = uniqueName("missing");
    final var request = uploadRequest(name, newTestDir());

    final var e =
        expectThrows(RemoteSolrException.class, () -> request.process(cluster.getSolrClient()));

    assertEquals(400, e.code());
    assertFalse(cluster.getZkClient().exists("/configs/" + name));
  }

  @Test
  public void testUploadSingleFileFromRepository() throws Exception {
    final URI dir = newTestDir();
    final URI file = stageFile(dir, "files/schema.xml", "<schema/>");
    final String name = uniqueName("singlefile");
    final URI baseDir = newTestDir();
    stageFile(baseDir, "solrconfig.xml", "<config/>");
    uploadRequest(name, baseDir).process(cluster.getSolrClient());

    uploadFileRequest(name, "conf/schema.xml", file).process(cluster.getSolrClient());

    assertEquals("<schema/>", read(name, "conf/schema.xml"));
    assertEquals("<config/>", read(name, "solrconfig.xml"));
  }

  @Test
  public void testSingleFileLocationMustBeAFile() throws Exception {
    final URI dir = newTestDir();
    stageFile(dir, "files/schema.xml", "<schema/>");
    final String name = uniqueName("notafile");
    final var request = uploadFileRequest(name, "schema.xml", newRepositoryChild(dir, "files"));

    final var e =
        expectThrows(RemoteSolrException.class, () -> request.process(cluster.getSolrClient()));

    assertEquals(400, e.code());
    assertFalse(cluster.getZkClient().exists("/configs/" + name));
  }

  @Test
  public void testRepositoryAndLocationAreBothRequired() throws Exception {
    final var onlyRepository = new ConfigsetsApi.UploadConfigSet(uniqueName("c"), emptyBody());
    onlyRepository.setRepository(repositoryName());
    final var onlyLocation = new ConfigsetsApi.UploadConfigSet(uniqueName("c"), emptyBody());
    onlyLocation.setLocation(locationParam(newTestDir()));

    for (var request : new ConfigsetsApi.UploadConfigSet[] {onlyRepository, onlyLocation}) {
      final var e =
          expectThrows(RemoteSolrException.class, () -> request.process(cluster.getSolrClient()));
      assertEquals(400, e.code());
    }
  }

  @Test
  public void testBodyAndRepositoryAreMutuallyExclusive() throws Exception {
    final URI dir = newTestDir();
    stageFile(dir, "solrconfig.xml", "<config/>");
    final String name = uniqueName("both");
    final var request =
        new ConfigsetsApi.UploadConfigSet(
            name, new ByteArrayInputStream("not empty".getBytes(UTF_8)));
    request.setRepository(repositoryName());
    request.setLocation(locationParam(dir));

    final var e =
        expectThrows(RemoteSolrException.class, () -> request.process(cluster.getSolrClient()));

    assertEquals(400, e.code());
    assertFalse(cluster.getZkClient().exists("/configs/" + name));
  }

  private URI newRepositoryChild(URI dir, String child) throws Exception {
    try (BackupRepository repository = newRepository()) {
      return repository.resolveDirectory(dir, child);
    }
  }
}
