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
package org.apache.solr.cli;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.cli.CommandLine;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

// Backups do checksum validation against a footer value not present in 'SimpleText'
@LuceneTestCase.SuppressCodecs({"SimpleText"})
public class SnapshotExportToolTest extends SolrCloudTestCase {

  private static final String COLLECTION = "snapshot_export_coll";

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.security.allow.paths", "*");
    String solrXml =
        MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML.replace(
            "</solr>",
            "<backup><repository name=\"local\" "
                + "class=\"org.apache.solr.core.backup.repository.LocalFileSystemRepository\"/>"
                + "</backup></solr>");
    configureCluster(1)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .withSolrXml(solrXml)
        .configure();

    CollectionAdminRequest.createCollection(COLLECTION, "conf1", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 1, 1);
    cluster.getSolrClient().add(COLLECTION, new SolrInputDocument("id", "1"));
    cluster.getSolrClient().commit(COLLECTION);
  }

  @AfterClass
  public static void tearDownClass() {
    System.clearProperty("solr.security.allow.paths");
  }

  private String solrUrl() {
    return cluster.getJettySolrRunner(0).getBaseUrl().toString();
  }

  /** A fresh destination per test: each one counts what it wrote. */
  private Path newDestDir() {
    return createTempDir("backups");
  }

  private CommandLine parse(String... args) throws IOException {
    SnapshotExportTool tool = new SnapshotExportTool(new CLITestHelper.TestingRuntime(false));
    return SolrCLI.processCommandLineArgs(tool, args);
  }

  /**
   * The option used to select which snapshot to export. That selection needed the non-incremental
   * backup format, so it is now accepted only to be refused with an explanation, rather than
   * silently backing up the live index instead.
   */
  @Test
  public void testSnapshotNameIsRejected() throws Exception {
    Path destDir = newDestDir();
    SnapshotExportTool tool = new SnapshotExportTool(new CLITestHelper.TestingRuntime(false));
    CommandLine cli =
        SolrCLI.processCommandLineArgs(
            tool,
            new String[] {
              "-c",
              COLLECTION,
              "--dest-dir",
              destDir.toString(),
              "--solr-url",
              solrUrl(),
              "--snapshot-name",
              "snap1"
            });

    IllegalArgumentException e =
        expectThrows(IllegalArgumentException.class, () -> tool.runImpl(cli));
    assertTrue(e.getMessage(), e.getMessage().contains("--snapshot-name is no longer supported"));

    // The CLI reports it as an error rather than a stack trace, and writes nothing.
    assertEquals(1, tool.runTool(cli));
    assertEquals(List.of(), backupDirs(destDir));
  }

  /**
   * It was {@code required()} while it still selected something. Parsing at all is the assertion: a
   * missing required option makes {@link SolrCLI} print help and exit.
   */
  @Test
  public void testSnapshotNameNoLongerRequired() throws Exception {
    CommandLine cli = parse("-c", COLLECTION, "--dest-dir", newDestDir().toString());
    assertFalse(
        "--snapshot-name should not be set",
        Arrays.stream(cli.getOptions()).anyMatch(o -> "snapshot-name".equals(o.getLongOpt())));
  }

  @Test
  public void testBackupNameIsDerivedFromCollectionAndTime() {
    assertEquals(
        "coll_20260905T123456Z",
        SnapshotExportTool.backupName("coll", Instant.parse("2026-09-05T12:34:56.789Z")));
  }

  /** The command still backs up the collection, under the name it derives and prints. */
  @Test
  public void testExportWritesBackupUnderTheDerivedName() throws Exception {
    Path destDir = newDestDir();
    CLITestHelper.TestingRuntime runtime = new CLITestHelper.TestingRuntime(true);
    int exitCode =
        CLITestHelper.runTool(
            new String[] {
              "snapshot-export",
              "-c",
              COLLECTION,
              "--dest-dir",
              destDir.toString(),
              "--backup-repo-name",
              "local",
              "--solr-url",
              solrUrl()
            },
            runtime,
            SnapshotExportTool.class);
    assertEquals(0, exitCode);

    List<String> written = backupDirs(destDir);
    assertEquals("one backup directory expected: " + written, 1, written.size());
    String name = written.get(0);
    assertTrue(name, name.matches(COLLECTION + "_\\d{8}T\\d{6}Z"));
    assertTrue(runtime.getOutput(), runtime.getOutput().contains(name));
  }

  private static List<String> backupDirs(Path destDir) throws IOException {
    try (Stream<Path> children = Files.list(destDir)) {
      return children.map(p -> p.getFileName().toString()).sorted().toList();
    }
  }
}
