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

package org.apache.solr.shell;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.shell.exec.Shell;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Port of {@code test_zk.bats}.
 *
 * <p>The symlink test works on a copy of the config set rather than mutating the shared
 * distribution, and the {@code ZK_HOST} test asserts only the fallback message.
 */
public class ZkTest extends SolrShellTestCase {

  @BeforeClass
  public static void startSolr() {
    // The node lives for the whole class and is stopped automatically.
    classCli.run("solr start -DminStateByteLenForCompression=0");
  }

  @Test
  public void shortHelp() {
    run("solr zk ls -h").assertContains("usage: bin/solr zk");
  }

  @Test
  public void shortHelpIsInferred() {
    // tryRun, not runFail: the two CLIs disagree about the status. commons-cli rewrites a bare zk
    // subcommand into a help request and exits 0; picocli reports the missing path as a usage
    // error and exits 2. Printing the usage is what both do, and what this test is about.
    tryRun("solr zk ls").assertContains("usage: bin/solr zk");
  }

  @Test
  public void longHelp() {
    run("solr zk -h")
        .assertContains("Pass --help or -h after any COMMAND")
        .assertContains("updateacls");
  }

  @Test
  public void listingOutFiles() {
    run("solr zk ls / -z localhost:${ZK_PORT} --recursive").assertContains("aliases.json");
  }

  @Test
  public void connectingViaVariousSolrUrlsAndZkHosts() {
    run("solr zk ls / --solr-url http://localhost:${SOLR_PORT}").assertContains("aliases.json");
    run("solr zk ls / --solr-connection http://localhost:${SOLR_PORT}")
        .assertContains("aliases.json");
    run("solr zk ls / -s http://localhost:${SOLR_PORT}").assertContains("aliases.json");
    run("solr zk ls /").assertContains("aliases.json");
    run("solr zk ls / -z localhost:${ZK_PORT}").assertContains("aliases.json");
    run("solr zk ls / --zk-host localhost:${ZK_PORT}").assertContains("aliases.json");
  }

  @Test
  public void copyingFilesAround() {
    // touch() rather than the shell's, which cmd.exe does not have. No cd() needed: a session
    // starts in its own work directory, so these land there and so does the download below.
    touch("myfile.txt");
    touch("myfile2.txt");
    touch("myfile3.txt");

    run("solr zk cp myfile.txt zk:/myfile.txt -z localhost:${ZK_PORT}")
        .assertContains(
            "Copying from 'myfile.txt' to 'zk:/myfile.txt'. ZooKeeper at localhost:${ZK_PORT}");
    run("solr zk ls / -z localhost:${ZK_PORT}").assertContains("myfile.txt");

    run("solr zk cp myfile2.txt zk:myfile2.txt -z localhost:${ZK_PORT}");
    run("solr zk ls / -z localhost:${ZK_PORT}").assertContains("myfile2.txt");

    run("solr zk cp myfile3.txt zk:/myfile3.txt -z localhost:${ZK_PORT}")
        .assertContains(
            "Copying from 'myfile3.txt' to 'zk:/myfile3.txt'. ZooKeeper at localhost:${ZK_PORT}");
    run("solr zk ls / -z localhost:${ZK_PORT}").assertContains("myfile3.txt");

    run(
        """
        solr zk cp zk:/ -r "recursive_download/"
        """);
    Path download = workDir().resolve("recursive_download");
    assertTrue(Files.exists(download.resolve("myfile.txt")));
    assertTrue(Files.exists(download.resolve("myfile2.txt")));
    assertTrue(Files.exists(download.resolve("myfile3.txt")));
  }

  @Test
  public void upconfig() {
    run("""
        solr zk upconfig -d "${SOLR_TIP}/server/solr/configsets/sample_techproducts_configs" \
            -n techproducts2 -z localhost:${ZK_PORT}
        """)
        .assertContains("Uploading")
        .assertNotContains("ERROR");

    // The harness' own HTTP helper needs no curl, which is what
    // makes this work on Windows.
    awaitOrFail(
        "config set techproducts2 to appear",
        Duration.ofSeconds(10),
        Duration.ofSeconds(1),
        () -> http("/api/configsets").body().contains("techproducts2"));
    assertTrue(
        http("/api/configsets").body().contains("\"configSets\":[\"_default\",\"techproducts2\"]"));
  }

  @Test
  @EnabledOnShell(
      Shell.BASH) // Creating a symlink on Windows needs privileges a test cannot assume.
  public void solr12429UpconfigFailsWithSymlink() throws IOException {
    // A copy, rather than symlinking inside the distribution's own config set and deleting the
    // links
    // afterwards. Here it works on a copy: the distribution is shared by every test class, so
    // mutating it would make this test unsafe to run alongside any other.
    Path configSet = workDir().resolve("configset");
    copyDirectory(
        harnessEnv.solrTip().resolve("server/solr/configsets/sample_techproducts_configs"),
        configSet);

    Path conf = configSet.resolve("conf");
    Files.createSymbolicLink(
        conf.resolve("symlinked_stopwords.txt"), conf.resolve("stopwords.txt"));
    Files.createSymbolicLink(conf.resolve("language"), conf.resolve("lang"));
    assertTrue(Files.isSymbolicLink(conf.resolve("symlinked_stopwords.txt")));
    assertTrue(Files.isSymbolicLink(conf.resolve("language")));

    runFail(
            """
        solr zk upconfig -d "configset" -n techproducts_with_symlinks -z localhost:${ZK_PORT}
        """)
        .assertContains("Uploading")
        .assertContains("ERROR: Not uploading symbolic link");
  }

  @Test
  public void downconfig() {
    run("""
        solr zk downconfig -z localhost:${ZK_PORT} -n _default -d "downconfig"
        """)
        .assertContains("Downloading")
        .assertNotContains("ERROR");
  }

  @Test
  public void zkCpGetsSolrHomeFromTheCommandLineOption() {
    touch("afile.txt");

    run("solr zk cp afile.txt zk:/afile.txt -z localhost:${ZK_PORT} --verbose"
            + " --solr-home \"${SOLR_TIP}/server/solr\"")
        .assertContains("Using SolrHome: ${SOLR_TIP}/server/solr")
        .assertNotContains("Failed to load solr.xml from ZK or SolrHome");
  }

  @Test
  public void envVarZkHostIsHonored() {
    // SOLR_PORT_LISTEN has to be absent, not empty: the tool looks at whether it is set at all
    // before falling back to the default port.
    unset("SOLR_PORT_LISTEN");

    // Only the fallback message is asserted, not that the listing came back empty. The BATS
    // original also refuted "aliases.json", which assumes nothing is listening on 8983 — a port no
    // test class owns, and the single most likely thing to be running on a Solr developer's own
    // machine. That made the test fail for a reason having nothing to do with ZK_HOST.
    tryRun("solr zk ls / --recursive").assertContains("assuming solr url is http://localhost:8983");

    env("ZK_HOST", "localhost:" + ports().zk());
    run("solr zk ls / --recursive").assertContains("aliases.json");
  }

  private static void copyDirectory(Path source, Path target) throws IOException {
    try (var paths = Files.walk(source)) {
      for (Path path : paths.toList()) {
        Path destination = target.resolve(source.relativize(path).toString());
        if (Files.isDirectory(path)) {
          Files.createDirectories(destination);
        } else {
          Files.createDirectories(destination.getParent());
          Files.copy(path, destination);
        }
      }
    } catch (IOException e) {
      IOUtils.rm(target);
      throw e;
    }
  }
}
