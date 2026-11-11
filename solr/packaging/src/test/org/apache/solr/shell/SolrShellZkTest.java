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
 * <p>Kept deliberately close to the original so the two can be compared line by line, including the
 * order of the tests and the text each one asserts on. Three things differ, each for a reason the
 * comment at the site explains: files are created through Java rather than {@code touch}, {@code
 * curl} becomes {@link #http}, and the symlink test works on a copy of the config set instead of
 * mutating the shared distribution.
 */
public class SolrShellZkTest extends SolrShellTestCase {

  @BeforeClass
  public static void startSolr() {
    // BATS' setup_file. The node lives for the whole class and is stopped automatically.
    classCli.run("solr start -DminStateByteLenForCompression=0").assertSuccess();
  }

  @Test
  public void shortHelp() {
    run("solr zk ls -h").assertOutputContains("usage: bin/solr zk");
  }

  @Test
  public void shortHelpIsInferred() {
    run("solr zk ls").assertOutputContains("usage: bin/solr zk");
  }

  @Test
  public void longHelp() {
    run("solr zk -h")
        .assertOutputContains("Pass --help or -h after any COMMAND")
        .assertOutputContains("updateacls");
  }

  @Test
  public void listingOutFiles() {
    run("solr zk ls / -z localhost:${ZK_PORT} --recursive").assertOutputContains("aliases.json");
  }

  @Test
  public void connectingViaVariousSolrUrlsAndZkHosts() {
    run("solr zk ls / --solr-url http://localhost:${SOLR_PORT}")
        .assertOutputContains("aliases.json");
    run("solr zk ls / --solr-connection http://localhost:${SOLR_PORT}")
        .assertOutputContains("aliases.json");
    run("solr zk ls / -s http://localhost:${SOLR_PORT}").assertOutputContains("aliases.json");
    run("solr zk ls /").assertOutputContains("aliases.json");
    run("solr zk ls / -z localhost:${ZK_PORT}").assertOutputContains("aliases.json");
    run("solr zk ls / --zk-host localhost:${ZK_PORT}").assertOutputContains("aliases.json");
  }

  @Test
  public void copyingFilesAround() {
    // touch() rather than the shell's, which cmd.exe does not have, and in the session's own work
    // directory so concurrent classes do not trip over each other.
    cd(workDir());
    touch("myfile.txt");
    touch("myfile2.txt");
    touch("myfile3.txt");

    run("solr zk cp myfile.txt zk:/myfile.txt -z localhost:${ZK_PORT}")
        .assertOutputContains(
            "Copying from 'myfile.txt' to 'zk:/myfile.txt'. ZooKeeper at localhost:${ZK_PORT}");
    run("solr zk ls / -z localhost:${ZK_PORT}").assertOutputContains("myfile.txt");

    run("solr zk cp myfile2.txt zk:myfile2.txt -z localhost:${ZK_PORT}");
    run("solr zk ls / -z localhost:${ZK_PORT}").assertOutputContains("myfile2.txt");

    run("solr zk cp myfile3.txt zk:/myfile3.txt -z localhost:${ZK_PORT}")
        .assertOutputContains(
            "Copying from 'myfile3.txt' to 'zk:/myfile3.txt'. ZooKeeper at localhost:${ZK_PORT}");
    run("solr zk ls / -z localhost:${ZK_PORT}").assertOutputContains("myfile3.txt");

    Path download = workDir().resolve("recursive_download");
    run("solr zk cp zk:/ -r \"%s/\"", download);
    assertTrue(Files.exists(download.resolve("myfile.txt")));
    assertTrue(Files.exists(download.resolve("myfile2.txt")));
    assertTrue(Files.exists(download.resolve("myfile3.txt")));
  }

  @Test
  public void upconfig() {
    run("solr zk upconfig -d \"${SOLR_TIP}/server/solr/configsets/sample_techproducts_configs\""
            + " -n techproducts2 -z localhost:${ZK_PORT}")
        .assertOutputContains("Uploading")
        .assertOutputNotContains("ERROR");

    // BATS polled with curl and grep; the harness' own HTTP helper needs no curl, which is what
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
    // The BATS original symlinked inside the distribution's own config set and deleted the links
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

    run(
            "solr zk upconfig -d \"%s\" -n techproducts_with_symlinks -z localhost:${ZK_PORT}",
            configSet)
        .assertOutputContains("Uploading")
        .assertOutputContains("ERROR: Not uploading symbolic link");
  }

  @Test
  public void downconfig() {
    run(
            "solr zk downconfig -z localhost:${ZK_PORT} -n _default -d \"%s\"",
            workDir().resolve("downconfig"))
        .assertOutputContains("Downloading")
        .assertOutputNotContains("ERROR");
  }

  @Test
  public void zkCpGetsSolrHomeFromTheCommandLineOption() {
    cd(workDir());
    touch("afile.txt");

    run("solr zk cp afile.txt zk:/afile.txt -z localhost:${ZK_PORT} --verbose"
            + " --solr-home ${SOLR_TIP}/server/solr")
        .assertOutputContains("Using SolrHome: ${SOLR_TIP}/server/solr")
        .assertOutputNotContains("Failed to load solr.xml from ZK or SolrHome");
  }

  @Test
  public void envVarZkHostIsHonored() {
    // SOLR_PORT_LISTEN has to be absent, not empty: the tool looks at whether it is set at all
    // before falling back to the default port.
    unset("SOLR_PORT_LISTEN");

    run("solr zk ls / --recursive")
        .assertOutputContains("assuming solr url is http://localhost:8983")
        .assertOutputNotContains("aliases.json");

    env("ZK_HOST", "localhost:" + ports().zk());
    run("solr zk ls / --recursive").assertOutputContains("aliases.json");
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
