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
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.Assume;
import org.junit.Test;

/**
 * Port of {@code test_security_manager.bats}.
 *
 * <p>The node's security manager is what this suite is about. On Java 24 and later {@code bin/solr}
 * does not install one, and Solr's own {@code allowPaths} check rejects the unpermitted directory
 * instead — hence the two alternatives in the final assertion.
 *
 * <p>The backup {@code location} is built from {@link Path#toUri()} rather than by gluing {@code
 * file://} onto the path, which would not be a valid file URL on Windows.
 */
public class SecurityManagerTest extends SolrCatsTestCase {

  @Test
  public void allowPathsBackup() throws IOException {
    // Make a test tmp dir, as the security policy includes TMP, so that might already contain the
    // session work directory.
    Path testTmpDir = mkdirs("tmp").toRealPath();

    Path backupDir = mkdirs("backup-dir").toRealPath();

    env("SOLR_SECURITY_MANAGER_ENABLED", "true");
    // As argv rather than SOLR_OPTS: the harness quotes each argument for the shell in use, where
    // a space in either path would otherwise split one -D into two JVM arguments. Quoting inside
    // SOLR_OPTS does not help, since bin/solr expands it unquoted.
    run(
        List.of(
            "solr",
            "start",
            "-Dsolr.security.allow.paths=" + backupDir,
            "-Djava.io.tmpdir=" + testTmpDir));
    run("solr create -c COLL_NAME");
    run("solr api --solr-url \"http://localhost:${SOLR_PORT}/solr/admin/collections"
            + "?action=BACKUP&name=test&collection=COLL_NAME&location="
            + fileUrl(backupDir)
            + "\"")
        .assertContains("\"status\":0");

    // Solr is not permissioned for this directory, so it should fail
    Path backupDirOther = mkdirs(backupDir.getFileName() + "-other").toRealPath();
    runFail(
            "solr api --solr-url \"http://localhost:${SOLR_PORT}/solr/admin/collections"
                + "?action=BACKUP&name=test-fail&collection=COLL_NAME&location="
                + fileUrl(backupDirOther)
                + "\"")

        // Java 24+ has no security manager; Solr's allowPaths check rejects it instead
        .assertMatches("access denied|Could not backup all shards");
  }

  /**
   * Jetty writes its state file into {@code SOLR_PID_DIR}, which the policy grants only that file
   * in. The harness default puts the PID dir in {@code SOLR_HOME}, which is granted anyway.
   */
  @Test
  public void stateFileInSeparatePidDir() throws IOException {
    Path pidDir = mkdirs("pid-dir").toRealPath();
    env("SOLR_SECURITY_MANAGER_ENABLED", "true");
    env("SOLR_PID_DIR", pidDir.toString());
    run("solr start");

    // Without a security manager this test proves nothing; fail rather than pass vacuously
    String systemInfo = http("/solr/admin/info/system").assertStatus(200).body();
    Matcher spec = Pattern.compile("\"spec\":\\{[^}]*\"version\":\"(\\d+)").matcher(systemInfo);
    assertTrue("No JVM spec version in " + systemInfo, spec.find());
    Assume.assumeTrue("Java 24+ has no security manager", Integer.parseInt(spec.group(1)) < 24);
    assertTrue(
        "Node runs without a security manager", systemInfo.contains("-Djava.security.manager"));
    Path stateFile = pidDir.resolve("solr-" + env("SOLR_PORT") + ".state");
    assertTrue(stateFile + " missing", Files.exists(stateFile));
    assertTrue(Files.readString(stateFile).contains("STARTED"));
  }

  /** A {@code file:} URL for a directory, without the trailing slash {@code toUri()} adds. */
  private static String fileUrl(Path dir) {
    String url = dir.toUri().toString();
    return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
  }
}
