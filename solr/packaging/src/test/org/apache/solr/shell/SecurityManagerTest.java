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
import java.nio.file.Path;
import org.junit.Test;

/**
 * Port of {@code test_security_manager.bats}.
 *
 * <p>Kept close to the original, including the order of the tests and the text each asserts on.
 * Where it differs:
 *
 * <ul>
 *   <li>the two directories are created with {@link #mkdirs} and canonicalized with {@code
 *       toRealPath()}, instead of {@code mkdir -p} followed by {@code cd -P && pwd};
 *   <li>the backup {@code location} is built from {@link Path#toUri()} rather than by gluing {@code
 *       file://} onto the path — the same string as the original produces on Unix, and a valid file
 *       URL on Windows too, where a bare {@code file://C:\...} would not be;
 *   <li>{@code solr start} and {@code solr create}, which the original ran without looking at the
 *       outcome, assert their exit status;
 *   <li>the rejected backup also asserts a non-zero exit status; the request is answered with HTTP
 *       500 either way the rejection happens.
 * </ul>
 *
 * <p>The node's security manager is what this suite is about. On Java 24 and later {@code bin/solr}
 * does not install one, and Solr's own {@code allowPaths} check rejects the unpermitted directory
 * instead — hence the two alternatives in the final assertion, exactly as in the original.
 */
public class SecurityManagerTest extends SolrShellTestCase {

  @Test
  public void allowPathsBackup() throws IOException {
    // Make a test tmp dir, as the security policy includes TMP, so that might already contain the
    // session work directory.
    Path testTmpDir = mkdirs("tmp").toRealPath();

    Path backupDir = mkdirs("backup-dir").toRealPath();

    env("SOLR_SECURITY_MANAGER_ENABLED", "true");
    env(
        "SOLR_OPTS",
        "-Dsolr.security.allow.paths=" + backupDir + " -Djava.io.tmpdir=" + testTmpDir);
    run("solr start").assertSuccess();
    run("solr create -c COLL_NAME").assertSuccess();
    run("solr api --solr-url \"http://localhost:${SOLR_PORT}/solr/admin/collections"
            + "?action=BACKUP&name=test&collection=COLL_NAME&location="
            + fileUrl(backupDir)
            + "\"")
        .assertContains("\"status\":0");

    // Solr is not permissioned for this directory, so it should fail
    Path backupDirOther = mkdirs(backupDir.getFileName() + "-other").toRealPath();
    run("solr api --solr-url \"http://localhost:${SOLR_PORT}/solr/admin/collections"
            + "?action=BACKUP&name=test-fail&collection=COLL_NAME&location="
            + fileUrl(backupDirOther)
            + "\"")
        .assertFailure()
        // Java 24+ has no security manager; Solr's allowPaths check rejects it instead
        .assertMatches("access denied|Could not backup all shards");
  }

  /** A {@code file:} URL for a directory, without the trailing slash {@code toUri()} adds. */
  private static String fileUrl(Path dir) {
    String url = dir.toUri().toString();
    return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
  }
}
