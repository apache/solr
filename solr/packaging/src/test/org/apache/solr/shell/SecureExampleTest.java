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

import static org.apache.solr.cats.EnabledOnShell.BASH;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase.Nightly;
import org.apache.solr.cats.EnabledOnShell;
import org.apache.solr.cats.exec.CommandResult;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Port of {@code test_secure_example.bats}.
 *
 * <p>The example generates its own certificate under {@code ${SOLR_HOME}/tls}, so the requests here
 * trust its PKCS12 trust store where the BATS original passed {@code curl --cacert cert.pem} — the
 * same anchor, in the form {@link org.apache.solr.cats.session.HttpsRequest} takes. Its generated
 * password is the store's password as well as every account's.
 *
 * <p>{@code curl --fail} turned an HTTP error into a non-zero exit and a message naming the status,
 * which is what the original asserted on; here the status itself is.
 */
@Nightly // The BATS original was tagged nightly: the example starts both nodes three times over.
@EnabledOnShell(BASH)
// Bash-only: RunExampleTool refuses the secure example on Windows outright, and would in any case
// drop every SOLR_* variable from the spawned nodes' environment, so they would get neither the
// process-scan marker nor SOLR_PID_DIR and could not be discovered or stopped (SOLR-18650).
public class SecureExampleTest extends SolrCatsTestCase {

  /** Stands in for the distribution's include file, which the example must leave alone. */
  private Path globalInclude;

  @Before
  public void setUpSecureExample() {
    // The example shells out to openssl to print the certificate it generated.
    Assume.assumeTrue("OpenSSL is not available", tryRun("openssl version").succeeded());
    // The distribution's own include file is shared with every test class, so the example is
    // handed a private one and must come back without having written to it.
    globalInclude = touch("global-solr.in.sh");
    env("SOLR_INCLUDE", globalInclude.toString());
    env("SOLR_STOP_WAIT", "15");
  }

  @Test
  public void startsTwoHttpsNodesWithAuthenticationAndZooKeeperAcls() throws IOException {
    Path home = harnessEnv.solrHome();
    Files.createDirectories(home.resolve("node1"));
    Files.writeString(home.resolve("node1").resolve("incomplete-file"), "left over\n");

    // The harness turns the resource-limit checks off for every suite; the example has to turn
    // them off for itself rather than warning about them.
    env("SOLR_ULIMIT_CHECKS", "true");
    // Both nodes are started three times over and the techproducts documents indexed, which is
    // more than the harness' five-minute default allows a command.
    CommandResult example =
        run(
            Duration.ofMinutes(15),
            "solr start -e secure --no-prompt --solr-home \"${SOLR_HOME}\" -p ${SOLR_PORT}");
    example
        .assertContains("Two-node secure example is ready")
        .assertContains("http://localhost:${SOLR_PORT}/api/node/health")
        .assertContains("http://localhost:${SOLR2_PORT}/api/node/health")
        .assertNotContains("forcefully killing it now")
        .assertNotContains("set SOLR_ULIMIT_CHECKS to false")
        .assertContains("Check status: SOLR_INCLUDE=");

    String password = example.firstMatch("Generated password: ([A-Za-z0-9]+)");

    // The reset recreated SOLR_HOME: no marker file of its own, and nothing of the half-finished
    // run left behind.
    assertFalse(names(home).contains(".secure-example"));
    assertFalse(names(home.resolve("node1")).contains("incomplete-file"));

    // The example writes its own include file, never the one it was handed.
    assertEquals("", read(globalInclude.toString()));

    Path trustStore = home.resolve("tls").resolve("solr-ssl.truststore.p12");

    https("/api/cluster/security/authentication")
        .trustStore(trustStore, password)
        .basicAuth("admin", password)
        .get()
        .assertStatus(200)
        .assertContains("\"admin\"")
        .assertNotContains("\"solr\"");

    for (int port : List.of(ports().solr(), ports().solr2())) {
      https(port, "/solr/techproducts/select?q=*:*&rows=0&shards.info=true")
          .trustStore(trustStore, password)
          .basicAuth("search", password)
          .get()
          .assertStatus(200)
          .assertContains("\"numFound\":31")
          .assertContains("https://127.0.0.1:${SOLR_PORT}/")
          .assertContains("https://127.0.0.1:${SOLR2_PORT}/")
          .assertNotContains("http://");

      // Trusting nothing, so the certificate cannot be verified. curl's wording for that was
      // "certificate"; this is the TLS stack's own for the same refusal.
      https(port, "/api/node/health")
          .get()
          .assertConnectionFailed()
          .assertContains("unable to find valid certification path to requested target");

      // Trusted but unauthenticated, and 'solr' is not one of the example's accounts.
      https(port, "/api/node/health").trustStore(trustStore, password).get().assertStatus(401);
      https(port, "/api/node/health")
          .trustStore(trustStore, password)
          .basicAuth("solr", password)
          .get()
          .assertStatus(401);

      // admin administers; querying and indexing are the other two accounts' permissions.
      https(port, "/solr/techproducts/select?q=*:*")
          .trustStore(trustStore, password)
          .basicAuth("admin", password)
          .get()
          .assertStatus(403);
      https(port, "/solr/techproducts/update")
          .trustStore(trustStore, password)
          .basicAuth("admin", password)
          .post("application/json", "[{\"id\":\"forbidden-admin\"}]")
          .assertStatus(403);
      https(port, "/solr/techproducts/update")
          .trustStore(trustStore, password)
          .basicAuth("search", password)
          .post("application/json", "[{\"id\":\"forbidden\"}]")
          .assertStatus(403);

      // Both nodes serve HTTPS only.
      http(port, "/api/node/health").assertConnectionFailed();
    }

    env("SOLR_INCLUDE", home.resolve("tls").resolve("solr.in.sh").toString());
    run("solr zk ls / --zk-host \"localhost:${ZK_PORT}\"").assertContains("collections");

    env("SOLR_INCLUDE", "/dev/null");
    env("SOLR_ZK_CREDS_AND_ACLS", "");
    runFail("solr zk ls / --zk-host \"localhost:${ZK_PORT}\"").assertContains("NoAuth");

    env(
        "SOLR_ZK_CREDS_AND_ACLS",
        "-DzkCredentialsInjector=org.apache.solr.common.cloud.VMParamsZkCredentialsInjector"
            + " -DzkCredentialsProvider=org.apache.solr.common.cloud.DigestZkCredentialsProvider"
            + " -DzkDigestUsername=zk-readonly -DzkDigestPassword="
            + password);
    run("solr zk ls /collections --zk-host \"localhost:${ZK_PORT}\"")
        .assertContains("techproducts");
    runFail(
            "solr zk cp zk:/security.json \""
                + workDir().resolve("security.json")
                + "\" --zk-host \"localhost:${ZK_PORT}\"")
        .assertContains("NoAuth");
    runFail("solr zk mkroot /forbidden --zk-host \"localhost:${ZK_PORT}\"")
        .assertContains("NoAuth");
  }

  @Test
  public void rejectsUnsupportedOptions() {
    runFail("solr start -e secure --no-prompt --solr-home \"${SOLR_HOME}\" --host other-host")
        .assertContains("host");
    runFail("solr start -e secure --no-prompt --solr-home \"${SOLR_HOME}\" --user-managed")
        .assertContains("requires its own embedded ZooKeeper");
  }

  /** The entry names of a directory, which is what {@code ls -a} gave the BATS original. */
  private List<String> names(Path dir) {
    return list(dir.toString()).stream().map(entry -> entry.getFileName().toString()).toList();
  }
}
