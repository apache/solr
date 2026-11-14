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
import org.apache.solr.cats.EnabledOnShell;
import org.junit.Before;
import org.junit.Test;

/**
 * Port of {@code test_ssl.bats}.
 *
 * <p>{@link #https} trusts the keystore directly, so there is no PEM conversion and no {@code
 * openssl} — which is what lets most of this suite run on Windows. {@code keytool} is invoked by
 * absolute path from the test JVM's {@code java.home}, the Gradle test environment not guaranteeing
 * it on {@code PATH}, and the mTLS tests use its own {@code -file}/{@code -infile}/{@code -outfile}
 * options rather than a pipe.
 *
 * <p>{@code solr auth enable} appends to the include file, by default the distribution's own and so
 * shared with every test class, hence {@code SOLR_INCLUDE} pointing at a private copy. {@link
 * #wipeSolrHomeBeforeEachTest} wipes {@code SOLR_HOME} per test, which the harness does only per
 * class — without it the collection named {@code test} would survive into the next test.
 *
 * <p>No JDK HTTP client can send a {@code Host} header for a name it has not resolved, so the
 * {@code curl --resolve} peer-name case lives in a bash-only companion test, {@link
 * #useDifferentHostnameWithCurlResolve}.
 */
public class SslTest extends SolrCatsTestCase {

  private static final String SECRET = "secret";

  /** The JDK's keytool, which both platforms have, by absolute path rather than via PATH. */
  private static final String KEYTOOL =
      "\"" + Path.of(System.getProperty("java.home"), "bin", "keytool") + "\"";

  @Before
  public void wipeSolrHomeBeforeEachTest() throws IOException {
    // Every test starts its own Solr, from a clean SOLR_HOME.
    IOUtils.rm(harnessEnv.solrHome());
    Files.createDirectories(harnessEnv.logsDir());
  }

  @Test
  public void startSolrWithSsl() {
    Path keystore = createKeystore("solr-ssl", "solr-ssl", "localhost", "127.0.0.1");

    // Set ENV_VARs so that Solr uses this keystore
    env("SOLR_SSL_ENABLED", "true");
    env("SOLR_SSL_KEY_STORE", keystore.toString());
    env("SOLR_SSL_KEY_STORE_PASSWORD", SECRET);
    env("SOLR_SSL_TRUST_STORE", keystore.toString());
    env("SOLR_SSL_TRUST_STORE_PASSWORD", SECRET);
    env("SOLR_SSL_NEED_CLIENT_AUTH", "false");
    env("SOLR_SSL_WANT_CLIENT_AUTH", "false");
    env("SOLR_HOST_ADVERTISE", "localhost");

    run("solr start");
    run("solr assert --started https://localhost:${SOLR_PORT} --timeout 5000");

    run("solr create -c test --shards 2").assertContains("Created collection 'test'");

    run("solr api --solr-url \"https://localhost:${SOLR_PORT}/solr/test/select?q=*:*\"")
        .assertContains("\"numFound\":0");

    https("/solr/test/select?q=*:*")
        .trustStore(keystore, SECRET)
        .get()
        .assertContains("\"numFound\":0");

    // The certificate is self-signed, so a client that does not trust it cannot connect.
    https("/solr/test/select?q=*:*").get().assertConnectionFailed();
  }

  @Test
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-18336")
  public void useDifferentHostnameWhenNotCheckingPeerName() {
    // Using a CN that is not localhost, as we will not be checking peer-name
    Path keystore = createKeystore("solr-ssl", "solr-ssl", "test.solr.apache.org", "127.0.0.1");

    // Set ENV_VARs so that Solr uses this keystore
    env("SOLR_SSL_ENABLED", "true");
    env("SOLR_SSL_KEY_STORE", keystore.toString());
    env("SOLR_SSL_KEY_STORE_PASSWORD", SECRET);
    env("SOLR_SSL_TRUST_STORE", keystore.toString());
    env("SOLR_SSL_TRUST_STORE_PASSWORD", SECRET);
    env("SOLR_SSL_NEED_CLIENT_AUTH", "false");
    env("SOLR_SSL_WANT_CLIENT_AUTH", "false");
    env("SOLR_SSL_CHECK_PEER_NAME", "false");
    env("SOLR_HOST_ADVERTISE", "127.0.0.1");

    run("solr start");
    run("solr assert --started https://localhost:${SOLR_PORT} --timeout 5000");

    run("solr create -c test --shards 2").assertContains("Created collection 'test'");

    run("solr api --solr-url \"https://localhost:${SOLR_PORT}/solr/test/select?q=*:*\"")
        .assertContains("\"numFound\":0");

    // Trusting the certificate is not enough: its names do not cover localhost. This
    // asserted curl's wording for this; here it is the TLS stack's own, the same text the
    // --check-peer-name assertion below looks for.
    https("/solr/test/select?q=*:*")
        .trustStore(keystore, SECRET)
        .get()
        .assertConnectionFailed()
        .assertContains("No subject alternative DNS name matching localhost found");

    // java.net.http has no per-request way to turn hostname
    // verification off, so the request addresses the node by the IP address the certificate
    // does cover, which makes the same point: the connection itself is fine.
    https("/solr/test/select?q=*:*")
        .host("127.0.0.1")
        .trustStore(keystore, SECRET)
        .get()
        .assertContains("\"numFound\":0");

    // This is a client setting, so we don't need to restart Solr to make sure that it fails
    env("SOLR_SSL_CHECK_PEER_NAME", "true");

    // This should fail the peername check
    runFail(
            "solr api --verbose --solr-url \"https://localhost:${SOLR_PORT}/solr/test/select?q=*:*\"")
        .assertMatches(
            "(No subject alternative DNS name matching localhost found|Server refused connection)");

    // Restart the server enabling the SNI hostcheck
    env("SOLR_SSL_CHECK_PEER_NAME", "false");
    env("SOLR_OPTS", solrOpts() + " -Dsolr.jetty.ssl.sni.host.check.enabled=true");
    run("solr restart");

    // This should fail the SNI Hostname check
    runFail(
            "solr api --verbose --solr-url"
                + " \"https://localhost:${SOLR_PORT}/solr/admin/collections?action=CLUSTERSTATUS\"")
        .assertContains("Invalid SNI");
  }

  @Test
  @EnabledOnShell("bash") // curl --resolve has no cross-shell equivalent; see the class javadoc.
  public void useDifferentHostnameWithCurlResolve() {
    Path keystore = createKeystore("solr-ssl", "solr-ssl", "test.solr.apache.org", "127.0.0.1");
    Path pem = path("ssl/solr-ssl.pem");
    run(
        "openssl pkcs12 -in \""
            + keystore
            + "\" -out \""
            + pem
            + "\""
            + " -passin pass:secret -passout pass:secret");

    env("SOLR_SSL_ENABLED", "true");
    env("SOLR_SSL_KEY_STORE", keystore.toString());
    env("SOLR_SSL_KEY_STORE_PASSWORD", SECRET);
    env("SOLR_SSL_TRUST_STORE", keystore.toString());
    env("SOLR_SSL_TRUST_STORE_PASSWORD", SECRET);
    env("SOLR_SSL_NEED_CLIENT_AUTH", "false");
    env("SOLR_SSL_WANT_CLIENT_AUTH", "false");
    env("SOLR_SSL_CHECK_PEER_NAME", "false");
    env("SOLR_HOST_ADVERTISE", "127.0.0.1");

    run("solr start");
    run("solr assert --started https://localhost:${SOLR_PORT} --timeout 5000");
    run("solr create -c test --shards 2").assertContains("Created collection 'test'");

    run("curl --http2 --cacert \""
            + pem
            + "\""
            + " --resolve \"test.solr.apache.org:${SOLR_PORT}:127.0.0.1\""
            + " \"https://test.solr.apache.org:${SOLR_PORT}/solr/test/select?q=*:*\"")
        .assertContains("\"numFound\":0");

    // Restart the server enabling the SNI hostcheck; using the right hostname should not fail it.
    env("SOLR_OPTS", solrOpts() + " -Dsolr.jetty.ssl.sni.host.check.enabled=true");
    run("solr restart");

    run("curl --http2 --cacert \""
            + pem
            + "\""
            + " --resolve \"test.solr.apache.org:${SOLR_PORT}:127.0.0.1\""
            + " \"https://test.solr.apache.org:${SOLR_PORT}"
            + "/solr/admin/collections?action=CLUSTERSTATUS\"")
        .assertContains("\"urlScheme\":\"https\"");
  }

  @Test
  public void startSolrWithSslAndAuth() {
    Path keystore = createKeystore("solr-ssl", "solr-ssl", "localhost", "127.0.0.1");

    // Set ENV_VARs so that Solr uses this keystore
    env("SOLR_SSL_ENABLED", "true");
    env("SOLR_SSL_KEY_STORE", keystore.toString());
    env("SOLR_SSL_KEY_STORE_PASSWORD", SECRET);
    env("SOLR_SSL_TRUST_STORE", keystore.toString());
    env("SOLR_SSL_TRUST_STORE_PASSWORD", SECRET);
    env("SOLR_SSL_NEED_CLIENT_AUTH", "false");
    env("SOLR_SSL_WANT_CLIENT_AUTH", "false");
    env("SOLR_SSL_CHECK_PEER_NAME", "true");
    env("SOLR_HOST_ADVERTISE", "localhost");

    // 'solr auth enable' writes into the include file, which by default is the one in the
    // distribution that every test class shares. A private copy keeps this test to itself.
    // The name has to keep its extension: bin\solr.cmd CALLs the include file.
    String includeFile = "bash".equals(cli.shell().id()) ? "solr.in.sh" : "solr.in.cmd";
    env(
        "SOLR_INCLUDE",
        copy(harnessEnv.solrTip().resolve("bin").resolve(includeFile).toString(), includeFile)
            .toString());

    run("solr start");
    run("solr assert --started https://localhost:${SOLR_PORT} --timeout 5000");
    run("solr auth enable --type basicAuth --credentials name:password");

    https(
            "/solr/admin/collections?action=CREATE&collection.configName=_default&name=test"
                + "&numShards=2&replicationFactor=1&router.name=compositeId&wt=json")
        .trustStore(keystore, SECRET)
        .basicAuth("name", "password")
        .get()
        .assertContains("\"status\":0");

    https("/solr/test/select?q=*:*")
        .trustStore(keystore, SECRET)
        .basicAuth("name", "password")
        .get()
        .assertContains("\"numFound\":0");

    // Unauthenticated, so this one is refused rather than failing to connect.
    https("/solr/test/select?q=*:*")
        .trustStore(keystore, SECRET)
        .get()
        .assertContains("Error 401 Authentication");

    // Only this test has anything to disable.
    run("solr auth disable");
  }

  @Test
  public void startSolrWithClientTruststoreAndSecurityManager() throws IOException {
    // Make a test tmp dir, as the security policy includes TMP, so that might already contain the
    // work directory the keystores live in.
    // toRealPath() for the same reason SecurityManagerTest canonicalises: the security policy
    // grants on the real path, so a java.io.tmpdir reached through a symlink is not covered and
    // the node dies under the security manager. The BATS original used cd -P && pwd here.
    Path testTmpDir = mkdirs("tmp").toRealPath();

    env("SOLR_SECURITY_MANAGER_ENABLED", "true");
    env("SOLR_OPTS", "-Djava.io.tmpdir=" + testTmpDir);
    env("SOLR_TOOL_OPTS", "-Djava.io.tmpdir=" + testTmpDir);

    // Create a keystore
    Path keystore = createKeystore("solr-ssl", "solr-ssl", "localhost", "127.0.0.1");

    // Create a truststore holding just the exported certificate
    mkdirs("ssl-client");
    Path certificate = path("ssl-client/solr-ssl.crt");
    Path clientTruststore = path("ssl-client/solr-ssl.truststore.p12");
    run(
        KEYTOOL
            + " -export -alias solr-ssl -file \""
            + certificate
            + "\" -keystore \""
            + keystore
            + "\" -keypass secret -storepass secret");
    run(
        KEYTOOL
            + " -import -v -trustcacerts -alias solr-ssl -file \""
            + certificate
            + "\" -storetype PKCS12 -keystore \""
            + clientTruststore
            + "\" -keypass secret -storepass secret -noprompt");
    copy("ssl", "ssl-client/ssl");

    // Set ENV_VARs so that Solr uses this keystore
    env("SOLR_SSL_ENABLED", "true");
    env("SOLR_SSL_KEY_STORE", keystore.toString());
    env("SOLR_SSL_KEY_STORE_PASSWORD", SECRET);
    env("SOLR_SSL_TRUST_STORE", keystore.toString());
    env("SOLR_SSL_TRUST_STORE_PASSWORD", SECRET);
    env("SOLR_SSL_CLIENT_TRUST_STORE", clientTruststore.toString());
    env("SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD", SECRET);
    env("SOLR_SSL_NEED_CLIENT_AUTH", "false");
    env("SOLR_SSL_WANT_CLIENT_AUTH", "true");
    env("SOLR_SSL_CHECK_PEER_NAME", "true");
    env("SOLR_HOST_ADVERTISE", "localhost");

    run("solr start");

    run("solr assert --started https://localhost:${SOLR_PORT} --timeout 5000");

    env("SOLR_SSL_KEY_STORE", "");
    env("SOLR_SSL_KEY_STORE_PASSWORD", "");
    env("SOLR_SSL_TRUST_STORE", "");
    env("SOLR_SSL_TRUST_STORE_PASSWORD", "");

    run("solr create -c test --shards 2").assertContains("Created collection 'test'");

    run("solr api --solr-url"
            + " \"https://localhost:${SOLR_PORT}/solr/admin/collections?action=CLUSTERSTATUS\"")
        .assertContains("\"urlScheme\":\"https\"");
  }

  @Test
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-18646")
  public void startSolrWithMtlsNeeded() throws IOException {
    // toRealPath() for the same reason SecurityManagerTest canonicalises: the security policy
    // grants on the real path, so a java.io.tmpdir reached through a symlink is not covered and
    // the node dies under the security manager. The BATS original used cd -P && pwd here.
    Path testTmpDir = mkdirs("tmp").toRealPath();

    env("SOLR_SECURITY_MANAGER_ENABLED", "true");
    env("SOLR_OPTS", "-Djava.io.tmpdir=" + testTmpDir);
    env("SOLR_LOG_LEVEL", "DEBUG");
    // To debug further use: -Djavax.net.debug=SSL,keymanager,trustmanager,ssl:handshake
    env("SOLR_TOOL_OPTS", "-Djava.io.tmpdir=" + testTmpDir);

    createCertificateAuthority();
    createSignedStores("server", "localhost", "DNS:localhost,IP:127.0.0.1", "serverAuth");
    // The client cert has a bogus DNS name and IP address, as we want the
    // clientHostnameVerification to fail
    createSignedStores(
        "client", "test.solr.apache.org", "DNS:test.solr.apache.org,IP:127.0.0.2", "clientAuth");

    // Set ENV_VARs so that Solr uses this keystore
    env("SOLR_SSL_ENABLED", "true");
    env("SOLR_SSL_KEY_STORE", path("ssl/server/solr-server.keystore.p12").toString());
    env("SOLR_SSL_KEY_STORE_PASSWORD", "server-key");
    env("SOLR_SSL_KEY_STORE_TYPE", "PKCS12");
    env("SOLR_SSL_TRUST_STORE", path("ssl/server/solr-server.truststore.p12").toString());
    env("SOLR_SSL_TRUST_STORE_PASSWORD", "server-trust");
    env("SOLR_SSL_TRUST_STORE_TYPE", "PKCS12");
    env("SOLR_SSL_CLIENT_KEY_STORE", path("ssl/client/solr-client.keystore.p12").toString());
    env("SOLR_SSL_CLIENT_KEY_STORE_PASSWORD", "client-key");
    env("SOLR_SSL_CLIENT_KEY_STORE_TYPE", "PKCS12");
    env("SOLR_SSL_CLIENT_TRUST_STORE", path("ssl/client/solr-client.truststore.p12").toString());
    env("SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD", "client-trust");
    env("SOLR_SSL_CLIENT_TRUST_STORE_TYPE", "PKCS12");
    env("SOLR_SSL_NEED_CLIENT_AUTH", "true");
    env("SOLR_SSL_WANT_CLIENT_AUTH", "false");
    env("SOLR_SSL_CHECK_PEER_NAME", "true");
    // Cannot set this to true, because the client certificate does not have the right hostname
    // ("localhost") or IP
    env("SOLR_SSL_CLIENT_HOSTNAME_VERIFICATION", "false");
    env("SOLR_HOST_ADVERTISE", "localhost");

    run("solr start");
    run("solr start -z localhost:${ZK_PORT} -p ${SOLR2_PORT}");

    // Test Client connections, which do not need the server keystore/truststore
    env("SOLR_SSL_KEY_STORE", "");
    env("SOLR_SSL_KEY_STORE_PASSWORD", "");
    env("SOLR_SSL_TRUST_STORE", "");
    env("SOLR_SSL_TRUST_STORE_PASSWORD", "");

    run("solr assert --started https://localhost:${SOLR_PORT} --timeout 5000");
    run("solr assert --started https://localhost:${SOLR2_PORT} --timeout 5000");

    run("solr create -c test --shards 2").assertContains("Created collection 'test'");

    run("solr api --solr-url"
            + " \"https://localhost:${SOLR_PORT}/solr/admin/collections?action=CLUSTERSTATUS\"")
        .assertContains("\"urlScheme\":\"https\"");

    run("solr api --solr-url"
            + " \"https://localhost:${SOLR2_PORT}/solr/test/select?q=*:*&rows=0\"")
        .assertContains("\"numFound\":0");

    // Try connecting with just the client truststore, which should fail because mTLS requires a
    // keystore and mTLS is needed
    env("SOLR_SSL_CLIENT_KEY_STORE", "");
    env("SOLR_SSL_CLIENT_KEY_STORE_PASSWORD", "");

    runFail(
            "solr api --verbose --solr-url"
                + " \"https://localhost:${SOLR_PORT}/solr/test/select?q=*:*&rows=0\"")
        .assertMatches(
            "(bad_certificate|java.nio.channels.ClosedChannelException"
                + "|Server refused connection)");

    // That wording is also what a node being down produces, and it differs between platforms, so
    // it cannot be tightened. Prove in-JVM that the rejection really was the handshake.
    https("/solr/test/select?q=*:*&rows=0")
        .get()
        .assertConnectionFailed()
        .assertMatches("(?s).*(SSLHandshakeException|SSLException|fatal alert).*");

    // The four server variables and the client
    // keystore for what follows.
    env("SOLR_SSL_CLIENT_KEY_STORE", path("ssl/client/solr-client.keystore.p12").toString());
    env("SOLR_SSL_CLIENT_KEY_STORE_PASSWORD", "client-key");
    env("SOLR_SSL_KEY_STORE", path("ssl/server/solr-server.keystore.p12").toString());
    env("SOLR_SSL_KEY_STORE_PASSWORD", "server-key");
    env("SOLR_SSL_TRUST_STORE", path("ssl/server/solr-server.truststore.p12").toString());
    env("SOLR_SSL_TRUST_STORE_PASSWORD", "server-trust");

    // Turn on client hostname verification, and start a new Solr node since the property is a
    // server setting. Test that it fails because the client cert does not use "localhost"
    env("SOLR_SSL_CLIENT_HOSTNAME_VERIFICATION", "true");
    run("solr start -z localhost:${ZK_PORT} -p ${SOLR3_PORT}");

    // A sleep would do, there being no way to connect to the node to check it is up. Its
    // registration in ZooKeeper is visible from a node we can talk to, so that is what we wait
    // for instead.
    awaitOrFail(
        "the third node to register in ZooKeeper",
        Duration.ofMinutes(1),
        Duration.ofSeconds(2),
        () ->
            run("solr api --solr-url \"https://localhost:${SOLR_PORT}"
                    + "/solr/admin/collections?action=CLUSTERSTATUS\"")
                .output()
                .contains("localhost:" + ports().solr3() + "_solr"));

    runFail(
            "solr api --verbose --solr-url"
                + " \"https://localhost:${SOLR3_PORT}/solr/test/select?q=*:*&rows=0\"")
        .assertMatches(
            "(certificate_unknown|java.nio.channels.ClosedChannelException"
                + "|Server refused connection)");

    // As above: the CLI cannot distinguish a TLS rejection from an unreachable node.
    https(ports().solr3(), "/solr/test/select?q=*:*&rows=0")
        .get()
        .assertConnectionFailed()
        .assertMatches("(?s).*(SSLHandshakeException|SSLException|fatal alert).*");
  }

  @Test
  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-18646")
  public void startSolrWithMtlsWanted() throws IOException {
    // toRealPath() for the same reason SecurityManagerTest canonicalises: the security policy
    // grants on the real path, so a java.io.tmpdir reached through a symlink is not covered and
    // the node dies under the security manager. The BATS original used cd -P && pwd here.
    Path testTmpDir = mkdirs("tmp").toRealPath();

    env("SOLR_SECURITY_MANAGER_ENABLED", "true");
    env("SOLR_OPTS", "-Djava.io.tmpdir=" + testTmpDir);
    env("SOLR_TOOL_OPTS", "-Djava.io.tmpdir=" + testTmpDir);

    createCertificateAuthority();
    createSignedStores("server", "localhost", "DNS:localhost,IP:127.0.0.1", "serverAuth");
    createSignedStores("client", "localhost", "DNS:localhost,IP:127.0.0.1", "clientAuth");

    // Set ENV_VARs so that Solr uses this keystore
    env("SOLR_SSL_ENABLED", "true");
    env("SOLR_SSL_KEY_STORE", path("ssl/server/solr-server.keystore.p12").toString());
    env("SOLR_SSL_KEY_STORE_PASSWORD", "server-key");
    env("SOLR_SSL_KEY_STORE_TYPE", "PKCS12");
    env("SOLR_SSL_TRUST_STORE", path("ssl/server/solr-server.truststore.p12").toString());
    env("SOLR_SSL_TRUST_STORE_PASSWORD", "server-trust");
    env("SOLR_SSL_TRUST_STORE_TYPE", "PKCS12");
    env("SOLR_SSL_CLIENT_KEY_STORE", path("ssl/client/solr-client.keystore.p12").toString());
    env("SOLR_SSL_CLIENT_KEY_STORE_PASSWORD", "client-key");
    env("SOLR_SSL_CLIENT_KEY_STORE_TYPE", "PKCS12");
    env("SOLR_SSL_CLIENT_TRUST_STORE", path("ssl/client/solr-client.truststore.p12").toString());
    env("SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD", "client-trust");
    env("SOLR_SSL_CLIENT_TRUST_STORE_TYPE", "PKCS12");
    env("SOLR_SSL_NEED_CLIENT_AUTH", "false");
    env("SOLR_SSL_WANT_CLIENT_AUTH", "true");
    env("SOLR_SSL_CHECK_PEER_NAME", "true");
    env("SOLR_SSL_CLIENT_HOSTNAME_VERIFICATION", "true");
    env("SOLR_HOST_ADVERTISE", "localhost");

    run("solr start");
    run("solr start -z localhost:${ZK_PORT} -p ${SOLR2_PORT}");

    env("SOLR_SSL_KEY_STORE", "");
    env("SOLR_SSL_KEY_STORE_PASSWORD", "");
    env("SOLR_SSL_TRUST_STORE", "");
    env("SOLR_SSL_TRUST_STORE_PASSWORD", "");

    run("solr assert --started https://localhost:${SOLR_PORT} --timeout 5000");
    run("solr assert --started https://localhost:${SOLR2_PORT} --timeout 5000");

    run("solr create -c test --shards 2").assertContains("Created collection 'test'");

    run("solr api --solr-url"
            + " \"https://localhost:${SOLR_PORT}/solr/admin/collections?action=CLUSTERSTATUS\"")
        .assertContains("\"urlScheme\":\"https\"");

    run("solr api --solr-url"
            + " \"https://localhost:${SOLR2_PORT}/solr/test/select?q=*:*&rows=0\"")
        .assertContains("\"numFound\":0");

    env("SOLR_SSL_CLIENT_KEY_STORE", "");
    env("SOLR_SSL_CLIENT_KEY_STORE_PASSWORD", "");

    // mTLS requires a keyStore, so just using the truststore would fail if mTLS was "NEED"ed,
    // however it is only "WANT"ed, so its ok
    run("solr api --solr-url" + " \"https://localhost:${SOLR_PORT}/solr/test/select?q=*:*&rows=0\"")
        .assertContains("\"numFound\":0");

    env("SOLR_SSL_CLIENT_TRUST_STORE", "");
    env("SOLR_SSL_CLIENT_TRUST_STORE_PASSWORD", "");

    // TLS cannot work if a truststore and keystore are not provided (either Server or Client)
    runFail(
            "solr api --verbose --solr-url"
                + " \"https://localhost:${SOLR_PORT}/solr/test/select?q=*:*&rows=0\"")
        .assertMatches(
            "(unable to find valid certification path to requested target"
                + "|Server refused connection)");

    // As above: without a truststore this is a certification-path failure, not an absent node.
    https("/solr/test/select?q=*:*&rows=0")
        .get()
        .assertConnectionFailed()
        .assertMatches("(?s).*(SSLHandshakeException|SSLException|fatal alert).*");
  }

  private static final String RELOAD_JVM_OPTS =
      "-Dsolr.jetty.ssl.context.reload.scan.interval.secs=1 -DsocketTimeout=20000";

  @Test
  public void testKeystoreReload() {
    // cert and keystore 1, cert and keystore 2. No PEM export is needed with
    // openssl; nothing in the test used them.
    createKeystore("cert1", "cert1", "localhost", "127.0.0.1");
    Path cert2 = createKeystore("cert2", "cert2", "localhost", "127.0.0.1");

    copy("ssl/cert1.keystore.p12", "ssl/server1.keystore.p12");
    copy("ssl/cert1.keystore.p12", "ssl/server2.keystore.p12");
    Path server1 = path("ssl/server1.keystore.p12");
    Path server2 = path("ssl/server2.keystore.p12");

    // Set ENV_VARs so that Solr uses this keystore
    env("SOLR_SSL_ENABLED", "true");
    env("SOLR_SSL_KEY_STORE_PASSWORD", SECRET);
    env("SOLR_SSL_TRUST_STORE_PASSWORD", SECRET);
    env("SOLR_SSL_NEED_CLIENT_AUTH", "true");
    env("SOLR_SSL_WANT_CLIENT_AUTH", "false");
    env("SOLR_HOST_ADVERTISE", "localhost");

    // server1 will run on $SOLR_PORT and will use server1.keystore
    env("SOLR_SSL_KEY_STORE", server1.toString());
    env("SOLR_SSL_TRUST_STORE", server1.toString());
    // It only has to be short enough that a request to a node
    // serving the wrong certificate gives up quickly, and 5s is not enough to create a collection
    // across two nodes while the other suites have the machine busy.
    run("solr start --jvm-opts \"" + RELOAD_JVM_OPTS + "\"");
    run("solr assert --started https://localhost:${SOLR_PORT} --timeout 5000");

    // server2 will run on $SOLR2_PORT and will use server2.keystore. Initially, this is the same
    // as server1.keystore
    env("SOLR_SSL_KEY_STORE", server2.toString());
    env("SOLR_SSL_TRUST_STORE", server2.toString());

    run(
        "solr start -z localhost:${ZK_PORT} -p ${SOLR2_PORT} --jvm-opts \""
            + RELOAD_JVM_OPTS
            + "\"");
    run("solr assert --started https://localhost:${SOLR2_PORT} --timeout 5000");

    // "test" collection is two shards, meaning there must be communication between shards for
    // queries (handled by http shard handler factory)
    run("solr create -c test --shards 2").assertContains("Created collection 'test'");

    // "test-single-shard" is one shard and one replica, this means that one of the nodes will
    // have to forward requests to the other
    run("solr create -c test-single-shard --shards 1")
        .assertContains("Created collection 'test-single-shard'");

    run("solr api --solr-url \"https://localhost:${SOLR_PORT}/solr/test/select?q=*:*\"")
        .assertContains("\"numFound\":0");
    run("solr api --solr-url \"https://localhost:${SOLR2_PORT}/solr/test/select?q=*:*\"")
        .assertContains("\"numFound\":0");

    run("solr api --solr-url"
            + " \"https://localhost:${SOLR_PORT}/solr/test-single-shard/select?q=*:*\"")
        .assertContains("\"numFound\":0");
    run("solr api --solr-url"
            + " \"https://localhost:${SOLR2_PORT}/solr/test-single-shard/select?q=*:*\"")
        .assertContains("\"numFound\":0");

    https("/solr/test/select?q=*:*").get().assertConnectionFailed();
    https(ports().solr2(), "/solr/test/select?q=*:*").get().assertConnectionFailed();

    https("/solr/test-single-shard/select?q=*:*").get().assertConnectionFailed();
    https(ports().solr2(), "/solr/test-single-shard/select?q=*:*").get().assertConnectionFailed();

    env("SOLR_SSL_KEY_STORE", cert2.toString());
    env("SOLR_SSL_KEY_STORE_PASSWORD", SECRET);
    env("SOLR_SSL_TRUST_STORE", cert2.toString());
    env("SOLR_SSL_TRUST_STORE_PASSWORD", SECRET);

    runFail("solr api --solr-url \"https://localhost:${SOLR_PORT}/solr/test/select?q=*:*\"");

    // Replace server1 keystore with client's
    copy("ssl/cert2.keystore.p12", "ssl/server1.keystore.p12");

    // Wait for Jetty keystore-reload scan to pick up the new certificate
    awaitOrFail(
        "the first node to serve the new certificate",
        Duration.ofSeconds(30),
        Duration.ofSeconds(1),
        () ->
            tryRun(
                    "solr api --solr-url"
                        + " \"https://localhost:${SOLR_PORT}/solr/admin/info/system\"")
                .succeeded());

    run("solr healthcheck -c test --solr-url https://localhost:${SOLR_PORT}");

    // Server 2 still uses the cert1, so this request should fail
    runFail("solr api --solr-url \"https://localhost:${SOLR2_PORT}/solr/test/select?q=query2\"");

    runFail("solr healthcheck -c test --solr-url https://localhost:${SOLR2_PORT}");

    // Replace server2 keystore with client's
    copy("ssl/cert2.keystore.p12", "ssl/server2.keystore.p12");

    // Wait for Jetty keystore-reload scan to pick up the new certificate
    awaitOrFail(
        "the second node to serve the new certificate",
        Duration.ofSeconds(30),
        Duration.ofSeconds(1),
        () ->
            tryRun(
                    "solr api --solr-url"
                        + " \"https://localhost:${SOLR2_PORT}/solr/admin/info/system\"")
                .succeeded());

    run("solr healthcheck -c test --solr-url https://localhost:${SOLR_PORT}");
    run("solr healthcheck -c test --solr-url https://localhost:${SOLR2_PORT}");

    run("solr api --solr-url \"https://localhost:${SOLR_PORT}/solr/test/select?q=query3\"")
        .assertContains("\"numFound\":0");

    run("solr api --solr-url \"https://localhost:${SOLR2_PORT}/solr/test/select?q=query3\"")
        .assertContains("\"numFound\":0");

    run("solr api --solr-url"
            + " \"https://localhost:${SOLR_PORT}/solr/test-single-shard/select?q=query4\"")
        .assertContains("\"numFound\":0");

    run("solr api --solr-url"
            + " \"https://localhost:${SOLR2_PORT}/solr/test-single-shard/select?q=query4\"")
        .assertContains("\"numFound\":0");

    run(
        "solr post --solr-url https://localhost:${SOLR_PORT} -c test"
            + " \"${SOLR_TIP}/example/exampledocs/books.csv\"");

    run("solr api --solr-url \"https://localhost:${SOLR_PORT}/solr/test/select?q=*:*\"")
        .assertContains("\"numFound\":10");
  }

  // --------------------------------------------------------------- keystore generation

  /**
   * The {@code keytool -genkeypair} block each test opens with, writing into an {@code ssl}
   * directory of the session's own work directory.
   */
  private Path createKeystore(String name, String alias, String dnsName, String ip) {
    mkdirs("ssl");
    Path keystore = path("ssl/" + name + ".keystore.p12");
    run(
        KEYTOOL
            + " -genkeypair -alias "
            + alias
            + " -keyalg RSA -keysize 2048 -keypass secret -storepass secret -validity 9999"
            + " -keystore \""
            + keystore
            + "\" -storetype PKCS12 -ext \"SAN=DNS:"
            + dnsName
            + ",IP:"
            + ip
            + "\" -dname \""
            + dname(dnsName)
            + "\"");
    return keystore;
  }

  /** Port of the root and intermediate CA the two mTLS tests sign their certificates with. */
  private void createCertificateAuthority() {
    mkdirs("ssl");
    Path root = path("ssl/root.p12");
    Path ca = path("ssl/ca.p12");
    Path rootPem = path("ssl/root.pem");
    Path caPem = path("ssl/ca.pem");
    Path caCsr = path("ssl/ca.csr");

    run(
        KEYTOOL
            + " -genkeypair -keystore \""
            + root
            + "\" -storetype PKCS12 -keypass secret -storepass secret -alias root -ext bc:c"
            + " -ext \"SAN=DNS:localhost,IP:127.0.0.1\" -dname \""
            + dname("localhost")
            + "\" -keyalg rsa");
    run(
        KEYTOOL
            + " -genkeypair -keystore \""
            + ca
            + "\" -storetype PKCS12 -keypass secret -storepass secret -alias ca -ext bc:c"
            + " -ext \"SAN=DNS:localhost,IP:127.0.0.1\" -dname \""
            + dname("localhost")
            + "\" -keyalg rsa");

    run(
        KEYTOOL
            + " -keystore \""
            + root
            + "\" -storetype PKCS12 -storepass secret -alias root -exportcert -rfc -file \""
            + rootPem
            + "\"");

    run(
        KEYTOOL
            + " -storepass secret -storetype PKCS12 -keystore \""
            + ca
            + "\" -certreq -alias ca -file \""
            + caCsr
            + "\"");
    run(
        KEYTOOL
            + " -storepass secret -keystore \""
            + root
            + "\" -storetype PKCS12 -gencert -alias root -ext BC=0 -rfc -infile \""
            + caCsr
            + "\" -outfile \""
            + caPem
            + "\"");
    run(
        KEYTOOL
            + " -keystore \""
            + ca
            + "\" -importcert -storetype PKCS12 -storepass secret -alias root -file \""
            + rootPem
            + "\" -noprompt");
    run(
        KEYTOOL
            + " -keystore \""
            + ca
            + "\" -importcert -storetype PKCS12 -storepass secret -alias ca -file \""
            + caPem
            + "\"");
  }

  /**
   * Port of the server and client store generation in the two mTLS tests: a keypair, a certificate
   * the CA signs, the CA chain imported alongside it, and a truststore holding just the CA chain.
   */
  private void createSignedStores(
      String role, String commonName, String san, String extendedKeyUsage) {
    String dir = "ssl/" + role;
    mkdirs(dir);
    Path keystore = path(dir + "/solr-" + role + ".keystore.p12");
    Path truststore = path(dir + "/solr-" + role + ".truststore.p12");
    Path pem = path(dir + "/" + role + ".pem");
    Path csr = path(dir + "/" + role + ".csr");
    Path rootPem = path("ssl/root.pem");
    Path caPem = path("ssl/ca.pem");
    String keyPassword = role + "-key";
    String trustPassword = role + "-trust";

    // Create a keystore and certificate
    run(
        KEYTOOL
            + " -genkeypair -keystore \""
            + keystore
            + "\" -storetype PKCS12 -keypass "
            + keyPassword
            + " -storepass "
            + keyPassword
            + " -alias "
            + role
            + " -ext \"SAN="
            + san
            + "\" -dname \""
            + dname(commonName)
            + "\" -keyalg rsa");

    // Trust the keystore cert with the CA
    run(
        KEYTOOL
            + " -storepass "
            + keyPassword
            + " -keystore \""
            + keystore
            + "\" -storetype PKCS12 -certreq -alias "
            + role
            + " -file \""
            + csr
            + "\"");
    run(
        KEYTOOL
            + " -storepass secret -keystore \""
            + path("ssl/ca.p12")
            + "\" -storetype PKCS12 -gencert -alias ca -ext \"SAN="
            + san
            + "\" -ext \"ku:c=nonRepudiation,digitalSignature,keyEncipherment\""
            + " -ext \"eku:c="
            + extendedKeyUsage
            + "\" -rfc -infile \""
            + csr
            + "\" -outfile \""
            + pem
            + "\"");
    importCert(keystore, keyPassword, "root", rootPem, true);
    importCert(keystore, keyPassword, "ca", caPem, true);
    importCert(keystore, keyPassword, role, pem, false);

    // Create a truststore with just the Root CA
    importCert(truststore, trustPassword, "root", rootPem, true);
    importCert(truststore, trustPassword, "ca", caPem, true);
  }

  private void importCert(
      Path store, String password, String alias, Path certificate, boolean noPrompt) {
    run(
        KEYTOOL
            + " -keystore \""
            + store
            + "\" -storetype PKCS12 -keypass "
            + password
            + " -storepass "
            + password
            + " -importcert -alias "
            + alias
            + " -file \""
            + certificate
            + "\""
            + (noPrompt ? " -noprompt" : ""));
  }

  private static String dname(String commonName) {
    return "CN="
        + commonName
        + ", OU=Organizational Unit, O=Organization, L=Location, ST=State, C=Country";
  }

  /** The session's {@code SOLR_OPTS}, which is appended to and which may be unset. */
  private String solrOpts() {
    String value = env("SOLR_OPTS");
    return value == null ? "" : value;
  }
}
