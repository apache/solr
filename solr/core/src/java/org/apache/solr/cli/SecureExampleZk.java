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
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.solr.common.cloud.DigestZkACLProvider;
import org.apache.solr.common.cloud.DigestZkCredentialsProvider;
import org.apache.solr.common.cloud.VMParamsZkCredentialsInjector;

/** Manages the digest credentials and ACL configuration for the secure example's ZooKeeper. */
final class SecureExampleZk {
  private static final String SERVER_OPTIONS =
      "SOLR_OPTS=\"${SOLR_OPTS:-} $SOLR_ZK_CREDS_AND_ACLS -Dsolr.zookeeper.server.enabled=${SOLR_EXAMPLE_EMBEDDED_ZK:-true} -Dsolr.example.zk.host=$SOLR_EXAMPLE_ZK_HOST\"";
  private final Path home;

  SecureExampleZk(Path home) {
    this.home = home.toAbsolutePath();
  }

  void assertFreshZkData() {
    if (Files.exists(home.resolve("zoo_data"))) {
      throw new IllegalStateException(
          "This example has existing ZooKeeper data without saved ACL credentials. "
              + "Use a fresh Solr home for the secured example, or migrate its ACLs while Solr is stopped "
              + "as described in the ZooKeeper Access Control reference guide.");
    }
  }

  private void configureServerTls(Path tls, int solrPort) throws IOException {
    Path config = home.resolve("zoo.cfg");
    Properties settings = new Properties();
    try (var reader = Files.newBufferedReader(config, StandardCharsets.UTF_8)) {
      settings.load(reader);
    }
    settings.remove("clientPort");
    settings.remove("clientPortAddress");
    settings.setProperty("secureClientPort", Integer.toString(solrPort + 1000));
    settings.setProperty("serverCnxnFactory", "org.apache.zookeeper.server.NettyServerCnxnFactory");
    settings.setProperty("ssl.keyStore.location", tls.resolve("solr-ssl.keystore.p12").toString());
    settings.setProperty("ssl.keyStore.type", "PKCS12");
    settings.setProperty(
        "ssl.keyStore.passwordPath", tls.resolve("keystore-password.txt").toString());
    settings.setProperty(
        "ssl.trustStore.location", tls.resolve("solr-ssl.keystore.p12.truststore").toString());
    settings.setProperty("ssl.trustStore.type", "PKCS12");
    settings.setProperty(
        "ssl.trustStore.passwordPath", tls.resolve("keystore-password.txt").toString());
    settings.setProperty("ssl.clientAuth", "none");
    settings.setProperty("client.portUnification", "false");
    StringWriter output = new StringWriter();
    settings.store(output, "TLS-only embedded ZooKeeper for the secure Solr example");
    IncludeFileEditor.write(config, output.toString().getBytes(StandardCharsets.UTF_8));
  }

  void configure(Path credentialsFile, Path includeFile, int solrPort) throws IOException {
    if (solrPort > 64535) {
      throw new IllegalArgumentException(
          "The embedded ZooKeeper requires a Solr port at or below 64535.");
    }
    Properties credentials =
        VMParamsZkCredentialsInjector.readCredentialsFile(credentialsFile.toString());
    for (String key :
        new String[] {
          "zkDigestUsername",
          "zkDigestPassword",
          "zkDigestReadonlyUsername",
          "zkDigestReadonlyPassword"
        }) {
      if (credentials.getProperty(key, "").isEmpty()) {
        throw new IllegalStateException("Missing " + key + " in " + credentialsFile);
      }
    }
    Path tls = home.resolve("tls");
    Map<String, String> properties = new HashMap<>();
    properties.put("zkCredentialsInjector", VMParamsZkCredentialsInjector.class.getName());
    properties.put("zkCredentialsProvider", DigestZkCredentialsProvider.class.getName());
    properties.put("zkACLProvider", DigestZkACLProvider.class.getName());
    properties.put("zkDigestCredentialsFile", credentialsFile.toString());
    properties.put("zookeeper.client.secure", "true");
    properties.put("zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty");
    properties.put(
        "zookeeper.ssl.trustStore.location",
        tls.resolve("solr-ssl.keystore.p12.truststore").toString());
    properties.put("zookeeper.ssl.trustStore.type", "PKCS12");
    properties.put(
        "zookeeper.ssl.trustStore.passwordPath", tls.resolve("keystore-password.txt").toString());
    properties.put("zookeeper.ssl.hostnameVerification", "true");
    configureServerTls(tls, solrPort);
    properties.forEach(System::setProperty);
    StringBuilder options = new StringBuilder();
    properties.forEach((key, value) -> options.append(" -D").append(key).append('=').append(value));
    Map<String, String> settings =
        Map.of(
            "SOLR_ZK_CREDS_AND_ACLS",
            options.toString().trim(),
            "SOLR_EXAMPLE_ZK_HOST",
            "localhost:" + (solrPort + 1000) + "/solr",
            "ZK_CREATE_CHROOT",
            "true");
    Path solrXml = home.resolve("solr.xml");
    String xml = Files.readString(solrXml);
    String zkHost = "<str name=\"zkHost\">${solr.example.zk.host:}</str>";
    if (!xml.contains(zkHost)) {
      if (xml.contains("name=\"zkHost\"")) {
        throw new IllegalStateException(
            "The secure example requires its own ZooKeeper host setting in " + solrXml);
      }
      Files.writeString(solrXml, xml.replace("<solrcloud>", "<solrcloud>\n    " + zkHost));
    }
    String original = Files.readString(includeFile);
    String updated =
        IncludeFileEditor.update(
            original.replace(SERVER_OPTIONS + "\n", ""),
            settings,
            settings.keySet(),
            IncludeFileEditor.Format.SHELL);
    IncludeFileEditor.write(
        includeFile, (updated + SERVER_OPTIONS + "\n").getBytes(StandardCharsets.UTF_8));
  }
}
