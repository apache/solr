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

  Path credentialsFile() {
    return home.resolve("zk-credentials.properties");
  }

  boolean isConfigured() {
    return Files.isRegularFile(credentialsFile());
  }

  void create(String adminPassword, String readonlyPassword) throws IOException {
    if (Files.exists(home.resolve("zoo_data"))) {
      throw new IllegalStateException(
          "This example has existing ZooKeeper data without saved ACL credentials. "
              + "Use a fresh Solr home for the secured example, or migrate its ACLs while Solr is stopped "
              + "as described in the ZooKeeper Access Control reference guide.");
    }
    Properties credentials = new Properties();
    credentials.setProperty("zkDigestUsername", "zk-admin");
    credentials.setProperty("zkDigestPassword", adminPassword);
    credentials.setProperty("zkDigestReadonlyUsername", "zk-readonly");
    credentials.setProperty("zkDigestReadonlyPassword", readonlyPassword);
    StringWriter output = new StringWriter();
    credentials.store(output, "ZooKeeper credentials for the secure Solr example");
    IncludeFileEditor.write(credentialsFile(), output.toString().getBytes(StandardCharsets.UTF_8));
  }

  void configure(Path includeFile, int solrPort) throws IOException {
    if (solrPort > 64535) {
      throw new IllegalArgumentException(
          "The embedded ZooKeeper requires a Solr port at or below 64535.");
    }
    Properties credentials =
        VMParamsZkCredentialsInjector.readCredentialsFile(credentialsFile().toString());
    for (String key :
        new String[] {
          "zkDigestUsername",
          "zkDigestPassword",
          "zkDigestReadonlyUsername",
          "zkDigestReadonlyPassword"
        }) {
      if (credentials.getProperty(key, "").isEmpty()) {
        throw new IllegalStateException("Missing " + key + " in " + credentialsFile());
      }
    }
    Map<String, String> properties =
        Map.of(
            "zkCredentialsInjector", VMParamsZkCredentialsInjector.class.getName(),
            "zkCredentialsProvider", DigestZkCredentialsProvider.class.getName(),
            "zkACLProvider", DigestZkACLProvider.class.getName(),
            "zkDigestCredentialsFile", credentialsFile().toString());
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
