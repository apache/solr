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
import java.util.List;
import java.util.Properties;

/** Persists HTTP credentials so the secure example can restart without password prompts. */
final class SecureExampleCredentials {
  private final Path file;
  private final Properties credentials = new Properties();

  SecureExampleCredentials(Path home) throws IOException {
    file = home.toAbsolutePath().resolve("http-credentials.properties");
    if (exists()) {
      try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
        credentials.load(reader);
      }
      for (String key :
          List.of("admin.username", "admin.password", "index.password", "search.password")) {
        if (credentials.getProperty(key, "").isEmpty()) {
          throw new IllegalStateException("Missing " + key + " in " + file);
        }
      }
    } else {
      credentials.setProperty("admin.username", "solr");
      for (String account : List.of("admin", "index", "search")) setPassword(account, "SolrRocks");
    }
  }

  boolean exists() {
    return Files.isRegularFile(file);
  }

  String adminUsername() {
    return credentials.getProperty("admin.username");
  }

  String password(String account) {
    return credentials.getProperty(account + ".password");
  }

  void setPassword(String account, String password) {
    credentials.setProperty(account + ".password", password);
  }

  void setAdministrator(String value) {
    int colon = value.indexOf(':');
    if (colon <= 0 || colon == value.length() - 1) {
      throw new IllegalArgumentException(
          "Administrator credentials must have the form username:password.");
    }
    String username = value.substring(0, colon);
    if (List.of("index", "search").contains(username)) {
      throw new IllegalArgumentException(
          "The administrator username must differ from index and search.");
    }
    credentials.setProperty("admin.username", username);
    setPassword("admin", value.substring(colon + 1));
  }

  Path file() {
    return file;
  }

  void save() throws IOException {
    StringWriter output = new StringWriter();
    credentials.store(output, "HTTP credentials for the secure Solr example");
    IncludeFileEditor.write(file, output.toString().getBytes(StandardCharsets.UTF_8));
  }
}
