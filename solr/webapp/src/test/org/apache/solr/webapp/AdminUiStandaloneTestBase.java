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
package org.apache.solr.webapp;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;

/**
 * Base class for Admin UI tests of a standalone (user-managed, no ZooKeeper) Solr node, whose UI
 * differs from cloud mode: no Cloud/Collections/Schema Designer menus, and the per-core menu offers
 * analysis, documents, query, replication etc. directly.
 *
 * <p>The base {@code @BeforeClass} only starts the browser (via {@link #standaloneMode}); this
 * class builds a solr home and starts a standalone {@link JettySolrRunner} serving the UI.
 */
public abstract class AdminUiStandaloneTestBase extends AdminUiTestBase {

  /**
   * Arms standalone mode before the cloud cluster can start lazily. Runs after the base class's
   * browser-starting {@code @BeforeClass} and before any subclass {@code @BeforeClass} or test.
   */
  @BeforeClass
  public static void enableStandaloneMode() {
    standaloneMode = true;
  }

  /** Builds a solr home with the shared test solr.xml and the given pre-created cores. */
  protected static Path buildStandaloneHome(String... coreNames) throws IOException {
    Path home = createTempDir("standalone-home");
    Files.copy(
        ExternalPaths.SOURCE_HOME.resolve("core/src/test-files/solr/solr.xml"),
        home.resolve("solr.xml"));
    for (String coreName : coreNames) {
      createStandaloneCoreDir(home, coreName);
      Properties props = new Properties();
      props.setProperty("name", coreName);
      writeCoreProperties(home.resolve(coreName), props, coreName);
    }
    return home;
  }

  /** Creates a core instance dir with the default configset, without registering the core. */
  protected static void createStandaloneCoreDir(Path home, String coreName) throws IOException {
    Path confDir = home.resolve(coreName).resolve("conf");
    Files.createDirectories(confDir);
    copyDirectory(ExternalPaths.DEFAULT_CONFIGSET, confDir);
  }

  /** Starts a standalone Jetty on the given home, serving the Admin UI. */
  protected static JettySolrRunner startStandaloneJetty(Path home) throws Exception {
    JettyConfig.Builder config = JettyConfig.builder();
    configureJettyForUi(config);
    JettySolrRunner jetty = new JettySolrRunner(home.toString(), new Properties(), config.build());
    jetty.start();
    return jetty;
  }

  protected static void copyDirectory(Path source, Path target) throws IOException {
    try (var paths = Files.walk(source)) {
      for (Path path : (Iterable<Path>) paths::iterator) {
        Path dest = target.resolve(source.relativize(path).toString());
        if (Files.isDirectory(path)) {
          Files.createDirectories(dest);
        } else {
          Files.createDirectories(dest.getParent());
          Files.copy(path, dest);
        }
      }
    }
  }
}
