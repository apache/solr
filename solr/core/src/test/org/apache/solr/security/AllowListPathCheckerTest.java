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
package org.apache.solr.security;

import java.nio.file.Path;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.NodeConfig;
import org.junit.Test;

/** Tests {@link AllowListPathChecker}. */
public class AllowListPathCheckerTest extends SolrTestCase {

  @Test
  public void testCreateFromNodeConfig() {
    Path home = createTempDir("home");
    Path cores = createTempDir("cores");
    Path configSets = createTempDir("configsets");
    Path dataHome = createTempDir("data");
    Path extra = createTempDir("extra");
    NodeConfig config =
        new NodeConfig.NodeConfigBuilder("testNode", home)
            .setCoreRootDirectory(cores.toString())
            .setConfigSetBaseDirectory(configSets.toString())
            .setSolrDataHome(dataHome.toString())
            .setAllowPaths(Set.of(extra))
            .build();

    AllowListPathChecker checker = AllowListPathChecker.create(config);

    for (Path allowed : new Path[] {home, cores, configSets, dataHome, extra}) {
      checker.assertPathAllowed(allowed.resolve("sub"));
    }
    checker.assertPathAllowed(Path.of("relative"));
    checker.assertPathAllowed(null);

    Path outside = createTempDir("outside");
    expectThrows(SolrException.class, () -> checker.assertPathAllowed(outside));
    expectThrows(SolrException.class, () -> checker.assertPathAllowed(home.resolve("../outside")));
  }

  @Test
  public void testInstancesAreIndependent() {
    Path home1 = createTempDir("home1");
    Path home2 = createTempDir("home2");
    AllowListPathChecker checker1 =
        AllowListPathChecker.create(new NodeConfig.NodeConfigBuilder("node1", home1).build());
    AllowListPathChecker checker2 =
        AllowListPathChecker.create(new NodeConfig.NodeConfigBuilder("node2", home2).build());

    checker1.assertPathAllowed(home1);
    checker2.assertPathAllowed(home2);
    expectThrows(SolrException.class, () -> checker1.assertPathAllowed(home2));
    expectThrows(SolrException.class, () -> checker2.assertPathAllowed(home1));
  }

  @Test
  public void testAllowAll() {
    AllowListPathChecker.ALLOW_ALL.assertPathAllowed(createTempDir("anywhere"));

    Path extra = createTempDir("extra");
    NodeConfig config =
        new NodeConfig.NodeConfigBuilder("testNode", createTempDir("home"))
            .setAllowPaths(Set.of(Path.of("_ALL_")))
            .build();
    AllowListPathChecker.create(config).assertPathAllowed(extra);
  }
}
