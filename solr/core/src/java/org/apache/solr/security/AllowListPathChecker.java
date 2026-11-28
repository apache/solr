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

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Set;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates file system paths against a node's allow-list: SOLR_HOME, SOLR_DATA_HOME,
 * coreRootDirectory, configSetBaseDirectory and the {@code allowPaths} configuration of solr.xml.
 *
 * <p>Use this rather than a {@link org.apache.solr.core.CoreContainer} reference when code needs to
 * validate paths but has no container at hand. Instances are per node, not global, as several nodes
 * with different homes may share a JVM.
 */
public class AllowListPathChecker {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Singleton checker which allows all paths. */
  public static final AllowListPathChecker ALLOW_ALL =
      new AllowListPathChecker(Set.of(SolrPaths.ALL_PATH));

  private final Set<Path> allowPaths;

  /**
   * @param allowPaths allowed path prefixes, typically built with {@code
   *     SolrPaths.AllowPathBuilder}. The set is used as is, not copied.
   */
  public AllowListPathChecker(Set<Path> allowPaths) {
    this.allowPaths = allowPaths;
  }

  /** Creates a checker for the node's home directories and the configured {@code allowPaths}. */
  public static AllowListPathChecker create(NodeConfig config) {
    SolrPaths.AllowPathBuilder builder = new SolrPaths.AllowPathBuilder();
    builder.addPath(config.getSolrHome());
    builder.addPath(config.getCoreRootDirectory());
    builder.addPath(config.getConfigSetBaseDirectory());
    builder.addPath(config.getSolrDataHome());
    if (!config.getAllowPaths().isEmpty()) {
      config.getAllowPaths().forEach(builder::addPath);
      if (log.isInfoEnabled()) {
        log.info("Allowing use of paths: {}", config.getAllowPaths());
      }
    }
    return new AllowListPathChecker(builder.build());
  }

  /**
   * Checks that the given path is within the allow-list. See {@link
   * SolrPaths#assertPathAllowed(Path, Set)} for the rules.
   *
   * @param pathToAssert path to check, may be null
   * @throws SolrException if path is outside allowed paths
   */
  public void assertPathAllowed(Path pathToAssert) throws SolrException {
    SolrPaths.assertPathAllowed(pathToAssert, allowPaths);
  }

  /** The allowed path prefixes. This is the live set; tests modify it directly. */
  public Set<Path> getAllowPaths() {
    return allowPaths;
  }

  @Override
  public String toString() {
    return "AllowListPathChecker{allowPaths=" + allowPaths + "}";
  }
}
