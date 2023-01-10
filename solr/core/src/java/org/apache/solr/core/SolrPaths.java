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

package org.apache.solr.core;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.exec.OS;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility methods about paths in Solr. */
public final class SolrPaths {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Special path which means to accept all paths. */
  public static final Path ALL_PATH = Paths.get("_ALL_");
  /** Special singleton path set containing only {@link #ALL_PATH}. */
  private static final Set<Path> ALL_PATHS = Collections.singleton(ALL_PATH);

  private SolrPaths() {} // don't create this

  /** Ensures a directory name always ends with a '/'. */
  public static String normalizeDir(String path) {
    return (path != null && (!(path.endsWith("/") || path.endsWith("\\"))))
        ? path + File.separator
        : path;
  }

  /**
   * Checks that the given path is relative to one of the allowPaths supplied. Typically this will
   * be called from {@link CoreContainer#assertPathAllowed(Path)} and allowPaths pre-filled with the
   * node's SOLR_HOME, SOLR_DATA_HOME and coreRootDirectory folders, as well as any paths specified
   * in solr.xml's allowPaths element. The following paths will always fail validation:
   *
   * <ul>
   *   <li>Relative paths starting with <code>..</code>
   *   <li>Windows UNC paths (such as <code>\\host\share\path</code>)
   *   <li>Paths which are not relative to any of allowPaths
   * </ul>
   *
   * @param pathToAssert path to check
   * @param allowPaths list of paths that should be allowed prefixes for pathToAssert
   * @throws SolrException if path is outside allowed paths
   */
  public static void assertPathAllowed(Path pathToAssert, Set<Path> allowPaths)
      throws SolrException {
    if (ALL_PATHS.equals(allowPaths)) return; // Catch-all allows all paths (*/_ALL_)
    if (pathToAssert == null) return;
    assertNotUnc(pathToAssert);
    // Conversion Path -> String -> Path is to be able to compare against
    // org.apache.lucene.mockfile.FilterPath instances
    final Path path = Path.of(pathToAssert.toString()).normalize();
    assertNoPathTraversal(path);
    if (!path.isAbsolute()) return; // All relative paths are accepted
    if (allowPaths.stream().noneMatch(p -> path.startsWith(Path.of(p.toString())))) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Path "
              + path
              + " must be relative to SOLR_HOME, SOLR_DATA_HOME coreRootDirectory. Set system property 'solr.allowPaths' to add other allowed paths.");
    }
  }

  /** Asserts that a path does not contain directory traversal */
  public static void assertNoPathTraversal(Path pathToAssert) {
    if (pathToAssert.toString().contains("..")) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Path " + pathToAssert + " disallowed due to path traversal..");
    }
  }

  /** Asserts that a path is not a Windows UNC path */
  public static void assertNotUnc(Path pathToAssert) {
    if (OS.isFamilyWindows() && pathToAssert.toString().startsWith("\\\\")) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Path " + pathToAssert + " disallowed. UNC paths not supported.");
    }
  }

  /**
   * Builds a set of allowed {@link Path}. Detects special paths "*" and "_ALL_" that mean all paths
   * are allowed.
   */
  public static class AllowPathBuilder {

    private static final String WILDCARD_PATH = "*";

    private Set<Path> paths;

    /** Adds an allowed path. Detects "*" and "_ALL_" which mean all paths are allowed. */
    public AllowPathBuilder addPath(String path) {
      if (path.equals(WILDCARD_PATH)) {
        paths = ALL_PATHS;
      } else {
        addPath(Paths.get(path));
      }
      return this;
    }

    /**
     * Adds an allowed path. Detects "_ALL_" which means all paths are allowed. Does not detect "*"
     * (not supported as a {@link Path} on Windows), see {@link #addPath(String)}.
     */
    public AllowPathBuilder addPath(Path path) {
      if (paths != ALL_PATHS) {
        if (path.equals(ALL_PATH)) {
          paths = ALL_PATHS;
        } else {
          if (paths == null) {
            paths = new HashSet<>();
          }
          paths.add(path.normalize());
        }
      }
      return this;
    }

    public Set<Path> build() {
      return paths == null ? Collections.emptySet() : paths;
    }
  }
}
