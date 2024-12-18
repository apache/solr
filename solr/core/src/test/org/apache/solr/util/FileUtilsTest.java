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
package org.apache.solr.util;

import java.io.File;
import java.nio.file.Path;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

public class FileUtilsTest extends SolrTestCase {

  @Test
  public void testDetectsPathEscape() {
    final var parent = Path.of(".");

    // Allows simple child
    assertTrue(FileUtils.isPathAChildOfParent(parent, parent.resolve("child")));

    // Allows "./" prefixed child
    assertTrue(FileUtils.isPathAChildOfParent(parent, parent.resolve(buildPath(".", "child"))));

    // Allows nested child
    assertTrue(
        FileUtils.isPathAChildOfParent(parent, parent.resolve(buildPath("nested", "child"))));

    // Allows backtracking, provided it stays "under" parent
    assertTrue(
        FileUtils.isPathAChildOfParent(
            parent, parent.resolve(buildPath("child1", "..", "child2"))));
    assertTrue(
        FileUtils.isPathAChildOfParent(
            parent, parent.resolve(buildPath("child", "grandchild1", "..", "grandchild2"))));

    // Prevents identical path
    assertFalse(FileUtils.isPathAChildOfParent(parent, parent));

    // Detects sibling of parent
    assertFalse(FileUtils.isPathAChildOfParent(parent, parent.resolve(buildPath("..", "sibling"))));

    // Detects "grandparent" of parent
    assertFalse(FileUtils.isPathAChildOfParent(parent, parent.resolve("..")));

    // Detects many-layered backtracking
    assertFalse(
        FileUtils.isPathAChildOfParent(parent, parent.resolve(buildPath("..", "..", "..", ".."))));
  }

  private static String buildPath(String... pathSegments) {
    final var sb = new StringBuilder();
    for (int i = 0; i < pathSegments.length; i++) {
      sb.append(pathSegments[i]);
      if (i < pathSegments.length - 1) {
        sb.append(File.separator);
      }
    }
    return sb.toString();
  }
}
