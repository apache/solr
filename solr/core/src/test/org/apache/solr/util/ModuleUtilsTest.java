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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import org.apache.solr.SolrTestCase;

public class ModuleUtilsTest extends SolrTestCase {
  private Path mockRootDir;
  private final Set<String> expectedMods = Set.of("mod1", "mod2");

  @Override
  public void setUp() throws Exception {
    super.setUp();
    mockRootDir = setupMockInstallDir(expectedMods);
  }

  public void testModuleExists() {
    assertTrue(ModuleUtils.moduleExists(mockRootDir, "mod1"));
    assertFalse(ModuleUtils.moduleExists(mockRootDir, "mod3"));
  }

  public void testIsValidName() {
    assertTrue(ModuleUtils.isValidName("mod1-foo_bar-123"));
    assertFalse(ModuleUtils.moduleExists(mockRootDir, "not valid"));
    assertFalse(ModuleUtils.moduleExists(mockRootDir, "not/valid"));
    assertFalse(ModuleUtils.moduleExists(mockRootDir, "not>valid"));
  }

  public void testGetModuleLibPath() {
    assertEquals(
        mockRootDir.resolve("modules").resolve("mod1").resolve("lib"),
        ModuleUtils.getModuleLibPath(mockRootDir, "mod1"));
  }

  public void testResolveFromSyspropOrEnv() {
    assertEquals(Collections.emptySet(), ModuleUtils.resolveFromSyspropOrEnv());
    System.setProperty("solr.modules", "foo ,bar, baz,mod1");
    assertEquals(Set.of("foo", "bar", "baz", "mod1"), ModuleUtils.resolveFromSyspropOrEnv());
    System.clearProperty("solr.modules");
  }

  public void testListAvailableModules() {
    assertEquals(expectedMods, ModuleUtils.listAvailableModules(mockRootDir));
  }

  public void testResolveModules() {
    assertEquals(
        Set.of("foo", "bar", "baz", "mod1"),
        Set.copyOf(ModuleUtils.resolveModulesFromStringOrSyspropOrEnv("foo ,bar, baz,mod1")));
    assertEquals(
        Collections.emptySet(), Set.copyOf(ModuleUtils.resolveModulesFromStringOrSyspropOrEnv("")));
    System.setProperty("solr.modules", "foo ,bar, baz,mod1");
    assertEquals(
        Set.of("foo", "bar", "baz", "mod1"),
        Set.copyOf(ModuleUtils.resolveModulesFromStringOrSyspropOrEnv(null)));
    System.clearProperty("solr.modules");
  }

  private Path setupMockInstallDir(Set<String> modules) throws IOException {
    Path root = Files.createTempDirectory("moduleUtilsTest");
    Path modPath = root.resolve("modules");
    Files.createDirectories(modPath);
    for (var m : modules) {
      Path libPath = modPath.resolve(m).resolve("lib");
      Files.createDirectories(libPath);
      Files.createFile(libPath.resolve("jar1.jar"));
      Files.createFile(libPath.resolve("jar2.jar"));
      Files.createFile(libPath.resolve("README.md"));
    }
    return root;
  }
}
