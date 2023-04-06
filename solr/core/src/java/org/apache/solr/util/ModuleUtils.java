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
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.solr.common.util.StrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses the list of modules the user has requested in solr.xml, property solr.modules or
 * environment SOLR_MODULES. Then resolves the lib folder for each, so they can be added to class
 * path.
 */
public class ModuleUtils {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String MODULES_FOLDER_NAME = "modules";
  private static final Pattern validModNamesPattern = Pattern.compile("[\\w\\d-_]+");

  /**
   * Returns a path to a module's lib folder
   *
   * @param moduleName name of module
   * @return the path to the module's lib folder
   */
  public static Path getModuleLibPath(Path solrInstallDirPath, String moduleName) {
    return getModulesPath(solrInstallDirPath).resolve(moduleName).resolve("lib");
  }

  /**
   * Finds list of module names requested by system property or environment variable
   *
   * @return set of raw volume names from sysprop and/or env.var
   */
  static Set<String> resolveFromSyspropOrEnv() {
    // Fall back to sysprop and env.var if nothing configured through solr.xml
    Set<String> mods = new HashSet<>();
    String modulesFromProps = System.getProperty("solr.modules");
    if (StrUtils.isNotNullOrEmpty(modulesFromProps)) {
      mods.addAll(StrUtils.splitSmart(modulesFromProps, ',', true));
    }
    String modulesFromEnv = System.getenv("SOLR_MODULES");
    if (StrUtils.isNotNullOrEmpty(modulesFromEnv)) {
      mods.addAll(StrUtils.splitSmart(modulesFromEnv, ',', true));
    }
    return mods.stream().map(String::trim).collect(Collectors.toSet());
  }

  /** Returns true if a module name is valid and exists in the system */
  public static boolean moduleExists(Path solrInstallDirPath, String moduleName) {
    if (!isValidName(moduleName)) return false;
    Path modPath = getModulesPath(solrInstallDirPath).resolve(moduleName);
    return Files.isDirectory(modPath);
  }

  /** Returns nam of all existing modules */
  public static Set<String> listAvailableModules(Path solrInstallDirPath) {
    try (var moduleFilesStream = Files.list(getModulesPath(solrInstallDirPath))) {
      return moduleFilesStream
          .filter(Files::isDirectory)
          .map(p -> p.getFileName().toString())
          .collect(Collectors.toSet());
    } catch (IOException e) {
      log.warn("Found no modules in {}", getModulesPath(solrInstallDirPath), e);
      return Collections.emptySet();
    }
  }

  /**
   * Parses comma separated string of module names, in practice found in solr.xml. If input string
   * is empty (nothing configured) or null (e.g. tag not present in solr.xml), we continue to
   * resolve from system property <code>-Dsolr.modules</code> and if still empty, fall back to
   * environment variable <code>SOLR_MODULES</code>.
   *
   * @param modulesFromString raw string of comma-separated module names
   * @return a set of module
   */
  public static Collection<String> resolveModulesFromStringOrSyspropOrEnv(
      String modulesFromString) {
    Collection<String> moduleNames;
    if (modulesFromString != null && !modulesFromString.isBlank()) {
      moduleNames = StrUtils.splitSmart(modulesFromString, ',', true);
    } else {
      // If nothing configured in solr.xml, check sysprop and environment
      moduleNames = resolveFromSyspropOrEnv();
    }
    return moduleNames.stream().map(String::trim).collect(Collectors.toSet());
  }

  /** Returns true if module name is valid */
  public static boolean isValidName(String moduleName) {
    return validModNamesPattern.matcher(moduleName).matches();
  }

  /** Returns path for modules directory, given the solr install dir path */
  public static Path getModulesPath(Path solrInstallDirPath) {
    return solrInstallDirPath.resolve(MODULES_FOLDER_NAME);
  }
}
