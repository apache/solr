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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Solr Standalone File System ConfigSetService impl.
 *
 * <p>
 * Loads a ConfigSet defined by the core's configSet property, looking for a directory named for
 * the configSet property value underneath a base directory. If no configSet property is set, loads
 * the ConfigSet instead from the core's instance directory.
 */
public class FileSystemConfigSetService extends ConfigSetService {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final Path configSetBase;

  public FileSystemConfigSetService(CoreContainer cc) {
    super(cc.getResourceLoader(), cc.getConfig().hasSchemaCache());
    this.configSetBase = cc.getConfig().getConfigSetBaseDirectory();
  }

  @Override
  public SolrResourceLoader createCoreResourceLoader(CoreDescriptor cd) {
    Path instanceDir = locateInstanceDir(cd);
    SolrResourceLoader solrResourceLoader = new SolrResourceLoader(instanceDir, parentLoader.getClassLoader());
    return solrResourceLoader;
  }

  @Override
  public String configSetName(CoreDescriptor cd) {
    return (cd.getConfigSet() == null ? "instancedir " : "configset ") + locateInstanceDir(cd);
  }

  @Override
  public boolean checkConfigExists(String configName) throws IOException {
    Path solrConfigXmlFile= configSetBase.resolve(configName).resolve("solrconfig.xml");
    return Files.exists(solrConfigXmlFile);
  }

  @Override
  public void deleteConfig(String configName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteFilesFromConfig(String configName, List<String> filesToDelete) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void copyConfig(String fromConfig, String toConfig) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void uploadConfig(String configName, Path dir) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void uploadFileToConfig(String configName, String fileName, byte[] data, boolean overwriteOnExists) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setConfigMetadata(String configName, Map<String, Object> data) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Object> getConfigMetadata(String configName) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void downloadConfig(String configName, Path dir) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listConfigs() throws IOException {
    try (Stream<Path> configs = Files.list(configSetBase)) {
      return configs.map(Path::getFileName)
              .map(Path::toString)
              .collect(Collectors.toList());
    }
  }

  @Override
  public byte[] downloadFileFromConfig(String configName, String filePath) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getAllConfigFiles(String configName) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected Path locateInstanceDir(CoreDescriptor cd) {
    String configSet = cd.getConfigSet();
    if (configSet == null) return cd.getInstanceDir();
    Path configSetDirectory = configSetBase.resolve(configSet);
    if (!Files.isDirectory(configSetDirectory))
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Could not load configuration from directory " + configSetDirectory);
    return configSetDirectory;
  }

  @Override
  protected Long getCurrentSchemaModificationVersion(String configSet, SolrConfig solrConfig, String schemaFileName) throws IOException {
    Path schemaFile = solrConfig.getResourceLoader().getConfigPath().resolve(schemaFileName);
    try {
      return Files.getLastModifiedTime(schemaFile).toMillis();
    } catch (FileNotFoundException e) {
      return null; // acceptable
    } catch (IOException e) {
      log.warn("Unexpected exception when getting modification time of {}", schemaFile, e);
      return null; // debatable; we'll see an error soon if there's a real problem
    }
  }
}
