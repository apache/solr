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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

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
  /** .metadata.json hidden file where metadata is stored */
  public static final String METADATA_FILE = ".metadata.json";

  public FileSystemConfigSetService(CoreContainer cc) {
    super(cc.getResourceLoader(), cc.getConfig().hasSchemaCache());
    this.configSetBase = cc.getConfig().getConfigSetBaseDirectory();
  }

  /** This is for testing purpose */
  public FileSystemConfigSetService(Path configSetBase) {
    super(null, false);
    this.configSetBase = configSetBase;
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
    Path configDir = configSetBase.resolve(configName);
    return Files.exists(configDir);
  }

  @Override
  public void deleteConfig(String configName) throws IOException {
    Path configDir = configSetBase.resolve(configName);
    deleteDir(configDir);
  }

  @Override
  public void deleteFilesFromConfig(String configName, List<String> filesToDelete) throws IOException {
    Path configDir = configSetBase.resolve(configName);
    Objects.requireNonNull(filesToDelete);
    for (String fileName : filesToDelete) {
      Path file = configDir.resolve(fileName);
      if (Files.exists(file)) {
        if (Files.isDirectory(file)) {
          deleteDir(file);
        } else {
          Files.delete(file);
        }
      }
    }
  }

  public void copyConfig(String fromConfig, String toConfig) throws IOException {
    Path source = configSetBase.resolve(fromConfig);
    Path dest = configSetBase.resolve(toConfig);
    copyRecursively(source, dest);
  }

  private void deleteDir(Path dir) throws IOException {
    try {
      Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
          Files.delete(path);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException ioException) throws IOException {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (NoSuchFileException e) {
      // do nothing
    }
  }

  @Override
  public void uploadConfig(String configName, Path source) throws IOException {
    Path dest = configSetBase.resolve(configName);
    copyRecursively(source, dest);
  }

  @Override
  public void uploadFileToConfig(String configName, String fileName, byte[] data, boolean overwriteOnExists) throws IOException {
    Path filePath = configSetBase.resolve(configName).resolve(fileName);
    if (!Files.exists(filePath) || overwriteOnExists) {
      Files.write(filePath, data);
    }
  }

  @Override
  public void setConfigMetadata(String configName, Map<String, Object> data) throws IOException {
    // store metadata in .metadata.json file
    Path metadataPath = configSetBase.resolve(configName).resolve(METADATA_FILE);
    Files.write(metadataPath, Utils.toJSON(data));
    Files.isHidden(metadataPath);
  }

  @Override
  public Map<String, Object> getConfigMetadata(String configName) throws IOException {
    // get metadata from .metadata.json file
    Path metadataPath = configSetBase.resolve(configName).resolve(METADATA_FILE);
    byte[] data = null;
    try {
      data = Files.readAllBytes(metadataPath);
    } catch (NoSuchFileException e) {
      return new HashMap<>();
    }
    if (data == null) {
      return new HashMap<>();
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> metadata = (Map<String, Object>) Utils.fromJSON(data);
    return metadata;
  }

  @Override
  public void downloadConfig(String configName, Path dest) throws IOException {
    Path source = configSetBase.resolve(configName);
    copyRecursively(source, dest);
  }

  private void copyRecursively(Path source, Path target) throws IOException {
    try {
      Files.walkFileTree(source, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
          Files.createDirectories(target.resolve(source.relativize(dir)));
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.copy(file, target.resolve(source.relativize(file)), REPLACE_EXISTING);
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (NoSuchFileException e) {
      // do nothing
    }
  }

  @Override
  public List<String> listConfigs() throws IOException {
    try (Stream<Path> configs = Files.list(configSetBase)) {
      return configs.map(Path::getFileName)
          .map(Path::toString)
          .sorted()
          .collect(Collectors.toList());
    }
  }

  @Override
  public byte[] downloadFileFromConfig(String configName, String fileName) throws IOException {
    Path filePath = configSetBase.resolve(configName).resolve(fileName);
    byte[] data = null;
    try {
      data = Files.readAllBytes(filePath);
    } catch (NoSuchFileException e) {
      // do nothing
    }
    return data;
  }

  @Override
  public List<String> getAllConfigFiles(String configName) throws IOException {
    Path configDir = configSetBase.resolve(configName);
    List<String> filePaths = new ArrayList<>();
    Files.walkFileTree(configDir, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        // don't include hidden (.) files
        if (!Files.isHidden(file)) {
          filePaths.add(configDir.relativize(file).toString());
          return FileVisitResult.CONTINUE;
        }
        return FileVisitResult.CONTINUE;
      }
      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException ioException) throws IOException {
        if (!dir.getFileName().toString().equals(configName)) {
          filePaths.add(configDir.relativize(dir).toString() + "/");
        }
        return FileVisitResult.CONTINUE;
      }
    });
    Collections.sort(filePaths);
    return filePaths;
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
  protected Long getCurrentSchemaModificationVersion(String configSet, SolrConfig solrConfig, String schemaFileName) {
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

  public Path getConfigDir(String configName) {
    return configSetBase.resolve(configName);
  }
}
