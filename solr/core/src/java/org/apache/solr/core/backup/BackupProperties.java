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

package org.apache.solr.core.backup;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Version;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.util.PropertiesInputStream;

/**
 * Represents a backup[-*].properties file, responsible for holding whole-collection and
 * whole-backup metadata.
 *
 * <p>These files live in a different location and hold different metadata depending on the backup
 * format used. The (now deprecated) traditional 'full-snapshot' backup format places this file at
 * $LOCATION/$NAME, while the preferred incremental backup format stores these files in
 * $LOCATION/$NAME/$COLLECTION.
 */
public class BackupProperties {

  private static final String EXTRA_PROPERTY_PREFIX = "property.";

  private double indexSizeMB;
  private int indexFileCount;

  private final Properties properties;

  private final Map<String, String> extraProperties;

  private BackupProperties(Properties properties, Map<String, String> extraProperties) {
    this.properties = properties;
    if (extraProperties == null) {
      extraProperties = Map.of();
    } else if (extraProperties.keySet().stream().anyMatch(String::isEmpty)) {
      throw new IllegalArgumentException("Can't have an extra property with an empty key");
    }
    this.extraProperties = extraProperties;
  }

  public static BackupProperties create(
      String backupName,
      String collectionName,
      String extCollectionName,
      String configName,
      Map<String, String> extraProperties) {
    final Properties properties = new Properties();

    properties.put(BackupManager.BACKUP_NAME_PROP, backupName);
    properties.put(BackupManager.COLLECTION_NAME_PROP, collectionName);
    properties.put(BackupManager.COLLECTION_ALIAS_PROP, extCollectionName);
    properties.put(CollectionAdminParams.COLL_CONF, configName);
    properties.put(BackupManager.START_TIME_PROP, Instant.now().toString());
    properties.put(BackupManager.INDEX_VERSION_PROP, Version.LATEST.toString());

    return new BackupProperties(properties, extraProperties);
  }

  public static Optional<BackupProperties> readFromLatest(
      BackupRepository repository, URI backupPath) throws IOException {
    Optional<BackupId> lastBackupId =
        BackupFilePaths.findMostRecentBackupIdFromFileListing(
            repository.listAllOrEmpty(backupPath));

    if (!lastBackupId.isPresent()) {
      return Optional.empty();
    }

    return Optional.of(
        readFrom(repository, backupPath, BackupFilePaths.getBackupPropsName(lastBackupId.get())));
  }

  public static BackupProperties readFrom(
      BackupRepository repository, URI backupPath, String fileName) throws IOException {
    Properties props = new Properties();
    try (Reader is =
        new InputStreamReader(
            new PropertiesInputStream(
                repository.openInput(backupPath, fileName, IOContext.DEFAULT)),
            StandardCharsets.UTF_8)) {
      props.load(is);
      Map<String, String> extraProperties = extractExtraProperties(props);
      return new BackupProperties(props, extraProperties);
    }
  }

  private static Map<String, String> extractExtraProperties(Properties props) {
    Map<String, String> extraProperties = new HashMap<>();
    props
        .entrySet()
        .removeIf(
            e -> {
              String entryKey = e.getKey().toString();
              if (entryKey.startsWith(EXTRA_PROPERTY_PREFIX)) {
                extraProperties.put(
                    entryKey.substring(EXTRA_PROPERTY_PREFIX.length()),
                    String.valueOf(e.getValue()));
                return true;
              }
              return false;
            });
    return extraProperties;
  }

  public List<String> getAllShardBackupMetadataFiles() {
    return properties.entrySet().stream()
        .filter(entry -> entry.getKey().toString().endsWith(".md"))
        .map(entry -> entry.getValue().toString())
        .collect(Collectors.toList());
  }

  public void countIndexFiles(int numFiles, double sizeMB) {
    indexSizeMB += sizeMB;
    indexFileCount += numFiles;
  }

  public Optional<ShardBackupId> getShardBackupIdFor(String shardName) {
    String key = getKeyForShardBackupId(shardName);
    if (properties.containsKey(key)) {
      return Optional.of(ShardBackupId.fromShardMetadataFilename(properties.getProperty(key)));
    }
    return Optional.empty();
  }

  public ShardBackupId putAndGetShardBackupIdFor(String shardName, int backupId) {
    final ShardBackupId shardBackupId = new ShardBackupId(shardName, new BackupId(backupId));
    properties.put(getKeyForShardBackupId(shardName), shardBackupId.getBackupMetadataFilename());
    return shardBackupId;
  }

  private String getKeyForShardBackupId(String shardName) {
    return shardName + ".md";
  }

  public void store(Writer propsWriter) throws IOException {
    Properties propertiesCopy = (Properties) properties.clone();
    propertiesCopy.put("indexSizeMB", String.valueOf(indexSizeMB));
    propertiesCopy.put("indexFileCount", String.valueOf(indexFileCount));
    propertiesCopy.put(BackupManager.END_TIME_PROP, Instant.now().toString());
    if (extraProperties != null && !extraProperties.isEmpty()) {
      extraProperties.forEach((k, v) -> propertiesCopy.put(EXTRA_PROPERTY_PREFIX + k, v));
    }
    propertiesCopy.store(propsWriter, "Backup properties file");
  }

  public String getCollection() {
    return properties.getProperty(BackupManager.COLLECTION_NAME_PROP);
  }

  public String getCollectionAlias() {
    return properties.getProperty(BackupManager.COLLECTION_ALIAS_PROP);
  }

  public String getConfigName() {
    return properties.getProperty(CollectionAdminParams.COLL_CONF);
  }

  public String getStartTime() {
    return properties.getProperty(BackupManager.START_TIME_PROP);
  }

  public String getEndTime() {
    return properties.getProperty(BackupManager.END_TIME_PROP);
  }

  public String getIndexVersion() {
    return properties.getProperty(BackupManager.INDEX_VERSION_PROP);
  }

  public Map<String, String> getExtraProperties() {
    return extraProperties;
  }

  public Map<String, Object> getDetails() {
    final Map<String, Object> result = new HashMap<>();
    properties.entrySet().forEach(entry -> result.put(entry.getKey().toString(), entry.getValue()));
    result.remove(BackupManager.BACKUP_NAME_PROP);
    result.remove(BackupManager.COLLECTION_NAME_PROP);
    result.put("indexSizeMB", Double.valueOf(properties.getProperty("indexSizeMB")));
    result.put("indexFileCount", Integer.valueOf(properties.getProperty("indexFileCount")));
    if (extraProperties != null && !extraProperties.isEmpty()) {
      result.put("extraProperties", extraProperties);
    }

    Map<String, String> shardBackupIds = new HashMap<>();
    Iterator<String> keyIt = result.keySet().iterator();
    while (keyIt.hasNext()) {
      String key = keyIt.next();
      if (key.endsWith(".md")) {
        shardBackupIds.put(key.substring(0, key.length() - 3), properties.getProperty(key));
        keyIt.remove();
      }
    }
    result.put("shardBackupIds", shardBackupIds);
    return result;
  }

  public String getBackupName() {
    return properties.getProperty(BackupManager.BACKUP_NAME_PROP);
  }
}
