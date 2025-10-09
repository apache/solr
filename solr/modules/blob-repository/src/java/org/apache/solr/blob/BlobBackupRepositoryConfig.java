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
package org.apache.solr.blob;

import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.NamedList;

/** Class representing the {@code backup} Blob Storage config bundle specified in solr.xml. */
public class BlobBackupRepositoryConfig {

  public static final String CONTAINER_NAME = "blob.container.name";
  public static final String CONNECTION_STRING = "blob.connection.string";
  public static final String ENDPOINT = "blob.endpoint";
  public static final String ACCOUNT_NAME = "blob.account.name";
  public static final String ACCOUNT_KEY = "blob.account.key";
  public static final String SAS_TOKEN = "blob.sas.token";
  public static final String TENANT_ID = "blob.tenant.id";
  public static final String CLIENT_ID = "blob.client.id";
  public static final String CLIENT_SECRET = "blob.client.secret";

  private final String containerName;
  private final String connectionString;
  private final String endpoint;
  private final String accountName;
  private final String accountKey;
  private final String sasToken;
  private final String tenantId;
  private final String clientId;
  private final String clientSecret;

  public BlobBackupRepositoryConfig(NamedList<?> config) {
    containerName = getStringConfig(config, CONTAINER_NAME);
    connectionString = getStringConfig(config, CONNECTION_STRING);
    endpoint = getStringConfig(config, ENDPOINT);
    accountName = getStringConfig(config, ACCOUNT_NAME);
    accountKey = getStringConfig(config, ACCOUNT_KEY);
    sasToken = getStringConfig(config, SAS_TOKEN);
    tenantId = getStringConfig(config, TENANT_ID);
    clientId = getStringConfig(config, CLIENT_ID);
    clientSecret = getStringConfig(config, CLIENT_SECRET);
  }

  /** Construct a {@link BlobStorageClient} from the provided config. */
  public BlobStorageClient buildClient() {
    return new BlobStorageClient(
        containerName,
        connectionString,
        endpoint,
        accountName,
        accountKey,
        sasToken,
        tenantId,
        clientId,
        clientSecret);
  }

  static String getStringConfig(NamedList<?> config, String property) {
    String envProp = EnvUtils.getProperty(property);
    if (envProp == null) {
      Object configProp = config.get(property);
      return configProp == null ? null : configProp.toString();
    } else {
      return envProp;
    }
  }
}
