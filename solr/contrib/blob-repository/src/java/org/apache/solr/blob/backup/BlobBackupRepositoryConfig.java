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
package org.apache.solr.blob.backup;

import com.google.common.base.Strings;
import org.apache.solr.blob.client.*;
import org.apache.solr.common.util.NamedList;

import java.util.Locale;
import java.util.Map;

/**
 * Class representing the {@code backup} blob config bundle specified in solr.xml. All user-provided config can be
 * overridden via environment variables (use uppercase, with '_' instead of '.'), see {@link BlobBackupRepositoryConfig#toEnvVar}.
 */
public class BlobBackupRepositoryConfig {

    public static final String PROVIDER_TYPE = "blob.store.provider.type";
    public static final String LOCAL_STORAGE_ROOT = "blob.store.localStorageRoot";
    public static final String BUCKET_NAME = "blob.store.bucket.name";
    public static final String REGION = "blob.store.region";
    public static final String PROXY_HOST = "blob.store.proxy.host";
    public static final String PROXY_PORT = "blob.store.proxy.port";

    public static final String UNKNOWN_PROVIDER_TYPE_MSG = "Blob storage provider [%s] is unknown. Please check configuration.";

    private final BlobstoreProviderType blobstoreProviderType;
    private String bucketName;
    private String proxyHost;
    private int proxyPort;
    private String localStorageRootDir;
    private String region;

    @SuppressWarnings({"rawtypes", "unchecked"})
    public BlobBackupRepositoryConfig(NamedList args) {
        NamedList<String> config = args.clone();

        String providerType = getStringConfig(config, PROVIDER_TYPE);

        blobstoreProviderType = BlobstoreProviderType.fromString(providerType);
        switch (blobstoreProviderType) {
            case S3:
                region = getStringConfig(config, REGION);
                bucketName = getStringConfig(config, BUCKET_NAME);
                proxyHost = getStringConfig(config, PROXY_HOST);
                proxyPort = getIntConfig(config, PROXY_PORT);
                break;
            case S3MOCK:
                bucketName = getStringConfig(config, BUCKET_NAME);
                break;
            case LOCAL:
                localStorageRootDir = getStringConfig(config, LOCAL_STORAGE_ROOT);
                break;
            default:
                // Default case for an unknown name already covered by fromString() call above. Handling here unexpected but parsed value
                throw new IllegalArgumentException(String.format(Locale.ROOT, BlobBackupRepositoryConfig.UNKNOWN_PROVIDER_TYPE_MSG, blobstoreProviderType));
        }
    }

    /**
     * Construct a {@link BlobStorageClient} from the provided config.
     */
    public BlobStorageClient buildClient() {
        switch (blobstoreProviderType) {
            case LOCAL:
                return new LocalStorageClient(localStorageRootDir);
            case S3MOCK:
                return new AdobeMockS3StorageClient(bucketName);
            case S3:
                return new S3StorageClient(bucketName, region, proxyHost, proxyPort);
            default:
                throw new IllegalArgumentException(String.format(Locale.ROOT, BlobBackupRepositoryConfig.UNKNOWN_PROVIDER_TYPE_MSG, blobstoreProviderType.name()));
        }
    }

    private static String getStringConfig(NamedList<String> config, String property) {
        Map<String, String> env = System.getenv();
        return env.getOrDefault(toEnvVar(property), config.get(property));
    }

    private static int getIntConfig(NamedList<String> config, String property) {
        String stringConfig = getStringConfig(config, property);

        if (!Strings.isNullOrEmpty(stringConfig)) {
            // Backup/restore cmd will fail if present but not an integer.
            return Integer.parseInt(stringConfig);
        } else {
            return 0;
        }
    }

    private static String toEnvVar(String property) {
        return property.toUpperCase(Locale.ROOT).replace('.', '_');
    }
}
