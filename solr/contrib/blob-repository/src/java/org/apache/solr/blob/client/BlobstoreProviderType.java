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
package org.apache.solr.blob.client;

import org.apache.solr.blob.backup.BlobBackupRepositoryConfig;

import java.util.Locale;

/**
 * Enum of identifiers for the underlying blob store service
 */
public enum BlobstoreProviderType {
    LOCAL,  // Local fs
    S3,     // AWS S3
    S3MOCK; // Adobe S3Mock

    /**
     * Util for doing case-insensitive lookup (instead of {@link Enum#valueOf}.
     */
    public static BlobstoreProviderType fromString(String input) {
        for (BlobstoreProviderType type : BlobstoreProviderType.values()) {
            if (type.name().equalsIgnoreCase(input)) {
                return type;
            }
        }
        throw new IllegalArgumentException(String.format(Locale.ROOT, BlobBackupRepositoryConfig.UNKNOWN_PROVIDER_TYPE_MSG, input));
    }
}
