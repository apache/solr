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

/**
 * Azure Blob Storage backup repository implementation for Apache Solr.
 *
 * <p>This package provides a {@link org.apache.solr.blob.BlobBackupRepository} implementation that
 * enables Solr to store and retrieve backup data from Azure Blob Storage.
 *
 * <p>The repository supports various Azure authentication methods including:
 *
 * <ul>
 *   <li>Connection strings
 *   <li>Account name and key
 *   <li>SAS tokens
 *   <li>Azure Identity (Managed Identity, Service Principal)
 * </ul>
 *
 * <p>Key components:
 *
 * <ul>
 *   <li>{@link org.apache.solr.blob.BlobBackupRepository} - Main repository implementation
 *   <li>{@link org.apache.solr.blob.BlobStorageClient} - Azure Blob Storage client wrapper
 *   <li>{@link org.apache.solr.blob.BlobBackupRepositoryConfig} - Configuration management
 * </ul>
 *
 * @see <a href="https://docs.microsoft.com/en-us/azure/storage/blobs/">Azure Blob Storage
 *     Documentation</a>
 */
package org.apache.solr.blob;
