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

import java.io.Closeable;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;

public interface BlobStorageClient extends Closeable {

    public static final String BLOB_FILE_PATH_DELIMITER = "/";

    /**
     * Create Directory in Blob Store
     *
     * @param path Directory Path in Blob Store
     */
    void createDirectory(String path) throws BlobException;

    /**
     * Delete files in Blob Store
     *
     * @param paths Paths to files or blobs in Blob Store
     */
    void delete(Collection<String> paths) throws BlobException;

    /**
     * Delete Directory and all the Files and Sub directories in Blob Store
     *
     * @param path Path to Directory in Blob Store
     */
    void deleteDirectory(String path) throws BlobException;

    /**
     * List all the Files and Sub directories in Path
     *
     * @param path Path to Directory in Blob Store
     * @return Files and Sub directories in Path
     */
    String[] listDir(String path) throws BlobException;

    /**
     * Check if path exists
     *
     * @param path to File/Directory in Blob Store
     * @return true if path exists otherwise false
     */
    boolean pathExists(String path) throws BlobException;

    /**
     * Check if path is directory
     *
     * @param path to File/Directory in Blob Store
     * @return true if path is directory otherwise false
     */
    boolean isDirectory(String path) throws BlobException;

    /**
     * Get length of File in Bytes
     *
     * @param path to File in Blob Store
     * @return length of File
     */
    long length(String path) throws BlobException;

    /**
     * Get InputStream of File for Read
     *
     * @param path to File in Blob Store
     * @return InputStream for File
     */
    InputStream pullStream(String path) throws BlobException;

    /**
     * Get OutputStream of File for Write. Caller needs to close the stream
     *
     * @param path to File in Blob Store
     * @return OutputStream for File
     */
    OutputStream pushStream(String path) throws BlobException;

    /**
     * Retrieves an identifier for the cloud service providing the blobstore
     *
     * @return string identifying the service provider
     */
    BlobstoreProviderType getStorageProvider();

    /**
     * Override {@link Closeable} since we throw no exception.
     */
    @Override
    void close();

}
