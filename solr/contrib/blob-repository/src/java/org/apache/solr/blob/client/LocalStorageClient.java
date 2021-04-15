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

import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.file.*;
import java.util.Collection;

/**
 * Primarily for testing purposes.
 */
public class LocalStorageClient implements BlobStorageClient {

    /**
     * The directory on the local file system where blobs will be stored.
     */
    private final String blobStoreRootDir;

    public LocalStorageClient(String blobStoreRootDir) {
        this.blobStoreRootDir = blobStoreRootDir;
    }

    @Override
    public void createDirectory(String path) throws BlobException {
        try {
            Path blobPath = getBlobAbsolutePath(path);
            Files.createDirectories(blobPath);
        } catch (IOException ex) {
            throw new BlobException(ex);
        }
    }

    @Override
    public void delete(Collection<String> paths) throws BlobException {
        try {
            for (String path : paths) {
                Path blobPath = getBlobAbsolutePath(path);
                Files.delete(blobPath);
            }
        } catch (NoSuchFileException e) {
            throw new BlobNotFoundException(e);
        } catch (IOException ex) {
            throw new BlobException(ex);
        }
    }

    @Override
    public void deleteDirectory(String path) throws BlobException {
        try {
            Path blobPath = getBlobAbsolutePath(path);
            FileUtils.deleteDirectory(blobPath.toFile());
        } catch (IOException ex) {
            throw new BlobException(ex);
        }
    }

    @Override
    public String[] listDir(String path) throws BlobException {
        try {
            Path blobPath = getBlobAbsolutePath(path);
            return Files.list(blobPath).map(Path::getFileName).map(Path::toString).toArray(String[]::new);
        } catch (IOException e) {
            throw new BlobException(e);
        }
    }

    @Override
    public boolean pathExists(String path) {
        Path blobPath = getBlobAbsolutePath(path);
        return Files.exists(blobPath);
    }

    @Override
    public boolean isDirectory(String path) {
        Path blobPath = getBlobAbsolutePath(path);
        return Files.isDirectory(blobPath);
    }

    @Override
    public long length(String path) throws BlobException {
        Path blobPath = getBlobAbsolutePath(path);

        if (!Files.exists(blobPath)) {
            throw new BlobNotFoundException(blobPath.toString());
        }

        // By default, symbolic links are followed
        if (!Files.isRegularFile(blobPath)) {
            throw new BlobException("Not a regular file");
        }

        try {
            return Files.size(blobPath);
        } catch (IOException e) {
            throw new BlobException(e);
        }
    }

    @Override
    public InputStream pullStream(String path) throws BlobException {
        try {
            Path blobPath = getBlobAbsolutePath(path);
            File blobFile = blobPath.toFile();
            return new FileInputStream(blobFile);
        } catch (FileNotFoundException ex) {
            throw new BlobNotFoundException(ex);
        }
    }

    @Override
    public OutputStream pushStream(String path) throws BlobException {
        try {
            Path blobPath = getBlobAbsolutePath(path);
            return Files.newOutputStream(blobPath);
        } catch (IOException ex) {
            throw new BlobException(ex);
        }
    }

    /**
     * Prefixes the given path with the blob store root directory on the local FS.
     * Concatenate two valid string paths together with a forward slash delimiter.
     */
    private Path getBlobAbsolutePath(String blobPath) {
        return Paths.get(blobStoreRootDir, blobPath);
    }

    @Override
    public BlobstoreProviderType getStorageProvider() {
        return BlobstoreProviderType.LOCAL;
    }

    @Override
    public void close() {
        // no-op
    }
}

