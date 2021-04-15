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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.solr.blob.client.BlobNotFoundException;
import org.apache.solr.blob.client.BlobStorageClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A concrete implementation of {@link BackupRepository} interface supporting backup/restore of Solr indexes to a blob store like S3, GCS.
 */
public class BlobBackupRepository implements BackupRepository {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int CHUNK_SIZE = 16 * 1024 * 1024; // 16 MBs
    static final String BLOB_SCHEME = "blob";

    private NamedList<String> config;
    private BlobStorageClient client;

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void init(NamedList args) {
        this.config = (NamedList<String>) args;
        BlobBackupRepositoryConfig backupConfig = new BlobBackupRepositoryConfig(this.config);

        // If a client was already created, close it to avoid any resource leak
        if (client != null) {
            client.close();
        }

        this.client = backupConfig.buildClient();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getConfigProperty(String name) {
        return (T) this.config.get(name);
    }

    @Override
    public URI createURI(String location) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(location));
        URI result;
        try {
            result = new URI(location);
            if (!result.isAbsolute()) {
                if (location.startsWith("/")) {
                    return new URI(BLOB_SCHEME, null, location, null);
                } else {
                    return new URI(BLOB_SCHEME, null, "/" + location, null);
                }
            }
        } catch (URISyntaxException ex) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ex);
        }

        return result;
    }

    @Override
    public URI resolve(URI baseUri, String... pathComponents) {
        Objects.requireNonNull(baseUri);
        Preconditions.checkArgument(baseUri.isAbsolute());
        Preconditions.checkArgument(pathComponents.length > 0);
        Preconditions.checkArgument(baseUri.getScheme().equalsIgnoreCase(BLOB_SCHEME));

        // If paths contains unnecessary '/' separators, they'll be removed by URI.normalize()
        String path = baseUri.toString() + "/" + String.join("/", pathComponents);
        return URI.create(path).normalize();
    }

    @Override
    public void createDirectory(URI path) throws IOException {
        Objects.requireNonNull(path);

        String blobPath = getBlobPath(path);

        if (log.isDebugEnabled()) {
            log.debug("Create directory '{}'", blobPath);
        }

        client.createDirectory(blobPath);
    }

    @Override
    public void deleteDirectory(URI path) throws IOException {
        Objects.requireNonNull(path);

        String blobPath = getBlobPath(path);

        if (log.isDebugEnabled()) {
            log.debug("Delete directory '{}'", blobPath);
        }

        client.deleteDirectory(blobPath);
    }

    @Override
    public void delete(URI path, Collection<String> files, boolean ignoreNoSuchFileException) throws IOException {
        Objects.requireNonNull(path);
        Objects.requireNonNull(files);

        if (log.isDebugEnabled()) {
            log.debug("Delete files {} from {}", files, getBlobPath(path));
        }
        Set<String> filesToDelete = files.stream()
            .map(file -> resolve(path, file))
            .map(BlobBackupRepository::getBlobPath)
            .collect(Collectors.toSet());

        try {
            client.delete(filesToDelete);
        } catch (BlobNotFoundException e) {
            if (!ignoreNoSuchFileException) {
                throw e;
            }
        }
    }

    @Override
    public boolean exists(URI path) throws IOException {
        Objects.requireNonNull(path);

        String blobPath = getBlobPath(path);

        if (log.isDebugEnabled()) {
            log.debug("Path exists '{}'", blobPath);
        }

        return client.pathExists(blobPath);
    }

    @Override
    public IndexInput openInput(URI path, String fileName, IOContext ctx) throws IOException {
        Objects.requireNonNull(path);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(fileName));

        URI filePath = resolve(path, fileName);
        String blobPath = getBlobPath(filePath);

        if (log.isDebugEnabled()) {
            log.debug("Read from blob '{}'", blobPath);
        }

        return new BlobIndexInput(client.pullStream(blobPath), blobPath, client.length(blobPath));
    }

    @Override
    public OutputStream createOutput(URI path) throws IOException {
        Objects.requireNonNull(path);

        String blobPath = getBlobPath(path);

        if (log.isDebugEnabled()) {
            log.debug("Write to blob '{}'", blobPath);
        }

        return client.pushStream(blobPath);
    }

    /**
     * This method returns all the entries (files and directories) in the specified directory.
     *
     * @param path The directory path
     * @return an array of strings, one for each entry in the directory
     */
    @Override
    public String[] listAll(URI path) throws IOException {
        Objects.requireNonNull(path);

        String blobPath = getBlobPath(path);

        if (log.isDebugEnabled()) {
            log.debug("listAll for '{}'", blobPath);
        }

        return client.listDir(blobPath);
    }

    @Override
    public PathType getPathType(URI path) throws IOException {
        Objects.requireNonNull(path);

        String blobPath = getBlobPath(path);

        if (log.isDebugEnabled()) {
            log.debug("getPathType for '{}'", blobPath);
        }

        return client.isDirectory(blobPath) ? PathType.DIRECTORY : PathType.FILE;
    }

    /**
     * Copy an index file from specified <code>sourceDir</code> to the destination repository (i.e. backup).
     *
     * @param sourceDir
     *          The source directory hosting the file to be copied.
     * @param sourceFileName
     *          The name of the file to be copied
     * @param dest
     *          The destination backup location.
     * @throws IOException
     *          in case of errors
     * @throws CorruptIndexException
     *          in case checksum of the file does not match with precomputed checksum stored at the end of the file
     * @since 8.3.0
     */
    @Override
    public void copyIndexFileFrom(Directory sourceDir, String sourceFileName, URI dest, String destFileName) throws IOException {
        Objects.requireNonNull(sourceDir);
        Objects.requireNonNull(dest);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(sourceFileName));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(destFileName));

        URI filePath = resolve(dest, destFileName);
        String blobPath = getBlobPath(filePath);
        Instant start = Instant.now();
        if (log.isDebugEnabled()) {
            log.debug("Upload started to blob'{}'", blobPath);
        }

        IndexInput indexInput = null;
        OutputStream outputStream = null;
        try {
            client.createDirectory(getBlobPath(dest));
            indexInput = sourceDir.openInput(sourceFileName, IOContext.DEFAULT);
            outputStream = client.pushStream(blobPath);

            byte[] buffer = new byte[CHUNK_SIZE];
            int bufferLen;
            long remaining = indexInput.length();

            while (remaining > 0) {
                bufferLen = remaining >= CHUNK_SIZE ? CHUNK_SIZE : (int) remaining;

                indexInput.readBytes(buffer, 0, bufferLen);
                outputStream.write(buffer, 0, bufferLen);
                remaining -= bufferLen;
            }
            outputStream.flush();
        } finally {
            if (indexInput != null) {
                indexInput.close();
            }
            if (outputStream != null) {
                outputStream.close();
            }
        }

        long timeElapsed = Duration.between(start, Instant.now()).toMillis();
        if (log.isInfoEnabled()) {
            log.info("Upload to blob: '{}' finished in {}ms", blobPath, timeElapsed);
        }
    }

    /**
     * Copy an index file from specified <code>sourceRepo</code> to the destination directory (i.e. restore).
     *
     * @param sourceDir
     *          The source URI hosting the file to be copied.
     * @param dest
     *          The destination where the file should be copied.
     * @throws IOException
     *           in case of errors.
     * @since 8.3.0
     */
    @Override
    public void copyIndexFileTo(URI sourceDir, String sourceFileName, Directory dest, String destFileName) throws IOException {
        Objects.requireNonNull(sourceDir);
        Objects.requireNonNull(dest);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(sourceFileName));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(destFileName));

        URI filePath = resolve(sourceDir, sourceFileName);
        String blobPath = getBlobPath(filePath);
        Instant start = Instant.now();
        if (log.isDebugEnabled()) {
            log.debug("Download started from blob '{}'", blobPath);
        }

        try (InputStream inputStream = client.pullStream(blobPath);
             IndexOutput indexOutput = dest.createOutput(destFileName, IOContext.DEFAULT)) {
            byte[] buffer = new byte[CHUNK_SIZE];
            int len;
            while ((len = inputStream.read(buffer)) != -1) {
                indexOutput.writeBytes(buffer, 0, len);
            }
        }

        long timeElapsed = Duration.between(start, Instant.now()).toMillis();

        if (log.isInfoEnabled()) {
            log.info("Download from blob '{}' finished in {}ms", blobPath, timeElapsed);
        }
    }

    @Override
    public void close() {
        client.close();
    }

    /**
     * Return the path to use in underlying blob store.
     */
    private static String getBlobPath(URI uri) {
        // Depending on the scheme, the first element may be the host. Following ones are the path
        String host = uri.getHost();
        return host == null ? uri.getPath() : Paths.get(host, uri.getPath()).toString();
    }
}
