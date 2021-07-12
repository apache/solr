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
package org.apache.solr.s3;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
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
public class S3BackupRepository implements BackupRepository {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private static final int CHUNK_SIZE = 16 * 1024 * 1024; // 16 MBs
    static final String S3_SCHEME = "s3";

    private NamedList<?> config;
    private S3StorageClient client;

    @Override
    public void init(NamedList<?> args) {
        this.config = args;
        S3BackupRepositoryConfig backupConfig = new S3BackupRepositoryConfig(this.config);

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
        if (StringUtils.isEmpty(location)) {
            throw new IllegalArgumentException("cannot create URI with an empty location");
        }

        URI result;
        try {
            result = new URI(location);
            if (!result.isAbsolute()) {
                if (location.startsWith("/")) {
                    return new URI(S3_SCHEME, null, location, null);
                } else {
                    return new URI(S3_SCHEME, null, "/" + location, null);
                }
            }
        } catch (URISyntaxException ex) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ex);
        }

        return result;
    }

    @Override
    public URI resolve(URI baseUri, String... pathComponents) {
        if (!S3_SCHEME.equalsIgnoreCase(baseUri.getScheme())) {
            throw new IllegalArgumentException("URI must being with 's3:' scheme");
        }

        // If paths contains unnecessary '/' separators, they'll be removed by URI.normalize()
        String path = baseUri + "/" + String.join("/", pathComponents);
        return URI.create(path).normalize();
    }

    @Override
    public void createDirectory(URI path) throws IOException {
        Objects.requireNonNull(path, "cannot create directory to a null URI");

        String blobPath = getS3Path(path);

        if (log.isDebugEnabled()) {
            log.debug("Create directory '{}'", blobPath);
        }

        client.createDirectory(blobPath);
    }

    @Override
    public void deleteDirectory(URI path) throws IOException {
        Objects.requireNonNull(path, "cannot delete directory with a null URI");

        String blobPath = getS3Path(path);

        if (log.isDebugEnabled()) {
            log.debug("Delete directory '{}'", blobPath);
        }

        client.deleteDirectory(blobPath);
    }

    @Override
    public void delete(URI path, Collection<String> files, boolean ignoreNoSuchFileException) throws IOException {
        Objects.requireNonNull(path, "cannot delete files without a valid URI path");
        Objects.requireNonNull(files, "collection of files to delete cannot be null");

        if (log.isDebugEnabled()) {
            log.debug("Delete files {} from {}", files, getS3Path(path));
        }

        Set<String> filesToDelete = files.stream()
            .map(file -> resolve(path, file))
            .map(S3BackupRepository::getS3Path)
            .collect(Collectors.toSet());

        try {
            client.delete(filesToDelete);
        } catch (S3NotFoundException e) {
            if (!ignoreNoSuchFileException) {
                throw e;
            }
        }
    }

    @Override
    public boolean exists(URI path) throws IOException {
        Objects.requireNonNull(path, "cannot test for existence of a null URI path");

        String blobPath = getS3Path(path);

        if (log.isDebugEnabled()) {
            log.debug("Path exists '{}'", blobPath);
        }

        return client.pathExists(blobPath);
    }

    @Override
    public IndexInput openInput(URI path, String fileName, IOContext ctx) throws IOException {
        Objects.requireNonNull(path, "cannot open a input stream without a valid URI path");
        if (StringUtils.isEmpty(fileName)) {
            throw new IllegalArgumentException("need a valid file name to read from S3");
        }

        URI filePath = resolve(path, fileName);
        String blobPath = getS3Path(filePath);

        if (log.isDebugEnabled()) {
            log.debug("Read from S3 '{}'", blobPath);
        }

        return new S3IndexInput(client.pullStream(blobPath), blobPath, client.length(blobPath));
    }

    @Override
    public OutputStream createOutput(URI path) throws IOException {
        Objects.requireNonNull(path, "cannot write to S3 without a valid URI path");

        String blobPath = getS3Path(path);

        if (log.isDebugEnabled()) {
            log.debug("Write to S3 '{}'", blobPath);
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
        String blobPath = getS3Path(path);

        if (log.isDebugEnabled()) {
            log.debug("listAll for '{}'", blobPath);
        }

        return client.listDir(blobPath);
    }

    @Override
    public PathType getPathType(URI path) throws IOException {
        String blobPath = getS3Path(path);

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
     */
    @Override
    public void copyIndexFileFrom(Directory sourceDir, String sourceFileName, URI dest, String destFileName) throws IOException {
        Objects.requireNonNull(sourceDir);
        Objects.requireNonNull(dest);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(sourceFileName));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(destFileName));

        URI filePath = resolve(dest, destFileName);
        String blobPath = getS3Path(filePath);
        Instant start = Instant.now();
        if (log.isDebugEnabled()) {
            log.debug("Upload started to S3 '{}'", blobPath);
        }

        try (IndexInput indexInput = sourceDir.openInput(sourceFileName, IOContext.DEFAULT)) {
            client.createDirectory(getS3Path(dest));
            try (OutputStream outputStream = client.pushStream(blobPath)) {

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
            }
        }

        long timeElapsed = Duration.between(start, Instant.now()).toMillis();
        if (log.isInfoEnabled()) {
            log.info("Upload to S3: '{}' finished in {}ms", blobPath, timeElapsed);
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
     */
    @Override
    public void copyIndexFileTo(URI sourceDir, String sourceFileName, Directory dest, String destFileName) throws IOException {
        Objects.requireNonNull(sourceDir);
        Objects.requireNonNull(dest);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(sourceFileName));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(destFileName));

        URI filePath = resolve(sourceDir, sourceFileName);
        String blobPath = getS3Path(filePath);
        Instant start = Instant.now();
        if (log.isDebugEnabled()) {
            log.debug("Download started from S3 '{}'", blobPath);
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
            log.info("Download from S3 '{}' finished in {}ms", blobPath, timeElapsed);
        }
    }

    @Override
    public void close() {
        client.close();
    }

    /**
     * Return the path to use in underlying blob store.
     */
    private static String getS3Path(URI uri) {
        // Depending on the scheme, the first element may be the host. Following ones are the path
        String host = uri.getHost();
        return host == null ? uri.getPath() : host + uri.getPath();
    }
}
