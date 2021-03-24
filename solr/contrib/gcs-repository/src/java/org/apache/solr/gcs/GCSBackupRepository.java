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

package org.apache.solr.gcs;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.*;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;

public class GCSBackupRepository implements BackupRepository {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final int LARGE_BLOB_THRESHOLD_BYTE_SIZE = 5 * 1024 * 1024;
    private static final int BUFFER_SIZE = 16 * 1024 * 1024;
    protected Storage storage;

    @SuppressWarnings("rawtypes")
    private NamedList config = null;
    private String bucketName = "backup-managed-solr";

    protected Storage initStorage(String credentialPath) {
        if (storage != null)
            return storage;

        try {
            if (credentialPath == null) {
                credentialPath = System.getenv("GCS_CREDENTIAL");
                if (credentialPath == null) {
                    credentialPath = System.getenv("BACKUP_CREDENTIAL");
                }
            }
            StorageOptions.Builder builder = StorageOptions.newBuilder();
            if (credentialPath != null) {
                log.info("Creating GCS client using credential at {}", credentialPath);
                GoogleCredentials credential = GoogleCredentials.fromStream(new FileInputStream(credentialPath));
                builder.setCredentials(credential);
            }

            // TODO JEGERLOW Allow configuration for some/all of these properties?
            storage = builder
                    .setTransportOptions(StorageOptions.getDefaultHttpTransportOptions().toBuilder()
                            .setConnectTimeout(20000)
                            .setReadTimeout(20000)
                            .build())
                    .setRetrySettings(RetrySettings.newBuilder().setMaxAttempts(10)
                            //http requests
                            .setInitialRetryDelay(Duration.ofSeconds(1))
                            .setMaxRetryDelay(Duration.ofSeconds(30))
                            .setRetryDelayMultiplier(1.0)
                            //rpc requests
                            .setInitialRpcTimeout(Duration.ofSeconds(10))
                            .setMaxRpcTimeout(Duration.ofSeconds(30))
                            .setRpcTimeoutMultiplier(1.0)
                            //total retry timeout
                            .setTotalTimeout(Duration.ofSeconds(300))
                            .build())
                    .build().getService();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return storage;
    }

    @Override
    public void init(@SuppressWarnings("rawtypes") NamedList args) {
        this.config = args;
        initStorage((String)args.get("credential"));

        if (args.get("bucket") != null) {
            this.bucketName = args.get("bucket").toString();
        } else {
            this.bucketName = System.getenv("GCS_BUCKET");
            if (this.bucketName == null) {
                this.bucketName = System.getenv().getOrDefault("BACKUP_BUCKET","backup-managed-solr");
            }
        }

        // TODO JEGERLOW We should fail early here if the required configuration wasn't provided or was invalid
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getConfigProperty(String name) {
        return (T) this.config.get(name);
    }

    @Override
    public URI createURI(String location) {
        Objects.requireNonNull(location);

        URI result;
        try {
            result = new URI(location);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Error on creating URI", e);
        }

        return result;
    }

    @Override
    public URI resolve(URI baseUri, String... pathComponents) {
        StringBuilder builder = new StringBuilder(baseUri.toString());
        for (String path : pathComponents) {
            if (path != null && !path.isEmpty()) {
                if (builder.charAt(builder.length()-1) != '/') {
                    builder.append('/');
                }
                builder.append(path);
            }
        }

        return URI.create(builder.toString());
    }

    @Override
    public boolean exists(URI path) throws IOException {
        if (path.toString().equals(getConfigProperty("location"))) {
            return true;
        }

        if (path.toString().endsWith("/")) {
            return storage.get(bucketName, path.toString(), Storage.BlobGetOption.fields()) != null;
        } else {
            List<BlobId> req = Arrays.asList(
                    BlobId.of(bucketName, path.toString()),
                    BlobId.of(bucketName, path.toString() + "/"));
            List<Blob> rs = storage.get(req);
            return rs.get(0) != null || rs.get(1) != null;
        }

    }

    @Override
    public PathType getPathType(URI path) throws IOException {
        if (path.toString().endsWith("/"))
            return PathType.DIRECTORY;

        Blob blob = storage.get(bucketName, path.toString()+"/", Storage.BlobGetOption.fields());
        if (blob != null)
            return PathType.DIRECTORY;

        return PathType.FILE;
    }

    private String toBlobName(URI path) {
        return path.toString();
    }

    @Override
    public String[] listAll(URI path) throws IOException {
        String blobName = path.toString();
        if (!blobName.endsWith("/"))
            blobName += "/";

        final String pathStr = blobName;
        final LinkedList<String> result = new LinkedList<>();
        // TODO JEGERLOW - LocalStorageHelper used for tests doesn't support fetching bucket.  Can _probably_ be replaced with the commented line below.
        //storage.list(bucketName, Storage.BlobListOption.currentDirectory(), Storage.BlobListOption.prefix(pathStr), Storage.BlobListOption.fields())
        storage.get(bucketName).list(
                Storage.BlobListOption.currentDirectory(),
                Storage.BlobListOption.prefix(pathStr),
                Storage.BlobListOption.fields()
        ).iterateAll().forEach(
                blob -> {
                    assert blob.getName().startsWith(pathStr);
                    final String suffixName = blob.getName().substring(pathStr.length());
                    if (!suffixName.isEmpty()) {
                        result.add(suffixName);
                    }
                });

        return result.toArray(new String[0]);
    }

    @Override
    public IndexInput openInput(URI dirPath, String fileName, IOContext ctx) throws IOException {
        return openInput(dirPath, fileName, ctx, 2 * 1024 * 1024);
    }

    private IndexInput openInput(URI dirPath, String fileName, IOContext ctx, int bufferSize) {
        String blobName = dirPath.toString();
        if (!blobName.endsWith("/")) {
            blobName += "/";
        }
        blobName += fileName;

        final BlobId blobId = BlobId.of(bucketName, blobName);
        final Blob blob = storage.get(blobId, Storage.BlobGetOption.fields(Storage.BlobField.SIZE));
        final ReadChannel readChannel = blob.reader();
        readChannel.setChunkSize(bufferSize);

        return new BufferedIndexInput(blobName, bufferSize) {

            @Override
            public long length() {
                return blob.getSize();
            }

            @Override
            protected void readInternal(ByteBuffer b) throws IOException {
                readChannel.read(b);
            }

            @Override
            protected void seekInternal(long pos) throws IOException {
                readChannel.seek(pos);
            }

            @Override
            public void close() throws IOException {
                readChannel.close();
            }
        };
    }

    @Override
    public OutputStream createOutput(URI path) throws IOException {
        final BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, toBlobName(path)).build();
        final Storage.BlobWriteOption[] writeOptions = new Storage.BlobWriteOption[0];
        final WriteChannel writeChannel = storage.writer(blobInfo, writeOptions);

        return Channels.newOutputStream(new WritableByteChannel() {
            @Override
            public int write(ByteBuffer src) throws IOException {
                return writeChannel.write(src);
            }

            @Override
            public boolean isOpen() {
                return writeChannel.isOpen();
            }

            @Override
            public void close() throws IOException {
                writeChannel.close();
            }
        });
    }

    @Override
    public void createDirectory(URI path) throws IOException {
        String name = path.toString();
        if (!name.endsWith("/"))
            name += "/";
        storage.create(BlobInfo.newBuilder(bucketName, name).build()) ;
    }

    @Override
    public void deleteDirectory(URI path) throws IOException {
        List<BlobId> blobIds = allBlobsAtDir(path);
        if (!blobIds.isEmpty()) {
            storage.delete(blobIds);
        } else {
            log.info("Path:{} doesn't have any blobs", path);
        }
    }

    private List<BlobId> allBlobsAtDir(URI path) throws IOException {
        String blobName = path.toString();
        if (!blobName.endsWith("/"))
            blobName += "/";

        final List<BlobId> result = new ArrayList<>();
        final String pathStr = blobName;
        // TODO JEGERLOW - LocalStorageHelper used for tests doesn't support fetching bucket.  Can _probably_ be replaced with the commented line below.
        //storage.list(bucketName, Storage.BlobListOption.prefix(pathStr), Storage.BlobListOption.fields())
        storage.get(bucketName).list(
                Storage.BlobListOption.prefix(pathStr),
                Storage.BlobListOption.fields()
        ).iterateAll().forEach(
                blob -> result.add(blob.getBlobId())
        );

        return result;

    }

    @Override
    public void delete(URI path, Collection<String> files, boolean ignoreNoSuchFileException) throws IOException {
        if (files.isEmpty()) {
            return;
        }
        String prefix;
        if (path.toString().endsWith("/")) {
            prefix = path.toString();
        } else {
            prefix = path.toString() + "/";
        }
        List<BlobId> blobDeletes = files.stream()
                .map(file -> BlobId.of(bucketName, prefix + file))
                .collect(Collectors.toList());
        List<Boolean> result = storage.delete(blobDeletes);
        if (!ignoreNoSuchFileException) {
            int failedDelete = result.indexOf(Boolean.FALSE);
            if (failedDelete != -1) {
                throw new NoSuchFileException("File " + blobDeletes.get(failedDelete).getName() + " was not found");
            }
        }
    }

    @Override
    public void copyIndexFileFrom(Directory sourceDir, String sourceFileName, URI destDir, String destFileName) throws IOException {
        String blobName = destDir.toString();
        if (!blobName.endsWith("/"))
            blobName += "/";
        blobName += destFileName;
        final BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, blobName).build();
        try (ChecksumIndexInput input = sourceDir.openChecksumInput(sourceFileName, DirectoryFactory.IOCONTEXT_NO_CACHE)) {
            if (input.length() <= CodecUtil.footerLength()) {
                throw new CorruptIndexException("file is too small:" + input.length(), input);
            }
            if (input.length() > LARGE_BLOB_THRESHOLD_BYTE_SIZE) {
                writeBlobResumable(blobInfo, input);
            } else {
                writeBlobMultipart(blobInfo, input, (int) input.length());
            }
        }
    }

    @Override
    public void copyIndexFileTo(URI sourceRepo, String sourceFileName, Directory dest, String destFileName) throws IOException {
        String blobName = sourceRepo.toString();
        if (!blobName.endsWith("/"))
            blobName += "/";
        blobName += sourceFileName;
        final BlobId blobId = BlobId.of(bucketName, blobName);
        try (final ReadChannel readChannel = storage.reader(blobId);
             IndexOutput output = dest.createOutput(destFileName, DirectoryFactory.IOCONTEXT_NO_CACHE)) {
            ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024 * 8);
            while (readChannel.read(buffer) > 0) {
                buffer.flip();
                byte[] arr = buffer.array();
                output.writeBytes(arr, buffer.position(), buffer.limit() - buffer.position());
                buffer.clear();
            }
        }

    }


    @Override
    public void close() throws IOException {

    }

    private void writeBlobMultipart(BlobInfo blobInfo, ChecksumIndexInput indexInput, int blobSize)
            throws IOException {
        byte[] bytes = new byte[blobSize];
        indexInput.readBytes(bytes, 0, blobSize - CodecUtil.footerLength());
        long checksum = CodecUtil.checkFooter(indexInput);
        ByteBuffer footerBuffer = ByteBuffer.wrap(bytes, blobSize - CodecUtil.footerLength(), CodecUtil.footerLength());
        writeFooter(checksum, footerBuffer);
        try {
            storage.create(blobInfo, bytes, Storage.BlobTargetOption.doesNotExist());
        } catch (final StorageException se) {
            if (se.getCode() == HTTP_PRECON_FAILED) {
                throw new FileAlreadyExistsException(blobInfo.getBlobId().getName(), null, se.getMessage());
            }
            throw se;
        }
    }

    private void writeBlobResumable(BlobInfo blobInfo, ChecksumIndexInput indexInput) throws IOException {
        try {
            final Storage.BlobWriteOption[] writeOptions = new Storage.BlobWriteOption[0];
            final WriteChannel writeChannel = storage.writer(blobInfo, writeOptions);

            ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
            writeChannel.setChunkSize(BUFFER_SIZE);

            long remain = indexInput.length() - CodecUtil.footerLength();
            while (remain > 0) {
                // reading
                int byteReads = (int) Math.min(buffer.capacity(), remain);
                indexInput.readBytes(buffer.array(), 0, byteReads);
                buffer.position(byteReads);
                buffer.flip();

                // writing
                writeChannel.write(buffer);
                buffer.clear();
                remain -= byteReads;
            }
            long checksum = CodecUtil.checkFooter(indexInput);
            ByteBuffer bytes = getFooter(checksum);
            writeChannel.write(bytes);
            writeChannel.close();
        } catch (final StorageException se) {
            if (se.getCode() == HTTP_PRECON_FAILED) {
                throw new FileAlreadyExistsException(blobInfo.getBlobId().getName(), null, se.getMessage());
            }
            throw se;
        }
    }

    private ByteBuffer getFooter(long checksum) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(CodecUtil.footerLength());
        writeFooter(checksum, buffer);
        return buffer;
    }

    private void writeFooter(long checksum, ByteBuffer buffer) throws IOException {
        IndexOutput out = new IndexOutput("", "") {

            @Override
            public void writeByte(byte b) throws IOException {
                buffer.put(b);
            }

            @Override
            public void writeBytes(byte[] b, int offset, int length) throws IOException {
                buffer.put(b, offset, length);
            }

            @Override
            public void close() throws IOException {

            }

            @Override
            public long getFilePointer() {
                return 0;
            }

            @Override
            public long getChecksum() throws IOException {
                return checksum;
            }
        };
        CodecUtil.writeFooter(out);
        buffer.flip();
    }

}
