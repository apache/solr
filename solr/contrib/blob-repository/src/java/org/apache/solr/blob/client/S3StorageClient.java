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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.io.input.ClosedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Creates a {@link BlobStorageClient} for communicating with AWS S3. Utilizes the default credential provider chain;
 * reference <a href="https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html">AWS SDK docs</a> for
 * details on where this client will fetch credentials from, and the order of precedence.
 */
public class S3StorageClient implements BlobStorageClient {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // S3 has a hard limit of 1000 keys per batch delete request
    private static final int MAX_KEYS_PER_BATCH_DELETE = 1000;

    // Metadata name used to identify flag directory entries in S3
    private static final String BLOB_DIR_HEADER = "x_is_directory";

    // Error messages returned by S3 for a key not found.
    private static final ImmutableSet<String> NOT_FOUND_CODES = ImmutableSet.of("NoSuchKey", "404 Not Found");

    private final AmazonS3 s3Client;

    /**
     * The S3 bucket where we write all of our blobs to.
     */
    private final String bucketName;

    public S3StorageClient(String bucketName, String region, String proxyHost, int proxyPort) {
        this(createInternalClient(region, proxyHost, proxyPort), bucketName);
    }

    @VisibleForTesting
    S3StorageClient(AmazonS3 s3Client, String bucketName) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
    }

    private static AmazonS3 createInternalClient(String region, String proxyHost, int proxyPort) {
        ClientConfiguration clientConfig = new ClientConfiguration()
            .withProtocol(Protocol.HTTPS);

        // If configured, add proxy
        if (!Strings.isNullOrEmpty(proxyHost)) {
            clientConfig.setProxyHost(proxyHost);
            if (proxyPort > 0) {
                clientConfig.setProxyPort(proxyPort);
            }
        }

        /*
         * Default s3 client builder loads credentials from disk and handles token refreshes
         */
        return AmazonS3ClientBuilder.standard()
            .enablePathStyleAccess()
            .withClientConfiguration(clientConfig)
            .withRegion(Regions.fromName(region))
            .build();
    }

    /**
     * Create Directory in Blob Store
     *
     * @param path         Directory Path in Blob Store
     */
    @Override
    public void createDirectory(String path) throws BlobException {
        path = sanitizedPath(path, false);

        if (!parentDirectoryExist(path)) {
            createDirectory(path.substring(0, path.lastIndexOf(BLOB_FILE_PATH_DELIMITER)));
            //TODO see https://issues.apache.org/jira/browse/SOLR-15359
//            throw new BlobException("Parent directory doesn't exist, path=" + path);
        }

        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.addUserMetadata(BLOB_DIR_HEADER, "true");
        objectMetadata.setContentLength(0);

        // Create empty blob object with header
        final InputStream im = ClosedInputStream.CLOSED_INPUT_STREAM;

        try {
            PutObjectRequest putRequest = new PutObjectRequest(bucketName, path, im, objectMetadata);
            s3Client.putObject(putRequest);
        } catch (AmazonClientException ase) {
            throw handleAmazonException(ase);
        }
    }

    /**
     * Delete files in Blob Store. Deletion order is not guaranteed.
     */
    @Override
    public void delete(Collection<String> paths) throws BlobException {
        Set<String> entries = new HashSet<>();
        for (String path : paths) {
            entries.add(sanitizedPath(path, true));
        }

        deleteBlobs(entries);
    }

    @Override
    public void deleteDirectory(String path) throws BlobException {
        path = sanitizedPath(path, false);

        List<String> entries = new ArrayList<>();
        entries.add(path);

        // Get all the files and subdirectories
        entries.addAll(listAll(path));

        deleteObjects(entries);
    }

    /**
     * List all the Files and Sub directories in Path
     *
     * @param path Path to Directory in Blob Store
     * @return Files and Sub directories in Path
     */
    @Override
    public String[] listDir(String path) throws BlobException {
        path = sanitizedPath(path, false);

        String prefix = path.equals("/") ? path : path + BLOB_FILE_PATH_DELIMITER;
        ListObjectsRequest listRequest = new ListObjectsRequest()
            .withBucketName(bucketName)
            .withPrefix(prefix)
            .withDelimiter(BLOB_FILE_PATH_DELIMITER);

        List<String> entries = new ArrayList<>();
        try {
            ObjectListing objectListing = s3Client.listObjects(listRequest);

            while (true) {
                List<String> files = objectListing.getObjectSummaries().stream()
                        .map(S3ObjectSummary::getKey)
                        // This filtering is needed only for S3mock. Real S3 does not ignore the trailing '/' in the prefix.
                        .filter(s -> s.startsWith(prefix))
                        .map(s -> s.substring(prefix.length()))
                        .collect(Collectors.toList());

                entries.addAll(files);

                if (objectListing.isTruncated()) {
                    objectListing = s3Client.listNextBatchOfObjects(objectListing);
                } else {
                    break;
                }
            }
            return entries.toArray(new String[0]);
        } catch (AmazonClientException ase) {
            throw handleAmazonException(ase);
        }
    }

    /**
     * Check if path exists
     *
     * @param path to File/Directory in Blob Store
     * @return true if path exists otherwise false
     */
    @Override
    public boolean pathExists(String path) throws BlobException {
        path = sanitizedPath(path, false);

        // for root return true
        if ("/".equals(path)) {
            return true;
        }

        try {
            return s3Client.doesObjectExist(bucketName, path);
        } catch (AmazonClientException ase) {
            throw handleAmazonException(ase);
        }
    }

    /**
     * Check if path is directory
     *
     * @param path to File/Directory in Blob Store
     * @return true if path is directory otherwise false
     */
    @Override
    public boolean isDirectory(String path) throws BlobException {
        path = sanitizedPath(path, false);

        try {
            ObjectMetadata objectMetadata = s3Client.getObjectMetadata(bucketName, path);
            String blobDirHeaderVal = objectMetadata.getUserMetaDataOf(BLOB_DIR_HEADER);

            return !Strings.isNullOrEmpty(blobDirHeaderVal) && blobDirHeaderVal.equalsIgnoreCase("true");
        } catch (AmazonClientException ase) {
            throw handleAmazonException(ase);
        }
    }

    /**
     * Get length of File in Bytes
     *
     * @param path to File in Blob Store
     * @return length of File
     */
    @Override
    public long length(String path) throws BlobException {
        path = sanitizedPath(path, true);
        try {
            ObjectMetadata objectMetadata = s3Client.getObjectMetadata(bucketName, path);
            String blobDirHeaderVal = objectMetadata.getUserMetaDataOf(BLOB_DIR_HEADER);

            if (Strings.isNullOrEmpty(blobDirHeaderVal) || !blobDirHeaderVal.equalsIgnoreCase("true")) {
                return objectMetadata.getContentLength();
            }
            throw new BlobException("Path is Directory");
        } catch (AmazonClientException ase) {
            throw handleAmazonException(ase);
        }
    }

    /**
     * Get InputStream of File for Read
     *
     * @param path to File in Blob Store
     * @return InputStream for File
     */
    @Override
    public InputStream pullStream(String path) throws BlobException {
        path = sanitizedPath(path, true);

        try {
            S3Object requestedObject = s3Client.getObject(bucketName, path);
            // This InputStream instance needs to be closed by the caller
            return requestedObject.getObjectContent();
        } catch (AmazonClientException ase) {
            throw handleAmazonException(ase);
        }
    }

    /**
     * Get OutputStream of File for Write. Caller needs to close the stream
     *
     * @param path         to File in Blob Store
     * @return OutputStream for File
     */
    @Override
    public OutputStream pushStream(String path) throws BlobException {
        path = sanitizedPath(path, true);

        if (!parentDirectoryExist(path)) {
            throw new BlobException("Parent directory doesn't exist");
        }

        try {
            return new S3OutputStream(s3Client, path, bucketName);
        } catch (AmazonClientException ase) {
            throw handleAmazonException(ase);
        }
    }

    public BlobstoreProviderType getStorageProvider() {
        return BlobstoreProviderType.S3;
    }

    @Override
    public void close() {
        s3Client.shutdown();
    }

    /**
     * Batch delete blob files from the blob store.
     *
     * @param entries collection of blob file keys to the files to be deleted.
     **/
    private void deleteBlobs(Collection<String> entries) throws BlobException {
        Collection<String> deletedPaths = deleteObjects(entries);

        // If we haven't deleted all requested objects, assume that's because some were missing
        if (entries.size() != deletedPaths.size()) {
            Set<String> notDeletedPaths = new HashSet<>(entries);
            entries.removeAll(deletedPaths);
            throw new BlobNotFoundException(notDeletedPaths.toString());
        }
    }

    /**
     *  Any blob file path that specifies a non-existent blob file will not be treated as an error.
     */
    private Collection<String> deleteObjects(Collection<String> paths) throws BlobException {
        try {
            /*
             * Per the S3 docs:
             * https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/DeleteObjectsResult.html
             * An exception is thrown if there's a client error processing the request or in
             * the Blob store itself. However there's no guarantee the delete did not happen
             * if an exception is thrown.
             */
            return deleteObjects(paths, MAX_KEYS_PER_BATCH_DELETE);
        } catch (AmazonClientException ase) {
            throw handleAmazonException(ase);
        }
    }

    @VisibleForTesting
    Collection<String> deleteObjects(Collection<String> entries, int batchSize) {
        List<KeyVersion> keysToDelete = entries.stream()
            .map(KeyVersion::new)
            .collect(Collectors.toList());

        List<List<KeyVersion>> partitions = Lists.partition(keysToDelete, batchSize);
        Set<String> deletedPaths = new HashSet<>();

        for (List<KeyVersion> partition : partitions) {
            DeleteObjectsRequest request = createBatchDeleteRequest(partition);

            DeleteObjectsResult result = s3Client.deleteObjects(request);

            result.getDeletedObjects().stream()
                    .map(DeleteObjectsResult.DeletedObject::getKey)
                    .forEach(deletedPaths::add);
        }

        return deletedPaths;
    }

    private DeleteObjectsRequest createBatchDeleteRequest(List<KeyVersion> keysToDelete) {
        return new DeleteObjectsRequest(bucketName).withKeys(keysToDelete);
    }

    private List<String> listAll(String path) throws BlobException {
        String prefix = path + BLOB_FILE_PATH_DELIMITER;
        ListObjectsRequest listRequest = new ListObjectsRequest()
            .withBucketName(bucketName)
            .withPrefix(prefix);

        List<String> entries = new ArrayList<>();
        try {
            ObjectListing objectListing = s3Client.listObjects(listRequest);

            while (true) {
                List<String> files = objectListing.getObjectSummaries().stream()
                        .map(S3ObjectSummary::getKey)
                        // This filtering is needed only for S3mock. Real S3 does not ignore the trailing '/' in the prefix.
                        .filter(s -> s.startsWith(prefix))
                        .collect(Collectors.toList());

                entries.addAll(files);

                if (objectListing.isTruncated()) {
                    objectListing = s3Client.listNextBatchOfObjects(objectListing);
                } else {
                    break;
                }
            }
            return entries;
        } catch (AmazonClientException ase) {
            throw handleAmazonException(ase);
        }
    }

    /**
     * Assumes the path does not end in a trailing slash
     */
    private boolean parentDirectoryExist(String path) throws BlobException {
        if (!path.contains(BLOB_FILE_PATH_DELIMITER)) {
            // Should only happen in S3Mock cases; otherwise we validate that all paths start with '/'
            return true;
        }

        String parentDirectory = path.substring(0, path.lastIndexOf(BLOB_FILE_PATH_DELIMITER));

        // If we have no specific parent directory, we consider parent is root (and always exists)
        if (parentDirectory.isEmpty()) {
            return true;
        }

        return pathExists(parentDirectory);
    }

    /**
     * Ensures path adheres to some rules:
     * -Starts with a leading slash
     * -If it's a file, throw an error if it ends with a trailing slash
     * -Else, silently trim the trailing slash
     */
    String sanitizedPath(String path, boolean isFile) throws BlobException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));

        // Trim space from start and end
        String sanitizedPath = path.trim();

        // Path should start with file delimiter
        if (!sanitizedPath.startsWith(BLOB_FILE_PATH_DELIMITER)) {
            throw new BlobException("Invalid Path. Path needs to start with '/'");
        }

        if (isFile && sanitizedPath.endsWith(BLOB_FILE_PATH_DELIMITER)) {
            throw new BlobException("Invalid Path. Path for file can't end with '/'");
        }

        // Trim file delimiter from end
        if (sanitizedPath.length() > 1 && sanitizedPath.endsWith(BLOB_FILE_PATH_DELIMITER)) {
            sanitizedPath = sanitizedPath.substring(0, path.length() - 1);
        }

        return sanitizedPath;
    }

    /**
     * Best effort to handle Amazon exceptions as checked exceptions. Amazon exception are all subclasses
     * of {@link RuntimeException} so some may still be uncaught and propagated.
     */
    static BlobException handleAmazonException(AmazonClientException ace) {

        if (ace instanceof AmazonServiceException) {
            AmazonServiceException ase = (AmazonServiceException)ace;
            String errMessage = String.format(Locale.ROOT, "An AmazonServiceException was thrown! [serviceName=%s] "
                            + "[awsRequestId=%s] [httpStatus=%s] [s3ErrorCode=%s] [s3ErrorType=%s] [message=%s]",
                    ase.getServiceName(), ase.getRequestId(), ase.getStatusCode(),
                    ase.getErrorCode(), ase.getErrorType(), ase.getErrorMessage());

            log.error(errMessage);

            if (ase.getStatusCode() == 404 && NOT_FOUND_CODES.contains(ase.getErrorCode())) {
                return new BlobNotFoundException(errMessage, ase);
            } else {
                return new BlobException(errMessage, ase);
            }
        }

        return new BlobException(ace);
    }
}
