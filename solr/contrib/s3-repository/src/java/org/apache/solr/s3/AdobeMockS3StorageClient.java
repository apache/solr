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

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.common.annotations.VisibleForTesting;

/**
 * This storage client exists to work around some of the incongruencies Adobe S3Mock has with the S3 API.
 * The main difference is that S3Mock does not support paths with a leading '/', but S3 does, and our code
 * in {@link S3StorageClient} requires all paths to have a leading '/'.
 */
class AdobeMockS3StorageClient extends S3StorageClient {

    static final int DEFAULT_MOCK_S3_PORT = 9090;
    private static final String DEFAULT_MOCK_S3_ENDPOINT = "http://localhost:" + DEFAULT_MOCK_S3_PORT;

    AdobeMockS3StorageClient(String bucketName) {
        super(createInternalClient(), bucketName);
    }

    @VisibleForTesting
    AdobeMockS3StorageClient(AmazonS3 s3client, String bucketName) {
        super(s3client, bucketName);
    }

    private static AmazonS3 createInternalClient() {
        String s3MockEndpoint = System.getenv().getOrDefault("MOCK_S3_ENDPOINT", DEFAULT_MOCK_S3_ENDPOINT);

        return AmazonS3ClientBuilder.standard()
            .enablePathStyleAccess()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3MockEndpoint, Regions.US_EAST_1.name()))
            .build();
    }

    /**
     * Ensures path adheres to some rules (different than the rules that S3 cares about):
     * -Trims leading slash, if given
     * -If it's a file, throw an error if it ends with a trailing slash
     */
    @Override
    String sanitizedPath(String path, boolean isFile) throws S3Exception {
        // Trim space from start and end
        String sanitizedPath = path.trim();

        // Throw error if it's a file with a trailing slash
        if (isFile && sanitizedPath.endsWith(BLOB_FILE_PATH_DELIMITER)) {
            throw new S3Exception("Invalid Path. Path for file can't end with '/'");
        }

        // Trim off leading slash
        if (sanitizedPath.length() > 1 && sanitizedPath.charAt(0) == '/') {
            return sanitizedPath.substring(1);
        } else {
            return sanitizedPath;
        }
    }
}
