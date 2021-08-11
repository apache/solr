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
 * This storage client exists to work around some of the incongruencies Adobe S3Mock has with the S3
 * API.
 */
class AdobeMockS3StorageClient extends S3StorageClient {

  private static final String DEFAULT_MOCK_S3_ENDPOINT = "http://localhost:9090";

  AdobeMockS3StorageClient(String bucketName, String endpoint) {
    super(createInternalClient(endpoint), bucketName);
  }

  @VisibleForTesting
  AdobeMockS3StorageClient(AmazonS3 s3client, String bucketName) {
    super(s3client, bucketName);
  }

  private static AmazonS3 createInternalClient(String endpoint) {
    if (endpoint == null || endpoint.isEmpty()) {
      endpoint = System.getProperty(S3BackupRepositoryConfig.ENDPOINT);
      if (endpoint == null || endpoint.isEmpty()) {
        endpoint = System.getenv().getOrDefault("BLOB_S3_ENDPOINT", DEFAULT_MOCK_S3_ENDPOINT);
      }
    }

    return AmazonS3ClientBuilder.standard()
        .enablePathStyleAccess()
        .withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(endpoint, Regions.US_EAST_1.name()))
        .build();
  }
}
