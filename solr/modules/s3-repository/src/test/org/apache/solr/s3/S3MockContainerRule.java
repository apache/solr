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

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import org.junit.Assume;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

/**
 * JUnit rule that manages a single S3Mock testcontainer. Declare as a {@code @ClassRule} so the
 * (expensive to start) mock server is shared across all {@code @Test} methods in a class instead of
 * being restarted for each one; JUnit starts it before, and stops it after, the whole class runs.
 *
 * <p>Skips the calling test (via {@link Assume}) instead of failing outright if
 * Docker/Testcontainers isn't available in this environment.
 */
public class S3MockContainerRule extends ExternalResource {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String S3MOCK_DOCKER_IMAGE = "adobe/s3mock:5.1.0";

  private final String bucketName;

  private S3MockContainer s3MockContainer;

  public S3MockContainerRule(String bucketName) {
    this.bucketName = bucketName;
  }

  @Override
  protected void before() {
    Assume.assumeTrue(
        "Docker/Testcontainers not available; skipping test",
        DockerClientFactory.instance().isDockerAvailable());
    s3MockContainer =
        new S3MockContainer(DockerImageName.parse(S3MOCK_DOCKER_IMAGE))
            .withInitialBuckets(bucketName);
    s3MockContainer.start();
  }

  @Override
  protected void after() {
    if (s3MockContainer != null) {
      try {
        s3MockContainer.stop();
      } catch (Exception e) {
        log.error("Exception stopping S3Mock container", e);
      } finally {
        s3MockContainer = null;
      }
    }
  }

  /** Full HTTP endpoint URL (including mapped host/port). */
  public String getHttpEndpoint() {
    return s3MockContainer.getHttpEndpoint();
  }

  public S3Client createS3ClientV2() {
    return S3Client.builder()
        .endpointOverride(URI.create(s3MockContainer.getHttpEndpoint()))
        .region(Region.US_EAST_1)
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
        .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
        .build();
  }
}
