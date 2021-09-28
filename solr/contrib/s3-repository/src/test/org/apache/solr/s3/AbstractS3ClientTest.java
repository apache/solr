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

import com.adobe.testing.s3mock.junit4.S3MockRule;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;

import io.findify.s3mock.S3Mock;
import org.apache.commons.io.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/** Abstract class for test with S3Mock. */
public class AbstractS3ClientTest extends SolrTestCaseJ4 {

  private static final String BUCKET_NAME = "test-bucket";

  S3StorageClient client;

  private static S3Mock s3Mock;
  private static int s3MockPort;

  @BeforeClass
  public static void setUpS3Mock() {
    s3Mock = S3Mock.create(0);
    s3MockPort = s3Mock.start().localAddress().getPort();

    try (S3Client s3 = S3Client.builder()
        .endpointOverride(URI.create("http://localhost:" + s3MockPort))
        .region(Region.of("us-west-2"))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
        .serviceConfiguration(builder -> builder.pathStyleAccessEnabled(true))
        .httpClientBuilder(UrlConnectionHttpClient.builder())
        .build()) {
      s3.createBucket(r -> r.bucket(BUCKET_NAME));
    }
  }

  @AfterClass
  public static void tearDownS3Mock() {
    s3Mock.shutdown();
  }

  @Before
  public void setUpClient() {
    System.setProperty("aws.accessKeyId", "foo");
    System.setProperty("aws.secretAccessKey", "bar");

    client =
        new S3StorageClient(
            BUCKET_NAME, "us-east-1", "", false, "http://localhost:" + s3MockPort);
  }

  @After
  public void tearDownClient() {
    client.close();
  }

  /**
   * Helper method to push a string to S3.
   *
   * @param path Destination path in S3.
   * @param content Arbitrary content for the test.
   */
  void pushContent(String path, String content) throws S3Exception {
    try (OutputStream output = client.pushStream(path)) {
      IOUtils.write(content, output, Charset.defaultCharset());
    } catch (IOException e) {
      throw new S3Exception(e);
    }
  }
}
