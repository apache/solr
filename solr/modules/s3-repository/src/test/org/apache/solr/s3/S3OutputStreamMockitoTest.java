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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

public class S3OutputStreamMockitoTest extends SolrTestCaseJ4 {

  private S3Client clientMock;

  private static byte[] largeBuffer;

  @BeforeClass
  public static void setUpClass() {
    assumeWorkingMockito();
    String content =
        RandomStrings.randomAsciiAlphanumOfLength(random(), S3OutputStream.PART_SIZE + 1024);
    largeBuffer = content.getBytes(StandardCharsets.UTF_8);
    // pre-check -- ensure that our test string isn't too small
    assertTrue(largeBuffer.length > S3OutputStream.PART_SIZE);
  }

  @AfterClass
  public static void tearDownClass() {
    largeBuffer = null;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    clientMock = mock(S3Client.class);
  }

  @SuppressWarnings("unchecked")
  public void testMultiPartUploadCompleted() throws IOException {
    when(clientMock.createMultipartUpload((Consumer<CreateMultipartUploadRequest.Builder>) any()))
        .thenReturn(CreateMultipartUploadResponse.builder().build());
    when(clientMock.uploadPart((UploadPartRequest) any(), (RequestBody) any()))
        .thenReturn(UploadPartResponse.builder().build());
    S3OutputStream stream = new S3OutputStream(clientMock, "key", "bucket");
    stream.write(largeBuffer);
    verify(clientMock)
        .createMultipartUpload((Consumer<CreateMultipartUploadRequest.Builder>) any());
    verify(clientMock).uploadPart((UploadPartRequest) any(), (RequestBody) any());
    verify(clientMock, never())
        .completeMultipartUpload((Consumer<CompleteMultipartUploadRequest.Builder>) any());
    verify(clientMock, never())
        .abortMultipartUpload((Consumer<AbortMultipartUploadRequest.Builder>) any());

    stream.close();
    verify(clientMock)
        .completeMultipartUpload((Consumer<CompleteMultipartUploadRequest.Builder>) any());
    verify(clientMock, never())
        .abortMultipartUpload((Consumer<AbortMultipartUploadRequest.Builder>) any());
  }

  @SuppressWarnings("unchecked")
  public void testMultiPartUploadAborted() throws IOException {
    when(clientMock.createMultipartUpload((Consumer<CreateMultipartUploadRequest.Builder>) any()))
        .thenReturn(CreateMultipartUploadResponse.builder().build());
    when(clientMock.uploadPart((UploadPartRequest) any(), (RequestBody) any()))
        .thenThrow(S3Exception.builder().message("fake exception").build());
    S3OutputStream stream = new S3OutputStream(clientMock, "key", "bucket");
    // first time it should throw the exception from S3Client
    org.apache.solr.s3.S3Exception solrS3Exception =
        assertThrows(org.apache.solr.s3.S3Exception.class, () -> stream.write(largeBuffer));
    assertEquals(S3Exception.class, solrS3Exception.getCause().getClass());
    assertEquals("fake exception", solrS3Exception.getCause().getMessage());
    verify(clientMock).abortMultipartUpload((Consumer<AbortMultipartUploadRequest.Builder>) any());

    // after that, the exception should be because the MPU is aborted
    solrS3Exception =
        assertThrows(org.apache.solr.s3.S3Exception.class, () -> stream.write(largeBuffer));
    assertEquals(IllegalStateException.class, solrS3Exception.getCause().getClass());
    assertTrue(
        "Unexpected exception message: " + solrS3Exception.getCause().getMessage(),
        solrS3Exception
            .getCause()
            .getMessage()
            .contains("Can't upload new parts on a MultipartUpload that was aborted"));

    verify(clientMock)
        .createMultipartUpload((Consumer<CreateMultipartUploadRequest.Builder>) any());
    verify(clientMock).uploadPart((UploadPartRequest) any(), (RequestBody) any());
    verify(clientMock, never())
        .completeMultipartUpload((Consumer<CompleteMultipartUploadRequest.Builder>) any());
    verify(clientMock, times(2))
        .abortMultipartUpload((Consumer<AbortMultipartUploadRequest.Builder>) any());

    stream.close();
    verify(clientMock, never())
        .completeMultipartUpload((Consumer<CompleteMultipartUploadRequest.Builder>) any());
  }
}
