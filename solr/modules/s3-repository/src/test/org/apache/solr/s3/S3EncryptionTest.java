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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.crypto.AEADBadTagException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ResumableInputStream;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.s3.S3BackupRepositoryConfig.EncryptionConfig;
import org.apache.solr.util.LogListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.encryption.s3.materials.AesKeyring;
import software.amazon.encryption.s3.materials.KmsKeyring;

/** Client-side encryption of object content, against S3Mock. */
@ThreadLeakFilters(
    filters = {
      SolrIgnoredThreadsFilter.class,
      QuickPatchThreadsFilter.class,
      S3MockTestcontainersThreadFilter.class,
      S3EncryptionThreadFilter.class
    })
public class S3EncryptionTest extends AbstractS3ClientTest {

  private static final int MIB = 1024 * 1024;

  /** The read is not retried, so the proxy sees its request almost at once. */
  private static final long PROXY_REQUEST_TIMEOUT_SECONDS = 30;

  private S3StorageClient encryptedClient;

  @Before
  public void setUpEncryptedClient() {
    encryptedClient = newClient(aesEncryption(newAesKey()));
  }

  @After
  public void tearDownEncryptedClient() {
    encryptedClient.close();
  }

  /** Small, exactly one part, exactly two parts, and a short final part. */
  @Test
  public void testRoundTrip() throws Exception {
    int partSize = S3OutputStream.PART_SIZE;
    for (int size : new int[] {7, partSize, 2 * partSize, partSize + 12345}) {
      String path = "round-trip-" + size;
      byte[] data = randomBytes(size);
      push(encryptedClient, path, data);

      assertArrayEquals("size " + size, data, pull(encryptedClient, path));
      assertEquals("plaintext length", size, encryptedClient.length(path));

      byte[] stored = rawObject(path);
      assertEquals(size + S3StorageClient.GCM_TAG_LENGTH, stored.length);
      assertFalse(
          "stored bytes must not be the plaintext",
          Arrays.equals(data, Arrays.copyOf(stored, size)));
    }
  }

  /** Byte-wise writes, a flush of an unaligned buffer, and a flush right before close. */
  @Test
  public void testByteWiseWritesAndFlush() throws Exception {
    int size = S3OutputStream.PART_SIZE + 1000;
    int flushAfter = S3OutputStream.MIN_PART_SIZE + 5;
    byte[] data = randomBytes(size);
    try (OutputStream output = encryptedClient.pushStream("byte-wise")) {
      for (int i = 0; i < size; i++) {
        output.write(data[i]);
        if (i == flushAfter) {
          output.flush();
        }
      }
      output.flush();
    }
    assertArrayEquals(data, pull(encryptedClient, "byte-wise"));
  }

  /** The encryption client cannot read objects over 64 MiB unless plaintext is streamed. */
  @Test
  public void testObjectOver64MiB() throws Exception {
    long size = 64L * MIB + 3;
    pushGenerated(encryptedClient, "over-64mib", size);
    assertEquals(size, encryptedClient.length("over-64mib"));
    try (InputStream input = encryptedClient.pullStream("over-64mib")) {
      assertGenerated(input, size);
    }
  }

  /**
   * An object larger than the whole heap can only be read if the plaintext is streamed: decrypting
   * it in memory before releasing it would run out of heap.
   */
  @Test
  @LuceneTestCase.Nightly
  public void testObjectLargerThanHeap() throws Exception {
    long size = Runtime.getRuntime().maxMemory() + 64L * MIB;
    pushGenerated(encryptedClient, "larger-than-heap", size);
    try (InputStream input = encryptedClient.pullStream("larger-than-heap")) {
      assertGenerated(input, size);
    } finally {
      encryptedClient.delete(List.of("larger-than-heap"));
    }
  }

  @Test
  public void testTamperedCiphertext() throws Exception {
    String path = "tampered";
    byte[] data = randomBytes(MIB + 5);
    push(encryptedClient, path, data);
    byte[] stored = rawObject(path);
    Map<String, String> metadata = rawMetadata(path);

    // In the content, and in the authentication tag
    for (int position : new int[] {10, stored.length - 1}) {
      byte[] tampered = stored.clone();
      tampered[position] ^= 1;
      rawPut(path, tampered, metadata);

      try (LogListener resumeLog = LogListener.warn(ResumableInputStream.class)) {
        S3Exception e =
            expectThrows(
                S3Exception.class,
                () -> {
                  try (InputStream input = encryptedClient.pullStream(path)) {
                    input.readAllBytes();
                  }
                });
        assertThat(e.getMessage(), containsString("'" + path + "' failed authentication"));

        // Read exactly the expected length without looking for the end, as S3IndexInput does
        long length = encryptedClient.length(path);
        e =
            expectThrows(
                S3Exception.class,
                () -> {
                  try (IndexInput input =
                      new S3IndexInput(encryptedClient.pullStream(path), path, length)) {
                    input.readBytes(new byte[(int) length], 0, (int) length);
                  }
                });
        assertThat(e.getMessage(), containsString("failed authentication"));
        assertEquals("a failed authentication must not be retried", 0, resumeLog.getCount());
      }
    }
  }

  /**
   * A connection dropped while skipping to the very end must not end the stream before the tag is
   * checked: the resumed read goes through the object again.
   */
  @Test
  public void testSkipToEndWithConnectionLoss() throws Exception {
    String path = "skip-connection-loss";
    byte[] data = randomBytes(40 * MIB);
    push(encryptedClient, path, data);
    byte[] stored = rawObject(path);
    Map<String, String> metadata = rawMetadata(path);

    for (boolean tamper : new boolean[] {false, true}) {
      if (tamper) {
        stored[stored.length - 1] ^= 1;
        rawPut(path, stored, metadata);
      }
      try (LogListener skipLog =
              LogListener.warn(ResumableInputStream.class).substring("skipping");
          InputStream input = encryptedClient.pullStream(path)) {
        assertEquals(MIB, input.readNBytes(MIB).length);
        initiateS3ConnectionLoss();
        if (tamper) {
          S3Exception e =
              expectThrows(S3Exception.class, () -> input.skip((long) data.length - MIB));
          assertThat(e.getMessage(), containsString("failed authentication"));
        } else {
          assertEquals(data.length - MIB, input.skip((long) data.length - MIB));
          assertEquals(-1, input.read());
        }
        assertNotEquals("the skip should have been resumed", 0, skipLog.getCount());
        skipLog.clearQueue();
      }
    }
  }

  @Test
  public void testSkipOrAbortAbortsOnRuntimeException() {
    AtomicBoolean aborted = new AtomicBoolean();
    InputStream failing =
        new InputStream() {
          @Override
          public int read() {
            throw new IllegalStateException("tag mismatch");
          }

          @Override
          public long skip(long n) {
            throw new IllegalStateException("tag mismatch");
          }
        };
    ResponseInputStream<GetObjectResponse> stream =
        new ResponseInputStream<>(
            GetObjectResponse.builder().build(),
            AbortableInputStream.create(failing, () -> aborted.set(true)));
    expectThrows(IllegalStateException.class, () -> S3StorageClient.skipOrAbort(stream, 10));
    assertTrue(aborted.get());
  }

  @Test
  public void testWrongKey() throws Exception {
    push(encryptedClient, "wrong-key", randomBytes(100));
    S3StorageClient otherKeyClient = newClient(aesEncryption(newAesKey()));
    try {
      S3Exception e = expectThrows(S3Exception.class, () -> otherKeyClient.pullStream("wrong-key"));
      assertThat(e.getMessage(), containsString("different key"));
      assertNotNull(S3StorageClient.findCause(e, AEADBadTagException.class));
    } finally {
      otherKeyClient.close();
    }
  }

  /** A failure to fetch an encrypted object is not reported as a key mismatch. */
  @Test
  public void testReadFailureIsNotReportedAsWrongKey() throws Exception {
    SecretKey key = newAesKey();
    S3StorageClient writer = newClient(aesEncryption(key));
    try {
      push(writer, "unreachable", randomBytes(100));
    } finally {
      writer.close();
    }

    int closedPort;
    try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
      closedPort = socket.getLocalPort();
    }
    S3AsyncClient unreachable =
        S3AsyncClient.builder()
            .endpointOverride(URI.create("http://127.0.0.1:" + closedPort))
            .region(Region.US_EAST_1)
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
            .forcePathStyle(true)
            .overrideConfiguration(b -> b.retryStrategy(AwsRetryStrategy.doNotRetry()))
            .httpClientBuilder(NettyNioAsyncHttpClient.builder())
            .build();
    S3StorageClient reader =
        new S3StorageClient(
            s3MockContainer.createS3ClientV2(),
            unreachable,
            AesKeyring.builder().wrappingKey(key).build(),
            BUCKET_NAME);
    try {
      S3Exception e = expectThrows(S3Exception.class, () -> reader.pullStream("unreachable"));
      assertEquals("Failed to read encrypted S3 object 'unreachable'", e.getMessage());
      assertNotNull(e.getCause());
      assertFalse(e instanceof S3NotFoundException);
    } finally {
      reader.close();
    }
  }

  /** The classification looks through wrappers, but the wrapper stays in the chain. */
  @Test
  public void testHandleAmazonExceptionKeepsWrapper() {
    AwsServiceException denied =
        AwsServiceException.builder()
            .statusCode(403)
            .awsErrorDetails(
                AwsErrorDetails.builder()
                    .serviceName("Sts")
                    .errorCode("AccessDenied")
                    .errorMessage("not authorized")
                    .build())
            .build();
    SdkClientException wrapper = SdkClientException.create("Unable to load credentials", denied);
    S3Exception e = S3StorageClient.handleAmazonException(wrapper);
    assertFalse(e instanceof S3NotFoundException);
    assertSame(wrapper, e.getCause());
    assertThat(e.getMessage(), containsString("AccessDenied"));

    NoSuchKeyException missing =
        NoSuchKeyException.builder()
            .statusCode(404)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("NoSuchKey").build())
            .build();
    SdkClientException wrappedMissing = SdkClientException.create("wrapped", missing);
    e = S3StorageClient.handleAmazonException(wrappedMissing);
    assertTrue(e instanceof S3NotFoundException);
    assertSame(wrappedMissing, e.getCause());

    e = S3StorageClient.handleAmazonException(missing);
    assertTrue(e instanceof S3NotFoundException);
    assertSame(missing, e.getCause());
  }

  @Test
  public void testEncryptedRepositoryRefusesUnencryptedObject() throws Exception {
    pushContent("unencrypted", "plain content");

    for (ThrowingRunnable read :
        new ThrowingRunnable[] {
          () -> encryptedClient.pullStream("unencrypted"),
          () -> encryptedClient.length("unencrypted")
        }) {
      S3Exception e = expectThrows(S3Exception.class, read);
      assertFalse(e instanceof S3NotFoundException);
      assertThat(e.getMessage(), containsString("'unencrypted' is not encrypted"));
      assertThat(e.getMessage(), containsString(S3BackupRepositoryConfig.ENCRYPTION_AES_KEY_FILE));
    }
  }

  @Test
  public void testPlainRepositoryRefusesEncryptedObject() throws Exception {
    push(encryptedClient, "encrypted", randomBytes(100));

    for (ThrowingRunnable read :
        new ThrowingRunnable[] {
          () -> client.pullStream("encrypted"), () -> client.length("encrypted")
        }) {
      S3Exception e = expectThrows(S3Exception.class, read);
      assertFalse(e instanceof S3NotFoundException);
      assertThat(e.getMessage(), containsString("'encrypted' is encrypted"));
      assertThat(e.getMessage(), containsString(S3BackupRepositoryConfig.ENCRYPTION_KMS_KEY_ID));
    }
  }

  @Test
  public void testNotFound() throws Exception {
    expectThrows(S3NotFoundException.class, () -> encryptedClient.pullStream("not-found"));
    expectThrows(S3NotFoundException.class, () -> encryptedClient.length("not-found"));

    push(encryptedClient, "directory/file", randomBytes(10));
    S3Exception e = expectThrows(S3Exception.class, () -> encryptedClient.length("directory"));
    assertThat(e.getMessage(), containsString("Path is Directory"));
  }

  /** Resuming must read the object again from the start, so that it is still authenticated. */
  @Test
  public void testReadWithConnectionLoss() throws Exception {
    String path = "connection-loss";
    byte[] data = randomBytes(40 * MIB);
    push(encryptedClient, path, data);

    try (LogListener resumeLog = LogListener.warn(ResumableInputStream.class)) {
      byte[] read = new byte[data.length];
      try (InputStream input = encryptedClient.pullStream(path)) {
        int offset = input.readNBytes(read, 0, MIB);
        // The rest of the object has not arrived yet, so this breaks the open response
        initiateS3ConnectionLoss();
        offset += input.readNBytes(read, offset, read.length - offset);
        assertEquals(data.length, offset);
        assertEquals(-1, input.read());
      }
      assertArrayEquals(data, read);
      assertNotEquals("the read should have been resumed", 0, resumeLog.getCount());
      resumeLog.clearQueue();
    }
  }

  /** Lucene's footer checks on restore depend on the plaintext length. */
  @Test
  public void testRepositoryBackupAndRestore() throws Exception {
    Path keyFile = createTempFile("aes", ".key");
    Files.writeString(keyFile, Base64.getEncoder().encodeToString(newAesKey().getEncoded()) + "\n");
    NamedList<Object> args = new NamedList<>();
    args.add(S3BackupRepositoryConfig.REGION, Region.US_EAST_1.id());
    args.add(S3BackupRepositoryConfig.BUCKET_NAME, BUCKET_NAME);
    args.add(S3BackupRepositoryConfig.ENDPOINT, s3MockContainer.getHttpEndpoint());
    args.add(S3BackupRepositoryConfig.ENCRYPTION_AES_KEY_FILE, keyFile.toString());

    byte[] content = randomBytes(3 * MIB + 7);
    URI backupDir = new URI("s3:/backup/");
    try (S3BackupRepository repo = new S3BackupRepository();
        Directory source = newFSDirectory(createTempDir("source"));
        Directory dest = newFSDirectory(createTempDir("dest"))) {
      repo.init(args);
      try (IndexOutput output = source.createOutput("file", IOContext.DEFAULT)) {
        CodecUtil.writeHeader(output, "test", 0);
        output.writeBytes(content, content.length);
        CodecUtil.writeFooter(output);
      }
      long fileLength = source.fileLength("file");

      repo.copyIndexFileFrom(source, "file", backupDir, "file");

      try (IndexInput input = repo.openInput(backupDir, "file", IOContext.DEFAULT)) {
        assertEquals(fileLength, input.length());
        CodecUtil.checksumEntireFile(input);
      }

      repo.copyIndexFileTo(backupDir, "file", dest, "restored");
      try (IndexInput restored = dest.openInput("restored", IOContext.READONCE)) {
        assertEquals(fileLength, restored.length());
        CodecUtil.checksumEntireFile(restored);
      }
    }
  }

  /** The KMS keyring is driven through a stubbed KMS client. */
  @Test
  public void testKmsKeyring() throws Exception {
    assumeWorkingMockito();
    String keyId = "alias/solr-backup";
    SdkBytes dataKey = SdkBytes.fromByteArray(newAesKey().getEncoded());
    SdkBytes wrappedDataKey = SdkBytes.fromUtf8String("wrapped-data-key");
    KmsClient kms = mock(KmsClient.class);
    when(kms.generateDataKey(any(GenerateDataKeyRequest.class)))
        .thenReturn(
            GenerateDataKeyResponse.builder()
                .keyId(keyId)
                .plaintext(dataKey)
                .ciphertextBlob(wrappedDataKey)
                .build());
    when(kms.decrypt(any(DecryptRequest.class)))
        .thenReturn(DecryptResponse.builder().keyId(keyId).plaintext(dataKey).build());

    URI endpoint = URI.create(s3MockContainer.getHttpEndpoint());
    StaticCredentialsProvider credentials =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar"));
    S3Client s3 = s3MockContainer.createS3ClientV2();
    S3AsyncClient s3Async =
        S3AsyncClient.builder()
            .endpointOverride(endpoint)
            .region(Region.US_EAST_1)
            .credentialsProvider(credentials)
            .forcePathStyle(true)
            .httpClientBuilder(NettyNioAsyncHttpClient.builder())
            .build();
    S3StorageClient kmsClient =
        new S3StorageClient(
            s3,
            s3Async,
            KmsKeyring.builder().kmsClient(kms).wrappingKeyId(keyId).build(),
            BUCKET_NAME);
    try {
      byte[] data = randomBytes(1000);
      push(kmsClient, "kms", data);
      assertArrayEquals(data, pull(kmsClient, "kms"));
    } finally {
      kmsClient.close();
    }

    ArgumentCaptor<GenerateDataKeyRequest> generated =
        ArgumentCaptor.forClass(GenerateDataKeyRequest.class);
    verify(kms).generateDataKey(generated.capture());
    assertEquals(keyId, generated.getValue().keyId());
    ArgumentCaptor<DecryptRequest> decrypted = ArgumentCaptor.forClass(DecryptRequest.class);
    verify(kms).decrypt(decrypted.capture());
    assertEquals(keyId, decrypted.getValue().keyId());
    assertEquals(wrappedDataKey, decrypted.getValue().ciphertextBlob());
  }

  /**
   * The KMS client built from {@code s3.encryption.kms.*}, against a local stub of KMS: it sends
   * the configured key id, to the configured endpoint, signed for the KMS region (or the S3 region
   * by default) with the configured credentials.
   */
  @Test
  public void testKmsConfiguration() throws Exception {
    String keyId = "alias/solr-backup";
    try (KmsStub kms = new KmsStub(newAesKey().getEncoded())) {
      for (String kmsRegion : new String[] {null, "eu-west-1"}) {
        kms.requests.clear();
        NamedList<Object> args = new NamedList<>();
        args.add(S3BackupRepositoryConfig.REGION, Region.US_EAST_1.id());
        args.add(S3BackupRepositoryConfig.BUCKET_NAME, BUCKET_NAME);
        args.add(S3BackupRepositoryConfig.ENDPOINT, s3MockContainer.getHttpEndpoint());
        args.add(S3BackupRepositoryConfig.ENCRYPTION_KMS_KEY_ID, keyId);
        args.add(S3BackupRepositoryConfig.ENCRYPTION_KMS_ENDPOINT, kms.endpoint());
        if (kmsRegion != null) {
          args.add(S3BackupRepositoryConfig.ENCRYPTION_KMS_REGION, kmsRegion);
        }
        S3StorageClient kmsClient = new S3BackupRepositoryConfig(args).buildClient();
        try {
          byte[] data = randomBytes(1000);
          push(kmsClient, "kms-config", data);
          assertArrayEquals(data, pull(kmsClient, "kms-config"));

          kms.failDecrypt = true;
          S3Exception e = expectThrows(S3Exception.class, () -> kmsClient.pullStream("kms-config"));
          assertThat(e.getMessage(), containsString("IncorrectKeyException"));
          assertThat(e.getMessage(), not(containsString("different key")));
          kms.failDecrypt = false;
        } finally {
          kmsClient.close();
        }

        String scope =
            "/" + (kmsRegion != null ? kmsRegion : Region.US_EAST_1.id()) + "/kms/aws4_request";
        List<String> targets = kms.requests.stream().map(KmsRequest::target).toList();
        assertEquals(
            List.of("TrentService.GenerateDataKey", "TrentService.Decrypt", "TrentService.Decrypt"),
            targets);
        for (KmsRequest request : kms.requests) {
          assertThat(request.body(), containsString("\"KeyId\":\"" + keyId + "\""));
          assertThat(request.authorization(), containsString("Credential=foo/"));
          assertThat(request.authorization(), containsString(scope));
        }
      }
    }
  }

  @Test
  public void testNettyProxyConfiguration() {
    ProxyConfiguration proxy =
        S3StorageClient.createNettyProxyConfiguration("http://proxy.invalid:3128", true);
    assertEquals("http", proxy.scheme());
    assertEquals("proxy.invalid", proxy.host());
    assertEquals(3128, proxy.port());

    proxy = S3StorageClient.createNettyProxyConfiguration("http://proxy.invalid", false);
    assertEquals(80, proxy.port());

    proxy = S3StorageClient.createNettyProxyConfiguration("https://proxy.invalid", false);
    assertEquals("https", proxy.scheme());
    assertEquals(443, proxy.port());

    proxy = S3StorageClient.createNettyProxyConfiguration("", false);
    assertNull(proxy.host());
  }

  /** Encrypted content is fetched through the Netty client, which must use the configured proxy. */
  @Test
  public void testEncryptedReadUsesProxy() throws Exception {
    CompletableFuture<String> connect = new CompletableFuture<>();
    ExecutorService executor =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("proxy-stub"));
    try (ServerSocket proxySocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
      executor.execute(
          () -> {
            while (!proxySocket.isClosed()) {
              try (Socket socket = proxySocket.accept()) {
                String requestLine =
                    new BufferedReader(
                            new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
                        .readLine();
                if (requestLine != null && requestLine.startsWith("CONNECT ")) {
                  connect.complete(requestLine);
                }
                socket
                    .getOutputStream()
                    .write(
                        "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .getBytes(StandardCharsets.UTF_8));
              } catch (IOException e) {
                // the server socket was closed, or the client went away
              }
            }
          });
      URI endpoint = URI.create(s3MockContainer.getHttpEndpoint());
      S3StorageClient proxied =
          new S3StorageClient(
              BUCKET_NAME,
              null,
              Region.US_EAST_1.id(),
              "http://127.0.0.1:" + proxySocket.getLocalPort(),
              false,
              endpoint.toString(),
              true,
              aesEncryption(newAesKey()));
      try {
        expectThrows(S3Exception.class, () -> proxied.pullStream("through-proxy"));
      } finally {
        proxied.close();
      }
      assertEquals(
          "CONNECT " + endpoint.getHost() + ":" + endpoint.getPort() + " HTTP/1.1",
          connect.get(PROXY_REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    } finally {
      ExecutorUtil.shutdownAndAwaitTermination(executor);
    }
  }

  private record KmsRequest(String target, String authorization, String body) {}

  /** Answers KMS GenerateDataKey and Decrypt with a fixed data key, recording the requests. */
  private static class KmsStub implements AutoCloseable {
    final List<KmsRequest> requests = new CopyOnWriteArrayList<>();
    volatile boolean failDecrypt;
    private final String plaintext;
    private final String ciphertext =
        Base64.getEncoder().encodeToString("wrapped".getBytes(StandardCharsets.UTF_8));
    private final ServerSocket serverSocket;
    private final ExecutorService executor =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("kms-stub"));

    KmsStub(byte[] dataKey) throws IOException {
      plaintext = Base64.getEncoder().encodeToString(dataKey);
      serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
      executor.execute(
          () -> {
            while (!serverSocket.isClosed()) {
              try (Socket socket = serverSocket.accept()) {
                handle(socket);
              } catch (IOException e) {
                // the server socket was closed, or the client went away
              }
            }
          });
    }

    /** Answers one request and closes the connection. */
    private void handle(Socket socket) throws IOException {
      InputStream in = socket.getInputStream();
      Map<String, String> headers = new HashMap<>();
      String line = readLine(in);
      while (!(line = readLine(in)).isEmpty()) {
        int colon = line.indexOf(':');
        headers.put(
            line.substring(0, colon).trim().toLowerCase(Locale.ROOT),
            line.substring(colon + 1).trim());
      }
      String body =
          new String(
              in.readNBytes(Integer.parseInt(headers.getOrDefault("content-length", "0"))),
              StandardCharsets.UTF_8);
      String target = headers.get("x-amz-target");
      requests.add(new KmsRequest(target, headers.get("authorization"), body));

      String keyIdJson = body.replaceAll("(?s).*(\"KeyId\":\"[^\"]*\").*", "$1");
      String status = "200 OK";
      String response;
      if ("TrentService.GenerateDataKey".equals(target)) {
        response =
            "{"
                + keyIdJson
                + ",\"Plaintext\":\""
                + plaintext
                + "\",\"CiphertextBlob\":\""
                + ciphertext
                + "\"}";
      } else if (failDecrypt) {
        status = "400 Bad Request";
        response = "{\"__type\":\"IncorrectKeyException\",\"message\":\"wrong KMS key\"}";
      } else {
        response = "{" + keyIdJson + ",\"Plaintext\":\"" + plaintext + "\"}";
      }
      byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
      OutputStream out = socket.getOutputStream();
      out.write(
          ("HTTP/1.1 "
                  + status
                  + "\r\nContent-Type: application/x-amz-json-1.1\r\nContent-Length: "
                  + bytes.length
                  + "\r\nConnection: close\r\n\r\n")
              .getBytes(StandardCharsets.UTF_8));
      out.write(bytes);
      out.flush();
    }

    private static String readLine(InputStream in) throws IOException {
      StringBuilder line = new StringBuilder();
      int c;
      while ((c = in.read()) != -1 && c != '\n') {
        if (c != '\r') {
          line.append((char) c);
        }
      }
      return line.toString();
    }

    String endpoint() {
      return "http://127.0.0.1:" + serverSocket.getLocalPort();
    }

    @Override
    public void close() throws IOException {
      serverSocket.close();
      ExecutorUtil.shutdownAndAwaitTermination(executor);
    }
  }

  private static EncryptionConfig aesEncryption(SecretKey key) {
    return new EncryptionConfig(key, null, null, null);
  }

  private static SecretKey newAesKey() {
    byte[] key = new byte[S3BackupRepositoryConfig.AES_KEY_LENGTH_BYTES];
    random().nextBytes(key);
    return new SecretKeySpec(key, "AES");
  }

  private static byte[] randomBytes(int size) {
    byte[] bytes = new byte[size];
    random().nextBytes(bytes);
    return bytes;
  }

  private static void push(S3StorageClient storageClient, String path, byte[] data)
      throws IOException {
    try (OutputStream output = storageClient.pushStream(path)) {
      output.write(data);
    }
  }

  private static byte[] pull(S3StorageClient storageClient, String path) throws IOException {
    try (InputStream input = storageClient.pullStream(path)) {
      return input.readAllBytes();
    }
  }

  /** Writes {@code size} bytes, generated one MiB block at a time so they need not be kept. */
  private static void pushGenerated(S3StorageClient storageClient, String path, long size)
      throws IOException {
    try (OutputStream output = storageClient.pushStream(path)) {
      for (long block = 0; block * MIB < size; block++) {
        output.write(generatedBlock(block, size));
      }
    }
  }

  private static void assertGenerated(InputStream input, long size) throws IOException {
    for (long block = 0; block * MIB < size; block++) {
      byte[] expected = generatedBlock(block, size);
      byte[] actual = input.readNBytes(expected.length);
      if (!Arrays.equals(expected, actual)) {
        fail("content differs in MiB block " + block);
      }
    }
    assertEquals("expected end of stream", -1, input.read());
  }

  private static byte[] generatedBlock(long block, long size) {
    byte[] bytes = new byte[(int) Math.min(MIB, size - block * MIB)];
    new Random(block).nextBytes(bytes);
    return bytes;
  }

  private static byte[] rawObject(String path) throws IOException {
    try (S3Client s3 = s3MockContainer.createS3ClientV2();
        InputStream input = s3.getObject(b -> b.bucket(BUCKET_NAME).key(path))) {
      return input.readAllBytes();
    }
  }

  private static Map<String, String> rawMetadata(String path) {
    try (S3Client s3 = s3MockContainer.createS3ClientV2()) {
      return s3.headObject(b -> b.bucket(BUCKET_NAME).key(path)).metadata();
    }
  }

  private static void rawPut(String path, byte[] data, Map<String, String> metadata) {
    try (S3Client s3 = s3MockContainer.createS3ClientV2()) {
      s3.putObject(
          b -> b.bucket(BUCKET_NAME).key(path).metadata(metadata), RequestBody.fromBytes(data));
    }
  }
}
