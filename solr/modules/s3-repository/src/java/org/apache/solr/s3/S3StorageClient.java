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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.crypto.AEADBadTagException;
import org.apache.solr.common.util.ResumableInputStream;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.s3.S3BackupRepositoryConfig.EncryptionConfig;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.awscore.retry.AwsRetryStrategy;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.KmsClientBuilder;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.encryption.s3.S3EncryptionClient;
import software.amazon.encryption.s3.S3EncryptionClientSecurityException;
import software.amazon.encryption.s3.internal.InstructionFileConfig;
import software.amazon.encryption.s3.materials.AesKeyring;
import software.amazon.encryption.s3.materials.Keyring;
import software.amazon.encryption.s3.materials.KmsKeyring;

/**
 * Creates a {@link S3Client} for communicating with AWS S3. Utilizes the default credential
 * provider chain; reference <a
 * href="https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html">AWS SDK
 * docs</a> for details on where this client will fetch credentials from, and the order of
 * precedence.
 *
 * <p>With client-side encryption configured, object content is read and written through an {@link
 * S3EncryptionClient}, while listing, metadata and deletes still use the plain client. Such a
 * client refuses to read objects that are not encrypted, and a client without encryption refuses to
 * read objects that are.
 */
public class S3StorageClient {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String S3_FILE_PATH_DELIMITER = "/";

  // S3 has a hard limit of 1000 keys per batch delete request
  private static final int MAX_KEYS_PER_BATCH_DELETE = 1000;

  // Error messages returned by S3 for a key not found.
  private static final Set<String> NOT_FOUND_CODES = Set.of("NoSuchKey", "404 Not Found");

  /** Bytes the AES-GCM authentication tag adds to the stored length of an encrypted object. */
  static final int GCM_TAG_LENGTH = 16;

  /**
   * User metadata keys of which at least one is present on any object written by an S3 encryption
   * client, whatever the format version.
   */
  private static final Set<String> ENCRYPTION_METADATA_KEYS =
      Set.of("x-amz-key", "x-amz-key-v2", "x-amz-3", "x-amz-c");

  /** Used for listing, object metadata and deletes. */
  private final S3Client s3Client;

  /** Used for object content; the same as {@link #s3Client} unless encryption is configured. */
  private final S3Client contentReadClient;

  private final S3Client contentWriteClient;
  private final boolean encrypted;
  private final List<AutoCloseable> ownedClients;

  /** The S3 bucket where we read/write all data. */
  private final String bucketName;

  /**
   * @param encryption client-side encryption settings, or {@code null} for none
   */
  S3StorageClient(
      String bucketName,
      String profile,
      String region,
      String proxyUrl,
      boolean proxyUseSystemSettings,
      String endpoint,
      boolean disableRetries,
      EncryptionConfig encryption) {
    this(
        createClients(
            profile,
            region,
            proxyUrl,
            proxyUseSystemSettings,
            endpoint,
            disableRetries,
            encryption),
        bucketName);
  }

  @VisibleForTesting
  S3StorageClient(S3Client s3Client, String bucketName) {
    this(new Clients(s3Client, s3Client, s3Client, false, List.of(s3Client)), bucketName);
  }

  /**
   * An encrypting client over the given clients and keyring. Closing it closes both clients, but
   * not anything held by the keyring.
   */
  @VisibleForTesting
  S3StorageClient(
      S3Client s3Client, S3AsyncClient s3AsyncClient, Keyring keyring, String bucketName) {
    this(
        encryptedClients(s3Client, s3AsyncClient, keyring, List.of(s3Client, s3AsyncClient)),
        bucketName);
  }

  private S3StorageClient(Clients clients, String bucketName) {
    this.s3Client = clients.plain();
    this.contentReadClient = clients.read();
    this.contentWriteClient = clients.write();
    this.encrypted = clients.encrypted();
    this.ownedClients = clients.owned();
    this.bucketName = bucketName;
  }

  private record Clients(
      S3Client plain,
      S3Client read,
      S3Client write,
      boolean encrypted,
      List<AutoCloseable> owned) {}

  private static Clients createClients(
      String profile,
      String region,
      String proxyUrl,
      boolean proxyUseSystemSettings,
      String endpoint,
      boolean disableRetries,
      EncryptionConfig encryption) {
    AwsCredentialsProvider credentialsProvider = createCredentialsProvider(profile);
    S3Client s3Client =
        createInternalClient(
            profile,
            region,
            proxyUrl,
            proxyUseSystemSettings,
            endpoint,
            disableRetries,
            credentialsProvider);
    if (encryption == null) {
      return new Clients(s3Client, s3Client, s3Client, false, List.of(s3Client));
    }

    List<AutoCloseable> owned = new ArrayList<>();
    owned.add(s3Client);
    try {
      S3AsyncClient s3AsyncClient =
          createAsyncClient(
              profile,
              region,
              proxyUrl,
              proxyUseSystemSettings,
              endpoint,
              disableRetries,
              credentialsProvider);
      owned.add(s3AsyncClient);
      Keyring keyring;
      if (encryption.aesKey() != null) {
        keyring = AesKeyring.builder().wrappingKey(encryption.aesKey()).build();
      } else {
        KmsClient kmsClient =
            createKmsClient(
                profile,
                encryption.kmsRegion() != null ? encryption.kmsRegion() : region,
                proxyUrl,
                proxyUseSystemSettings,
                encryption.kmsEndpoint(),
                disableRetries,
                credentialsProvider);
        owned.add(kmsClient);
        keyring =
            KmsKeyring.builder().kmsClient(kmsClient).wrappingKeyId(encryption.kmsKeyId()).build();
      }
      return encryptedClients(s3Client, s3AsyncClient, keyring, owned);
    } catch (RuntimeException e) {
      closeAll(owned);
      throw e;
    }
  }

  /**
   * Writes use the JDK's crypto provider, which is fast. Reads use BouncyCastle, because the JDK's
   * AES-GCM releases no plaintext until the whole object has been decrypted, which would hold the
   * object in memory; BouncyCastle releases it as it goes. Both clients wrap the same S3 clients,
   * and are not closed themselves since that only closes the wrapped clients.
   */
  private static Clients encryptedClients(
      S3Client s3Client, S3AsyncClient s3AsyncClient, Keyring keyring, List<AutoCloseable> owned) {
    S3EncryptionClient writeClient =
        encryptionClientBuilder(s3Client, s3AsyncClient, keyring).build();
    S3EncryptionClient readClient =
        encryptionClientBuilder(s3Client, s3AsyncClient, keyring)
            .cryptoProvider(new BouncyCastleProvider())
            // Releases plaintext before the tag is checked; required for objects over 64 MiB
            .enableDelayedAuthenticationMode(true)
            .build();
    return new Clients(s3Client, readClient, writeClient, true, List.copyOf(owned));
  }

  private static S3EncryptionClient.Builder encryptionClientBuilder(
      S3Client s3Client, S3AsyncClient s3AsyncClient, Keyring keyring) {
    return S3EncryptionClient.builderV4()
        .wrappedClient(s3Client)
        .wrappedAsyncClient(s3AsyncClient)
        .keyring(keyring)
        .instructionFileConfig(
            InstructionFileConfig.builder().disableInstructionFile(true).build());
  }

  private static S3Client createInternalClient(
      String profile,
      String region,
      String proxyUrl,
      boolean proxyUseSystemSettings,
      String endpoint,
      boolean disableRetries,
      AwsCredentialsProvider credentialsProvider) {
    /*
     * Default s3 client builder loads credentials from disk and handles token refreshes
     */
    S3ClientBuilder clientBuilder =
        S3Client.builder()
            .credentialsProvider(credentialsProvider)
            .overrideConfiguration(
                builder -> builder.retryStrategy(createRetryStrategy(profile, disableRetries)))
            .serviceConfiguration(createS3Configuration(profile))
            .httpClient(createApacheHttpClientBuilder(proxyUrl, proxyUseSystemSettings).build());

    if (StrUtils.isNotNullOrEmpty(endpoint)) {
      clientBuilder.endpointOverride(URI.create(endpoint));
    }
    if (StrUtils.isNotNullOrEmpty(region)) {
      clientBuilder.region(Region.of(region));
    }

    return clientBuilder.build();
  }

  /** The encryption client sends object content through an async client. */
  private static S3AsyncClient createAsyncClient(
      String profile,
      String region,
      String proxyUrl,
      boolean proxyUseSystemSettings,
      String endpoint,
      boolean disableRetries,
      AwsCredentialsProvider credentialsProvider) {
    S3AsyncClientBuilder clientBuilder =
        S3AsyncClient.builder()
            .credentialsProvider(credentialsProvider)
            .overrideConfiguration(
                builder -> builder.retryStrategy(createRetryStrategy(profile, disableRetries)))
            .serviceConfiguration(createS3Configuration(profile))
            // A builder rather than a client, so that closing the S3 client closes it
            .httpClientBuilder(
                NettyNioAsyncHttpClient.builder()
                    .proxyConfiguration(
                        createNettyProxyConfiguration(proxyUrl, proxyUseSystemSettings)));

    if (StrUtils.isNotNullOrEmpty(endpoint)) {
      clientBuilder.endpointOverride(URI.create(endpoint));
    }
    if (StrUtils.isNotNullOrEmpty(region)) {
      clientBuilder.region(Region.of(region));
    }

    return clientBuilder.build();
  }

  /**
   * Netty's proxy configuration has no {@code endpoint(URI)}, so the URL is split into scheme, host
   * and port, defaulting the port from the scheme. It shares its name with Apache's.
   */
  @VisibleForTesting
  static software.amazon.awssdk.http.nio.netty.ProxyConfiguration createNettyProxyConfiguration(
      String proxyUrl, boolean proxyUseSystemSettings) {
    software.amazon.awssdk.http.nio.netty.ProxyConfiguration.Builder proxyConfigurationBuilder =
        software.amazon.awssdk.http.nio.netty.ProxyConfiguration.builder();
    if (StrUtils.isNotNullOrEmpty(proxyUrl)) {
      URI proxyUri = URI.create(proxyUrl);
      int port = proxyUri.getPort();
      if (port == -1) {
        port = "https".equalsIgnoreCase(proxyUri.getScheme()) ? 443 : 80;
      }
      proxyConfigurationBuilder.scheme(proxyUri.getScheme()).host(proxyUri.getHost()).port(port);
    } else {
      proxyConfigurationBuilder.useSystemPropertyValues(proxyUseSystemSettings);
    }
    return proxyConfigurationBuilder.build();
  }

  private static KmsClient createKmsClient(
      String profile,
      String region,
      String proxyUrl,
      boolean proxyUseSystemSettings,
      String endpoint,
      boolean disableRetries,
      AwsCredentialsProvider credentialsProvider) {
    KmsClientBuilder clientBuilder =
        KmsClient.builder()
            .credentialsProvider(credentialsProvider)
            .overrideConfiguration(
                builder -> builder.retryStrategy(createRetryStrategy(profile, disableRetries)))
            .httpClientBuilder(createApacheHttpClientBuilder(proxyUrl, proxyUseSystemSettings));

    if (StrUtils.isNotNullOrEmpty(endpoint)) {
      clientBuilder.endpointOverride(URI.create(endpoint));
    }
    if (StrUtils.isNotNullOrEmpty(region)) {
      clientBuilder.region(Region.of(region));
    }

    return clientBuilder.build();
  }

  private static S3Configuration createS3Configuration(String profile) {
    S3Configuration.Builder configBuilder = S3Configuration.builder().pathStyleAccessEnabled(true);
    if (StrUtils.isNotNullOrEmpty(profile)) {
      configBuilder.profileName(profile);
    }
    return configBuilder.build();
  }

  private static ApacheHttpClient.Builder createApacheHttpClientBuilder(
      String proxyUrl, boolean proxyUseSystemSettings) {
    ApacheHttpClient.Builder sdkHttpClientBuilder = ApacheHttpClient.builder();
    // If configured, add proxy
    ProxyConfiguration.Builder proxyConfigurationBuilder = ProxyConfiguration.builder();
    if (StrUtils.isNotNullOrEmpty(proxyUrl)) {
      proxyConfigurationBuilder.endpoint(URI.create(proxyUrl));
    } else {
      proxyConfigurationBuilder.useSystemPropertyValues(proxyUseSystemSettings);
    }
    sdkHttpClientBuilder.proxyConfiguration(proxyConfigurationBuilder.build());
    sdkHttpClientBuilder.useIdleConnectionReaper(false);
    return sdkHttpClientBuilder;
  }

  /** Each client gets its own instance, since a retry strategy keeps state. */
  private static RetryStrategy createRetryStrategy(String profile, boolean disableRetries) {
    if (disableRetries) {
      return AwsRetryStrategy.doNotRetry();
    }
    RetryMode.Resolver retryModeResolver = RetryMode.resolver();
    if (StrUtils.isNotNullOrEmpty(profile)) {
      retryModeResolver.profileName(profile);
    }
    return AwsRetryStrategy.forRetryMode(retryModeResolver.resolve());
  }

  private static AwsCredentialsProvider createCredentialsProvider(String profile) {
    DefaultCredentialsProvider.Builder credentialsProviderBuilder =
        DefaultCredentialsProvider.builder();
    if (StrUtils.isNotNullOrEmpty(profile)) {
      credentialsProviderBuilder.profileName(profile);
    }
    return credentialsProviderBuilder.build();
  }

  /**
   * No-op: S3 has no directories, a "directory" is just a key prefix that exists as soon as an
   * object is written under it. Older Solr versions wrote zero-byte marker objects here, but those
   * get lost when a bucket is copied with tools such as {@code aws s3 sync}, so nothing may depend
   * on them.
   */
  void createDirectory(String path) {}

  /**
   * Delete files from S3. Deletion order is not guaranteed.
   *
   * @throws S3NotFoundException if the number of deleted objects does not match {@code entries}
   *     size
   */
  void delete(Collection<String> paths) throws S3Exception {
    Set<String> entries = new HashSet<>();
    for (String path : paths) {
      entries.add(sanitizedFilePath(path));
    }

    Collection<String> deletedPaths = deleteObjects(entries);

    // If we haven't deleted all requested objects, assume that's because some were missing
    if (entries.size() != deletedPaths.size()) {
      Set<String> notDeletedPaths = new HashSet<>(entries);
      entries.removeAll(deletedPaths);
      throw new S3NotFoundException(notDeletedPaths.toString());
    }
  }

  /**
   * Delete directory, all the files and subdirectories from S3.
   *
   * @param path Path to directory in S3.
   */
  void deleteDirectory(String path) throws S3Exception {
    path = sanitizedDirPath(path);

    // Get all the files and subdirectories
    Set<String> entries = listAll(path);

    deleteObjects(entries);
  }

  /**
   * List all the files and subdirectories directly under given path.
   *
   * @param path Path to directory in S3.
   * @return Files and subdirectories in path.
   */
  String[] listDir(String path) throws S3Exception {
    path = sanitizedDirPath(path);

    final String prefix = path;

    try {
      ListObjectsV2Iterable objectListing =
          s3Client.listObjectsV2Paginator(
              builder ->
                  builder
                      .bucket(bucketName)
                      .prefix(prefix)
                      .delimiter(S3_FILE_PATH_DELIMITER)
                      .build());

      return Stream.concat(
              objectListing.contents().stream().map(S3Object::key),
              objectListing.commonPrefixes().stream().map(CommonPrefix::prefix))
          .filter(s -> s.startsWith(prefix))
          .map(s -> s.substring(prefix.length()))
          .filter(s -> !s.isEmpty())
          .filter(
              s -> {
                int slashIndex = s.indexOf(S3_FILE_PATH_DELIMITER);
                return slashIndex == -1 || slashIndex == s.length() - 1;
              })
          .map(
              s -> {
                if (s.endsWith(S3_FILE_PATH_DELIMITER)) {
                  return s.substring(0, s.length() - 1);
                }
                return s;
              })
          .toArray(String[]::new);
    } catch (SdkException sdke) {
      throw handleAmazonException(sdke);
    }
  }

  /**
   * Check if path exists. A directory path (ending with {@code /}) exists if any object is stored
   * under that prefix, including a directory marker object written by older Solr versions.
   *
   * @param path to File/Directory in S3.
   * @return true if path exists, otherwise false
   */
  boolean pathExists(String path) throws S3Exception {
    final String s3Path = sanitizedPath(path);

    // for root return true
    if (s3Path.isEmpty() || S3_FILE_PATH_DELIMITER.equals(s3Path)) {
      return true;
    }

    if (s3Path.endsWith(S3_FILE_PATH_DELIMITER)) {
      return hasObjectsUnderPrefix(s3Path);
    }

    try {
      s3Client.headObject(builder -> builder.bucket(bucketName).key(s3Path));
      return true;
    } catch (NoSuchKeyException e) {
      return false;
    } catch (SdkException sdke) {
      throw handleAmazonException(sdke);
    }
  }

  /**
   * Check if path is directory, i.e. if any object is stored under the {@code path + "/"} prefix.
   *
   * @param path to File/Directory in S3.
   * @return true if path is directory, otherwise false.
   */
  boolean isDirectory(String path) throws S3Exception {
    return hasObjectsUnderPrefix(sanitizedDirPath(path));
  }

  private boolean hasObjectsUnderPrefix(String prefix) throws S3Exception {
    try {
      // listObjectsV2's prefix already guarantees every returned key starts with it.
      return !s3Client
          .listObjectsV2(b -> b.bucket(bucketName).prefix(prefix).maxKeys(1))
          .contents()
          .isEmpty();
    } catch (SdkException sdke) {
      throw handleAmazonException(sdke);
    }
  }

  /**
   * Get length of file in bytes.
   *
   * @param path to file in S3.
   * @return length of file.
   */
  long length(String path) throws S3Exception {
    String s3Path = sanitizedFilePath(path);
    try {
      // sanitizedFilePath rejects a trailing '/', so a successful HEAD here can never land on a
      // directory marker object (whose key always ends in '/'); it's always a real file.
      HeadObjectResponse objectMetadata =
          s3Client.headObject(b -> b.bucket(bucketName).key(s3Path));
      boolean objectEncrypted = isClientSideEncrypted(objectMetadata.metadata());
      if (objectEncrypted != encrypted) {
        throw new S3Exception(encryptionMismatchMessage(s3Path, objectEncrypted));
      }
      return objectEncrypted
          ? objectMetadata.contentLength() - GCM_TAG_LENGTH
          : objectMetadata.contentLength();
    } catch (NoSuchKeyException nske) {
      // Only pay for the extra listing when the file is missing
      if (isDirectory(s3Path)) {
        throw new S3Exception("Path is Directory");
      }
      throw handleAmazonException(nske);
    } catch (SdkException sdke) {
      throw handleAmazonException(sdke);
    }
  }

  /**
   * Open a new {@link InputStream} to file for read. Caller needs to close the stream.
   *
   * @param path to file in S3.
   * @return InputStream for file.
   */
  InputStream pullStream(String path) throws S3Exception {
    final String s3Path = sanitizedFilePath(path);
    if (encrypted) {
      return pullEncryptedStream(s3Path);
    }

    try {
      GetObjectRequest.Builder getBuilder =
          GetObjectRequest.builder().bucket(bucketName).key(s3Path);
      // This InputStream instance needs to be closed by the caller
      // Use Duration.ZERO to disable timeout and prevent response-input-stream-timeout-scheduler
      // thread leak (see https://github.com/aws/aws-sdk-java-v2/issues/6567)
      ResponseInputStream<GetObjectResponse> responseStream =
          s3Client.getObject(getBuilder.build(), ResponseTransformer.toInputStream(Duration.ZERO));
      if (isClientSideEncrypted(responseStream.response().metadata())) {
        responseStream.abort();
        throw new S3Exception(encryptionMismatchMessage(s3Path, true));
      }
      final long contentLength = responseStream.response().contentLength();
      return new ResumableInputStream(
          responseStream,
          bytesRead -> {
            if (contentLength > 0 && bytesRead >= contentLength) {
              // No more bytes to read
              return null;
            } else if (bytesRead > 0) {
              getBuilder.range(String.format(Locale.ROOT, "bytes=%d-", bytesRead));
            }
            // Use Duration.ZERO to disable timeout on resumed streams as well
            return s3Client.getObject(
                getBuilder.build(), ResponseTransformer.toInputStream(Duration.ZERO));
          });
    } catch (SdkException sdke) {
      throw handleAmazonException(sdke);
    }
  }

  /**
   * Opens an object through the encryption client. A dropped connection is resumed by reading the
   * object again from the start, since a ranged read cannot be authenticated.
   */
  private InputStream pullEncryptedStream(String s3Path) throws S3Exception {
    GetObjectRequest request = GetObjectRequest.builder().bucket(bucketName).key(s3Path).build();
    ResponseInputStream<GetObjectResponse> responseStream;
    try {
      // The outer stream needs no timeout; the one the encryption client creates inside it does
      // have one, which only aborts the stream if it is not read within 60s of being opened.
      responseStream =
          contentReadClient.getObject(request, ResponseTransformer.toInputStream(Duration.ZERO));
    } catch (SdkException sdke) {
      throw handleEncryptedReadException(s3Path, sdke);
    }
    final long length = responseStream.response().contentLength() - GCM_TAG_LENGTH;
    // Runtime exceptions from decryption must reach S3EncryptedInputStream without being retried.
    // Even at the end of the object, a resumed read goes through it again, so that the end is
    // still authenticated; skipping past the end fails.
    InputStream resumable =
        new ResumableInputStream(
            responseStream,
            bytesRead ->
                skipOrAbort(
                    contentReadClient.getObject(
                        request, ResponseTransformer.toInputStream(Duration.ZERO)),
                    bytesRead));
    return new S3EncryptedInputStream(resumable, s3Path, length);
  }

  /** Skips {@code n} bytes of the stream, aborting it if that fails. */
  @VisibleForTesting
  static <T extends InputStream & Abortable> T skipOrAbort(T stream, long n) {
    try {
      stream.skipNBytes(n);
    } catch (IOException e) {
      stream.abort();
      throw new UncheckedIOException(e);
    } catch (RuntimeException e) {
      stream.abort();
      throw e;
    }
    return stream;
  }

  /**
   * The encryption client wraps every failure of GetObject, including a missing key, and an object
   * without encryption metadata fails like an object encrypted with a different key.
   */
  private S3Exception handleEncryptedReadException(String s3Path, SdkException sdke) {
    if (findCause(sdke, AwsServiceException.class) != null) {
      return handleAmazonException(sdke);
    }
    HeadObjectResponse objectMetadata;
    try {
      objectMetadata = s3Client.headObject(b -> b.bucket(bucketName).key(s3Path));
    } catch (SdkException headException) {
      S3Exception e = handleAmazonException(headException);
      e.addSuppressed(sdke);
      return e;
    }
    if (!isClientSideEncrypted(objectMetadata.metadata())) {
      return new S3Exception(encryptionMismatchMessage(s3Path, false));
    }
    if (isAuthenticationFailure(sdke)) {
      return new S3Exception(
          "Unable to decrypt S3 object '"
              + s3Path
              + "'; it may have been written with a different key than the one configured",
          sdke);
    }
    return new S3Exception("Failed to read encrypted S3 object '" + s3Path + "'", sdke);
  }

  /**
   * Whether decryption failed a cryptographic check: a wrong key fails to unwrap the data key, and
   * modified content fails its tag or key commitment check.
   */
  static boolean isAuthenticationFailure(Throwable t) {
    return findCause(t, AEADBadTagException.class) != null
        || findCause(t, S3EncryptionClientSecurityException.class) != null;
  }

  static boolean isClientSideEncrypted(Map<String, String> userMetadata) {
    return userMetadata != null
        && ENCRYPTION_METADATA_KEYS.stream().anyMatch(userMetadata::containsKey);
  }

  private static String encryptionMismatchMessage(String s3Path, boolean objectEncrypted) {
    String keyOptions =
        S3BackupRepositoryConfig.ENCRYPTION_KMS_KEY_ID
            + " or "
            + S3BackupRepositoryConfig.ENCRYPTION_AES_KEY_FILE;
    if (objectEncrypted) {
      return "S3 object '"
          + s3Path
          + "' is encrypted, but this repository has no client-side encryption configured."
          + " Set "
          + keyOptions
          + " to the key it was written with.";
    }
    return "S3 object '"
        + s3Path
        + "' is not encrypted, but this repository is configured for client-side encryption"
        + " and only reads encrypted objects. Read it with a repository that does not set "
        + keyOptions
        + ".";
  }

  /**
   * Open a new {@link OutputStream} to file for write. Caller needs to close the stream.
   *
   * @param path to file in S3.
   * @return OutputStream for file.
   */
  OutputStream pushStream(String path) throws S3Exception {
    path = sanitizedFilePath(path);

    try {
      return new S3OutputStream(contentWriteClient, path, bucketName);
    } catch (SdkException sdke) {
      throw handleAmazonException(sdke);
    }
  }

  /** Override {@link Closeable} since we throw no exception. */
  void close() {
    closeAll(ownedClients);
  }

  private static void closeAll(List<AutoCloseable> clients) {
    for (AutoCloseable client : clients) {
      try {
        client.close();
      } catch (Exception e) {
        log.warn("Failed to close {}", client, e);
      }
    }
  }

  /** Any file path that specifies a non-existent file will not be treated as an error. */
  private Collection<String> deleteObjects(Collection<String> paths) throws S3Exception {
    try {
      /*
       * Per the S3 docs:
       * https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/model/DeleteObjectsResult.html
       * An exception is thrown if there's a client error processing the request or in S3 itself.
       * However, there's no guarantee the delete operation did not happen if an exception is thrown.
       */
      return deleteObjects(paths, MAX_KEYS_PER_BATCH_DELETE);
    } catch (SdkException sdke) {
      throw handleAmazonException(sdke);
    }
  }

  /**
   * Batch deletes from S3.
   *
   * @param entries collection of S3 keys of the files to be deleted.
   * @param batchSize number of deletes to send to S3 at a time
   */
  @VisibleForTesting
  @SuppressForbidden(reason = "Lists.partition")
  Collection<String> deleteObjects(Collection<String> entries, int batchSize) throws S3Exception {
    List<ObjectIdentifier> keysToDelete =
        entries.stream()
            .map(s -> ObjectIdentifier.builder().key(s).build())
            .sorted(Comparator.comparing(ObjectIdentifier::key).reversed())
            .collect(Collectors.toList());

    List<List<ObjectIdentifier>> partitions = Lists.partition(keysToDelete, batchSize);
    Set<String> deletedPaths = new HashSet<>();

    boolean deleteIndividually = false;
    for (List<ObjectIdentifier> partition : partitions) {
      DeleteObjectsRequest request = createBatchDeleteRequest(partition);

      try {
        DeleteObjectsResponse response = s3Client.deleteObjects(request);

        response.deleted().stream().map(DeletedObject::key).forEach(deletedPaths::add);
      } catch (AwsServiceException ase) {
        if (ase.statusCode() == 501) {
          // This means that the batch-delete is not implemented by this S3 server
          deleteIndividually = true;
          break;
        } else {
          throw handleAmazonException(ase);
        }
      } catch (SdkException sdke) {
        throw handleAmazonException(sdke);
      }
    }

    if (deleteIndividually) {
      for (ObjectIdentifier k : keysToDelete) {
        try {
          s3Client.deleteObject(b -> b.bucket(bucketName).key(k.key()));
          deletedPaths.add(k.key());
        } catch (SdkException sdke) {
          throw new S3Exception("Could not delete object with key: " + k.key(), sdke);
        }
      }
    }

    return deletedPaths;
  }

  private DeleteObjectsRequest createBatchDeleteRequest(List<ObjectIdentifier> keysToDelete) {
    return DeleteObjectsRequest.builder()
        .bucket(bucketName)
        .delete(Delete.builder().objects(keysToDelete).build())
        .build();
  }

  private Set<String> listAll(String path) throws S3Exception {
    String prefix = sanitizedDirPath(path);

    try {
      ListObjectsV2Iterable objectListing =
          s3Client.listObjectsV2Paginator(
              builder -> builder.bucket(bucketName).prefix(prefix).build());

      return objectListing.contents().stream()
          .map(S3Object::key)
          // This filtering is needed only for S3mock. Real S3 does not ignore the trailing
          // '/' in the prefix.
          .filter(s -> s.startsWith(prefix))
          .collect(Collectors.toSet());
    } catch (SdkException sdke) {
      throw handleAmazonException(sdke);
    }
  }

  /** Ensures path adheres to some rules: -Doesn't start with a leading slash */
  String sanitizedPath(String path) {
    // Trim space from start and end
    String sanitizedPath = path.trim();

    // Path should start with file delimiter
    if (sanitizedPath.startsWith(S3_FILE_PATH_DELIMITER)) {
      // throw new S3Exception("Invalid Path. Path needs to start with '/'");
      sanitizedPath = sanitizedPath.substring(1).trim();
    }

    return sanitizedPath;
  }

  /**
   * Ensures file path adheres to some rules: -Overall Path rules from `sanitizedPath` -Throw an
   * error if it ends with a trailing slash
   */
  String sanitizedFilePath(String path) throws S3Exception {
    // Trim space from start and end
    String sanitizedPath = sanitizedPath(path);

    if (sanitizedPath.endsWith(S3_FILE_PATH_DELIMITER)) {
      throw new S3Exception("Invalid Path. Path for file can't end with '/'");
    }

    if (sanitizedPath.isEmpty()) {
      throw new S3Exception("Invalid Path. Path cannot be empty");
    }

    return sanitizedPath;
  }

  /**
   * Ensures directory path adheres to some rules: -Overall Path rules from `sanitizedPath` -Add a
   * trailing slash if one does not exist
   */
  String sanitizedDirPath(String path) {
    // Trim space from start and end
    String sanitizedPath = sanitizedPath(path);

    if (!sanitizedPath.endsWith(S3_FILE_PATH_DELIMITER)) {
      sanitizedPath += S3_FILE_PATH_DELIMITER;
    }

    return sanitizedPath;
  }

  /**
   * Best effort to handle Amazon exceptions as checked exceptions. Amazon exception are all
   * subclasses of {@link RuntimeException} so some may still be uncaught and propagated.
   */
  static S3Exception handleAmazonException(SdkException sdke) {
    // Service exceptions may be wrapped, by the encryption client or the credentials provider
    AwsServiceException ase = findCause(sdke, AwsServiceException.class);
    if (ase != null) {
      String errMessage =
          String.format(
              Locale.ROOT,
              "An AmazonServiceException was thrown! [serviceName=%s] "
                  + "[awsRequestId=%s] [httpStatus=%s] [s3ErrorCode=%s] [message=%s]",
              ase.awsErrorDetails().serviceName(),
              ase.requestId(),
              ase.statusCode(),
              ase.awsErrorDetails().errorCode(),
              ase.awsErrorDetails().errorMessage());

      log.error(errMessage);

      if (ase instanceof NoSuchKeyException
          || ase instanceof NoSuchBucketException
          || (ase.statusCode() == 404
              && NOT_FOUND_CODES.contains(ase.awsErrorDetails().errorCode()))) {
        return new S3NotFoundException(errMessage, sdke);
      } else {
        return new S3Exception(errMessage, sdke);
      }
    }

    return new S3Exception(sdke);
  }

  /** The first throwable of the given type in the cause chain, starting with {@code t} itself. */
  static <T extends Throwable> T findCause(Throwable t, Class<T> type) {
    // Bounded, in case of a cycle in the chain
    for (int depth = 0; t != null && depth < 20; t = t.getCause(), depth++) {
      if (type.isInstance(t)) {
        return type.cast(t);
      }
    }
    return null;
  }
}
