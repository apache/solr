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

import static org.apache.solr.common.params.CoreAdminParams.BACKUP_LOCATION;
import static org.apache.solr.gcs.GCSConfigParser.GCS_BUCKET_ENV_VAR_NAME;
import static org.apache.solr.gcs.GCSConfigParser.GCS_CREDENTIAL_ENV_VAR_NAME;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.solr.cloud.api.collections.AbstractBackupRepositoryTest;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Test;

/** Unit tests for {@link GCSBackupRepository} that use an in-memory Storage object */
public class GCSBackupRepositoryTest extends AbstractBackupRepositoryTest {

  @AfterClass
  public static void tearDownClass() {
    LocalStorageGCSBackupRepository.clearStashedStorage();
  }

  @Override
  protected Class<? extends BackupRepository> getRepositoryClass() {
    return LocalStorageGCSBackupRepository.class;
  }

  @Override
  protected NamedList<Object> getBaseBackupRepositoryConfiguration() {
    final NamedList<Object> config = new NamedList<>();
    config.add(BACKUP_LOCATION, "backup1");
    return config;
  }

  @Override
  protected BackupRepository getRepository() {
    NamedList<Object> config = getBaseBackupRepositoryConfiguration();
    final GCSBackupRepository repository = new LocalStorageGCSBackupRepository();
    repository.init(config);

    return repository;
  }

  @Override
  protected URI getBaseUri() throws URISyntaxException {
    return new URI("tmp");
  }

  @Test
  public void testInitStoreDoesNotFailWithMissingCredentials() {
    Map<String, String> config = new HashMap<>();
    config.put(GCS_BUCKET_ENV_VAR_NAME, "a_bucket_name");
    // explicitly setting credential name to null; will work inside google-cloud project
    config.put(GCS_CREDENTIAL_ENV_VAR_NAME, null);
    config.put(BACKUP_LOCATION, "/==");

    BackupRepository gcsBackupRepository = getRepository();

    gcsBackupRepository.init(new NamedList<>(config));
  }

  @Test
  public void testCopyIndexFileToPropagatesReadFailures() throws Exception {
    Storage failingStorage = createFailingStorage();
    GCSBackupRepository repo = createRepositoryWithStorage(failingStorage);

    try (Directory dest = new ByteBuffersDirectory()) {
      URI sourceDir = repo.resolve(getBaseUri(), "backup");
      IOException thrown =
          expectThrows(
              IOException.class,
              () -> repo.copyIndexFileTo(sourceDir, "any.dat", dest, "dest.dat"));
      assertTrue(thrown.getMessage().contains("Failed to copy index file from GCS"));
      assertNotNull(thrown.getCause());
      assertTrue(thrown.getCause() instanceof StorageException);
      assertEquals("simulated GCS read failure", thrown.getCause().getMessage());
    }
  }

  @Test
  public void testCopyIndexFileToHandlesZeroByteReads() throws Exception {
    Storage realStorage = LocalStorageHelper.customOptions(false).getService();
    byte[] data = new byte[100];
    random().nextBytes(data);
    // "solrBackupsBucket" matches GCSConfigParser.DEFAULT_GCS_BUCKET_VALUE
    String bucketName = "solrBackupsBucket";

    GCSBackupRepository repo = createRepositoryWithStorage(realStorage);
    URI sourceDir = repo.resolve(getBaseUri(), "backup");
    BlobId blobId = BlobId.of(bucketName, sourceDir + "/source.dat");
    createBlob(realStorage, blobId, data);

    Storage zeroReturningStorage = createZeroReturningStorage(realStorage);
    GCSBackupRepository proxyRepo = createRepositoryWithStorage(zeroReturningStorage);

    try (Directory dest = new ByteBuffersDirectory()) {
      proxyRepo.copyIndexFileTo(sourceDir, "source.dat", dest, "dest.dat");
      try (IndexInput in = dest.openInput("dest.dat", IOContext.DEFAULT)) {
        assertEquals(data.length, in.length());
        byte[] read = new byte[data.length];
        in.readBytes(read, 0, data.length);
        assertArrayEquals(data, read);
      }
    }
  }

  @Test
  public void testCopyIndexFileToCopiesFile() throws Exception {
    Storage realStorage = LocalStorageHelper.customOptions(false).getService();
    byte[] data = new byte[100];
    random().nextBytes(data);
    // "solrBackupsBucket" matches GCSConfigParser.DEFAULT_GCS_BUCKET_VALUE
    String bucketName = "solrBackupsBucket";

    GCSBackupRepository repo = createRepositoryWithStorage(realStorage);
    URI sourceDir = repo.resolve(getBaseUri(), "backup");
    BlobId blobId = BlobId.of(bucketName, sourceDir + "/source.dat");
    createBlob(realStorage, blobId, data);

    try (Directory dest = new ByteBuffersDirectory()) {
      repo.copyIndexFileTo(sourceDir, "source.dat", dest, "dest.dat");
      try (IndexInput in = dest.openInput("dest.dat", IOContext.DEFAULT)) {
        assertEquals(data.length, in.length());
        byte[] read = new byte[data.length];
        in.readBytes(read, 0, data.length);
        assertArrayEquals(data, read);
      }
    }
  }

  /**
   * Creates a blob, skipping (rather than failing) the test if the current default locale trips the
   * known FakeStorageRpc/RFC3339 date-parsing bug - see {@link
   * LocalStorageGCSBackupRepository#initializeBackupLocation()} for the same pattern.
   */
  private static void createBlob(Storage storage, BlobId blobId, byte[] data) {
    try {
      storage.create(BlobInfo.newBuilder(blobId).build(), data);
    } catch (Exception e) {
      final Throwable cause = e.getCause();
      Assume.assumeFalse(
          "This test uses a GCS mock library that is incompatible with the current default locale",
          cause != null
              && e instanceof StorageException
              && cause.getMessage().contains("Invalid date/time format")
              && cause instanceof NumberFormatException);
      // Not the known locale incompatibility - a genuine failure, so don't swallow it.
      throw new RuntimeException(e);
    }
  }

  /** Storage proxy that fails on {@code reader} so we can assert copy errors are propagated. */
  private static Storage createFailingStorage() {
    Storage delegate = LocalStorageHelper.customOptions(false).getService();
    return (Storage)
        Proxy.newProxyInstance(
            Storage.class.getClassLoader(),
            new Class<?>[] {Storage.class},
            (proxy, method, args) -> {
              if ("reader".equals(method.getName())) {
                throw new StorageException(0, "simulated GCS read failure");
              }
              return invokeAndUnwrap(method, delegate, args);
            });
  }

  /**
   * Storage proxy whose {@code reader} first returns a zero-byte {@link ReadChannel}, so we can
   * assert that {@code copyIndexFileTo} retries instead of treating 0 as EOF.
   */
  private static Storage createZeroReturningStorage(Storage delegate) {
    return (Storage)
        Proxy.newProxyInstance(
            Storage.class.getClassLoader(),
            new Class<?>[] {Storage.class},
            (proxy, method, args) -> {
              if ("reader".equals(method.getName())
                  && args != null
                  && args.length == 1
                  && args[0] instanceof BlobId) {
                ReadChannel realChannel = (ReadChannel) invokeAndUnwrap(method, delegate, args);
                return createZeroFirstReadChannel(realChannel);
              }
              return invokeAndUnwrap(method, delegate, args);
            });
  }

  /**
   * {@link ReadChannel} that returns 0 on the first {@code read} (a short/empty read, not EOF),
   * then delegates later reads to {@code delegate}. Used to reproduce the restore-truncation bug
   * where a 0-byte read was treated as end-of-stream.
   */
  private static ReadChannel createZeroFirstReadChannel(ReadChannel delegate) {
    return (ReadChannel)
        Proxy.newProxyInstance(
            ReadChannel.class.getClassLoader(),
            new Class<?>[] {ReadChannel.class},
            new InvocationHandler() {
              private boolean returnedZero = false;

              @Override
              public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                if ("read".equals(method.getName())
                    && args != null
                    && args.length == 1
                    && args[0] instanceof ByteBuffer) {
                  if (!returnedZero) {
                    returnedZero = true;
                    return 0;
                  }
                }
                return invokeAndUnwrap(method, delegate, args);
              }
            });
  }

  /**
   * Forwards a JDK proxy call to {@code target}. {@link Method#invoke} wraps checked exceptions in
   * {@link InvocationTargetException}; unwrap so tests and {@code copyIndexFileTo} see the real GCS
   * / channel exception instead of a reflection wrapper.
   */
  private static Object invokeAndUnwrap(Method method, Object target, Object[] args)
      throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private GCSBackupRepository createRepositoryWithStorage(Storage storage) {
    TestGCSBackupRepository repo = new TestGCSBackupRepository(storage);
    repo.init(getBaseBackupRepositoryConfiguration());
    return repo;
  }

  /**
   * Test-only repository that injects a given {@link Storage} instead of creating a real GCS
   * client. Lets tests simulate failures and unusual read behavior without talking to GCS.
   */
  private static class TestGCSBackupRepository extends GCSBackupRepository {
    private final Storage testStorage;

    TestGCSBackupRepository(Storage testStorage) {
      this.testStorage = testStorage;
    }

    @Override
    protected Storage initStorage() {
      this.storage = testStorage;
      return testStorage;
    }
  }
}
