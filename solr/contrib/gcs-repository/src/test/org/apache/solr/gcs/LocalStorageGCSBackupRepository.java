package org.apache.solr.gcs;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.net.URI;
import java.nio.file.NoSuchFileException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class LocalStorageGCSBackupRepository extends GCSBackupRepository {

  // TODO JEGERLOW: Will need a 'clear' method for tests to call in their 'AFterClass' so that multiple GCSREpository test classes don't inherit state
  protected static Storage stashedStorage = null;

    @Override
    public Storage initStorage(String credentialPath) {
      // LocalStorageHelper produces 'Storage' objects that track blob store state in non-static memory unique to each
      // Storage instance.  For various components in Solr to have a coherent view of the blob-space then, they need to
      // share a single 'Storage' object.
      setupSingletonStorageInstanceIfNecessary();
      storage = stashedStorage;
      return storage;
    }

    // A reimplementation of delete functionality that avoids batching.  Batching is ideal in production use cases, but
    // isn't supported by the in-memory Storage implementation provided by LocalStorageHelper
    @Override
    public void delete(URI path, Collection<String> files, boolean ignoreNoSuchFileException) throws IOException {
      final List<Boolean> results = Lists.newArrayList();

      final List<String> filesOrdered = files.stream().collect(Collectors.toList());
      for (String file : filesOrdered) {
        final String prefix = path.toString().endsWith("/") ? path.toString() : path.toString() + "/";
        results.add(storage.delete(BlobId.of(bucketName, prefix + file)));
      }

      if (!ignoreNoSuchFileException) {
        int failedDelete = results.indexOf(Boolean.FALSE);
        if (failedDelete != -1) {
          throw new NoSuchFileException("File " + filesOrdered.get(failedDelete) + " was not found");
        }
      }
    }

    @Override
    public void deleteDirectory(URI path) throws IOException {
      List<BlobId> blobIds = allBlobsAtDir(path);
      if (!blobIds.isEmpty()) {
        for (BlobId blob : blobIds) {
          storage.delete(blob);
        }
      }
    }

    // Should only be called after locking on 'stashedStorage'
    protected synchronized void setupSingletonStorageInstanceIfNecessary() {
      if (stashedStorage != null) {
        return;
      }

      stashedStorage = LocalStorageHelper.customOptions(false).getService();
      storage = stashedStorage;
      // Bootstrap 'location' on first creation
      try {
        final String baseLocation = getBackupLocation(null);
        final URI baseLocationUri = createURI(baseLocation);
        createDirectory(baseLocationUri);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
}
