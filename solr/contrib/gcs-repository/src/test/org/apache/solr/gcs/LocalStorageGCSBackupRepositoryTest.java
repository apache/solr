package org.apache.solr.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

public class LocalStorageGCSBackupRepositoryTest extends SolrTestCaseJ4 {

  @Test
  public void testDifferentClientsSeeSameFiles() {
    Storage storage1 = LocalStorageHelper.customOptions(false).getService();
    storage1.create(BlobInfo.newBuilder("bucketName", "/blobname").build(), "hello world".getBytes(), Storage.BlobTargetOption.doesNotExist());

    //storage1 = LocalStorageHelper.customOptions(false).getService();
    Page<Blob> foo = storage1.list("bucketName", Storage.BlobListOption.currentDirectory(), Storage.BlobListOption.prefix("/"), Storage.BlobListOption.fields());
    for (Blob b : foo.iterateAll()) {
      System.out.println (b.getContent());
    }
  }
}
