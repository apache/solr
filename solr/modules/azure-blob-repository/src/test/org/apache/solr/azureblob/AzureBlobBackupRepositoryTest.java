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
package org.apache.solr.azureblob;

import static org.apache.solr.azureblob.AzureBlobBackupRepository.BLOB_SCHEME;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.commons.io.file.PathUtils;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.junit.Before;
import org.junit.Test;

public class AzureBlobBackupRepositoryTest extends AbstractAzureBlobClientTest {

  private AzureBlobBackupRepository repository;

  protected static final String CONTAINER_NAME = "test-container";

  protected Class<? extends BackupRepository> getRepositoryClass() {
    return AzureBlobBackupRepository.class;
  }

  protected BackupRepository getRepository() {
    return repository;
  }

  protected URI getBaseUri() {
    return URI.create(BLOB_SCHEME + ":/");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    NamedList<Object> config = new NamedList<>();
    config.add("azure.blob.container.name", CONTAINER_NAME);
    config.add("azure.blob.connection.string", getConnectionString());

    repository =
        new AzureBlobBackupRepository() {
          @Override
          public void init(NamedList<?> args) {
            this.config = args;
            setClient(AzureBlobBackupRepositoryTest.this.client);
          }
        };

    repository.init(config);
  }

  @Override
  public void tearDown() throws Exception {
    if (repository != null) {
      repository.close();
    }
    super.tearDown();
  }

  @Test
  public void testCreateDirectory() throws IOException {
    URI dirUri = getBaseUri().resolve("test-dir/");
    repository.createDirectory(dirUri);
    assertTrue("Directory should exist", repository.exists(dirUri));
    assertEquals(
        "Should be a directory",
        BackupRepository.PathType.DIRECTORY,
        repository.getPathType(dirUri));
  }

  @Test
  public void testCreateFile() throws IOException {
    URI fileUri = getBaseUri().resolve("test-file.txt");
    String content = "Hello, Azure Blob Storage!";

    try (OutputStream output = repository.createOutput(fileUri)) {
      output.write(content.getBytes(StandardCharsets.UTF_8));
    }

    assertTrue("File should exist", repository.exists(fileUri));
    assertEquals(
        "Should be a file", BackupRepository.PathType.FILE, repository.getPathType(fileUri));
  }

  @Test
  public void testReadWriteFile() throws IOException {
    URI fileUri = getBaseUri().resolve("read-write-test.txt");
    String originalContent = "Test content for read/write operations";

    try (OutputStream output = repository.createOutput(fileUri)) {
      output.write(originalContent.getBytes(StandardCharsets.UTF_8));
    }

    try (IndexInput input =
        repository.openInput(getBaseUri(), "read-write-test.txt", IOContext.DEFAULT)) {
      byte[] buffer = new byte[1024];
      input.readBytes(buffer, 0, (int) input.length());
      String readContent = new String(buffer, 0, (int) input.length(), StandardCharsets.UTF_8);
      assertEquals("Content should match", originalContent, readContent);
    }
  }

  @Test
  public void testDeleteFile() throws IOException {
    URI fileUri = getBaseUri().resolve("delete-test.txt");
    String content = "File to be deleted";

    try (OutputStream output = repository.createOutput(fileUri)) {
      output.write(content.getBytes(StandardCharsets.UTF_8));
    }

    assertTrue("File should exist before deletion", repository.exists(fileUri));

    repository.delete(fileUri, Arrays.asList("delete-test.txt"));

    assertFalse("File should not exist after deletion", repository.exists(fileUri));
  }

  @Test
  public void testDeleteFilesInDirectory() throws IOException {
    URI dirUri = getBaseUri().resolve("delete-files-dir/");
    repository.createDirectory(dirUri);

    URI fileAUri = dirUri.resolve("a.txt");
    URI fileBUri = dirUri.resolve("b.txt");
    try (OutputStream output = repository.createOutput(fileAUri)) {
      output.write("alpha".getBytes(StandardCharsets.UTF_8));
    }
    try (OutputStream output = repository.createOutput(fileBUri)) {
      output.write("beta".getBytes(StandardCharsets.UTF_8));
    }
    assertTrue("File a should exist before deletion", repository.exists(fileAUri));
    assertTrue("File b should exist before deletion", repository.exists(fileBUri));

    repository.delete(dirUri, Arrays.asList("a.txt", "b.txt"));

    assertFalse("File a should not exist after deletion", repository.exists(fileAUri));
    assertFalse("File b should not exist after deletion", repository.exists(fileBUri));
  }

  @Test
  public void testDeleteDirectory() throws IOException {
    URI dirUri = getBaseUri().resolve("delete-dir/");
    URI fileUri = dirUri.resolve("nested-file.txt");

    repository.createDirectory(dirUri);
    try (OutputStream output = repository.createOutput(fileUri)) {
      output.write("Nested file content".getBytes(StandardCharsets.UTF_8));
    }

    assertTrue("Directory should exist", repository.exists(dirUri));
    assertTrue("File should exist", repository.exists(fileUri));

    repository.deleteDirectory(dirUri);

    assertFalse("Directory should not exist after deletion", repository.exists(dirUri));
    assertFalse("File should not exist after deletion", repository.exists(fileUri));
  }

  @Test
  public void testListDirectory() throws IOException {
    URI dirUri = getBaseUri().resolve("list-test/");
    repository.createDirectory(dirUri);

    String[] fileNames = {"file1.txt", "file2.txt", "subdir/"};
    for (String fileName : fileNames) {
      URI fileUri = dirUri.resolve(fileName);
      if (fileName.endsWith("/")) {
        repository.createDirectory(fileUri);
      } else {
        try (OutputStream output = repository.createOutput(fileUri)) {
          output.write(("Content of " + fileName).getBytes(StandardCharsets.UTF_8));
        }
      }
    }

    String[] listedFiles = repository.listAll(dirUri);
    assertEquals("Should list all files and directories", fileNames.length, listedFiles.length);

    for (String fileName : fileNames) {
      boolean found = false;
      for (String listedFile : listedFiles) {
        if (fileName.equals(listedFile)) {
          found = true;
          break;
        }
      }
      assertTrue("Should find file: " + fileName, found);
    }
  }

  @Test
  public void testCopyFileFromDirectory() throws IOException {
    Path tempDir = Files.createTempDirectory("blob-test");
    Path tempFile = tempDir.resolve("source-file.txt");
    String content = "Source file content";
    Files.write(tempFile, content.getBytes(StandardCharsets.UTF_8));

    try {
      Directory sourceDir = new MMapDirectory(tempDir);
      URI destDirUri = getBaseUri().resolve("copy-from-dir");

      repository.copyFileFrom(sourceDir, "source-file.txt", destDirUri);

      URI destUri = repository.resolve(destDirUri, "source-file.txt");
      assertTrue("Copied file should exist", repository.exists(destUri));

      // Verify content
      try (IndexInput input =
          repository.openInput(destDirUri, "source-file.txt", IOContext.DEFAULT)) {
        byte[] buffer = new byte[1024];
        input.readBytes(buffer, 0, (int) input.length());
        String readContent = new String(buffer, 0, (int) input.length(), StandardCharsets.UTF_8);
        assertEquals("Content should match", content, readContent);
      }

      sourceDir.close();
    } finally {
      PathUtils.deleteDirectory(tempDir);
    }
  }

  @Test
  public void testCopyFileToDirectory() throws IOException {
    URI sourceUri = getBaseUri().resolve("source-file.txt");
    String content = "Source file content";

    try (OutputStream output = repository.createOutput(sourceUri)) {
      output.write(content.getBytes(StandardCharsets.UTF_8));
    }

    Path tempDir = Files.createTempDirectory("blob-test");

    try {
      Directory destDir = new MMapDirectory(tempDir);

      repository.copyFileTo(sourceUri, "source-file.txt", destDir);

      Path destFile = tempDir.resolve("source-file.txt");
      assertTrue("Destination file should exist", Files.exists(destFile));

      String readContent = Files.readString(destFile, StandardCharsets.UTF_8);
      assertEquals("Content should match", content, readContent);

      destDir.close();
    } finally {
      PathUtils.deleteDirectory(tempDir);
    }
  }

  @Test
  public void testIndexInputOutput() throws IOException {
    URI fileUri = getBaseUri().resolve("index-test.txt");
    String content = "Test content for index input/output";

    try (OutputStream output = repository.createOutput(fileUri)) {
      output.write(content.getBytes(StandardCharsets.UTF_8));
    }

    try (IndexInput input =
        repository.openInput(getBaseUri(), "index-test.txt", IOContext.DEFAULT)) {
      byte[] buffer = new byte[(int) input.length()];
      input.readBytes(buffer, 0, buffer.length);
      String readContent = new String(buffer, StandardCharsets.UTF_8);
      assertEquals("Content should match", content, readContent);
    }
  }

  @Test
  public void testRetrieveChecksumViaRepository() throws IOException {
    Path tempDir = Files.createTempDirectory("blob-checksum-test");
    String fileName = "checksum-test.bin";
    long expectedChecksum;
    long expectedLength;
    try (Directory localDir = new MMapDirectory(tempDir)) {
      try (IndexOutput out = localDir.createOutput(fileName, IOContext.DEFAULT)) {
        CodecUtil.writeIndexHeader(out, "azure-blob-test", 1, new byte[16], "suffix");
        for (int i = 0; i < 10000; i++) {
          out.writeInt(i);
        }
        CodecUtil.writeFooter(out);
      }

      try (IndexInput in = localDir.openInput(fileName, IOContext.READONCE)) {
        expectedChecksum = CodecUtil.retrieveChecksum(in);
        expectedLength = in.length();
      }

      URI dirUri = getBaseUri().resolve("checksum-restore-dir");
      repository.copyFileFrom(localDir, fileName, dirUri);

      try (IndexInput in = repository.openInput(dirUri, fileName, IOContext.READONCE)) {
        assertEquals("Length should match original", expectedLength, in.length());
        long checksumFromRepo = CodecUtil.retrieveChecksum(in);
        assertEquals(
            "Checksum read via repository should match local checksum",
            expectedChecksum,
            checksumFromRepo);

        // After retrieveChecksum, the input is positioned near EOF. Seek back to 0 (the pattern
        // used by checksumEntireFile / readIndexHeader) and verify we can read from the start.
        in.seek(0);
        assertEquals("Position should be 0 after backward seek", 0, in.getFilePointer());

        byte[] magicBytes = new byte[4];
        in.readBytes(magicBytes, 0, magicBytes.length);
        int magic =
            ((magicBytes[0] & 0xFF) << 24)
                | ((magicBytes[1] & 0xFF) << 16)
                | ((magicBytes[2] & 0xFF) << 8)
                | (magicBytes[3] & 0xFF);
        assertEquals("First int should be the Lucene codec magic", CodecUtil.CODEC_MAGIC, magic);
      }
    } finally {
      PathUtils.deleteDirectory(tempDir);
    }
  }

  @Test
  public void testChecksumVerification() throws IOException {
    URI fileUri = getBaseUri().resolve("checksum-test.txt");
    String content = "Test content for checksum verification";

    try (OutputStream output = repository.createOutput(fileUri)) {
      output.write(content.getBytes(StandardCharsets.UTF_8));
      output.write("FOOTER".getBytes(StandardCharsets.UTF_8));
    }

    try (IndexInput input =
        repository.openInput(getBaseUri(), "checksum-test.txt", IOContext.DEFAULT)) {
      byte[] buffer = new byte[1024];
      input.readBytes(buffer, 0, (int) input.length());
      String readContent = new String(buffer, 0, (int) input.length(), StandardCharsets.UTF_8);
      assertTrue("Content should contain original text", readContent.contains(content));
    }
  }

  @Test
  public void testCopyIndexFileFromVerifiesChecksum() throws IOException {
    Path tempDir = Files.createTempDirectory("blob-verify-checksum");
    String fileName = "verify-checksum.bin";
    try (Directory localDir = new MMapDirectory(tempDir)) {
      writeLuceneFile(localDir, fileName);

      long expectedChecksum;
      long expectedLength;
      try (IndexInput in = localDir.openInput(fileName, IOContext.READONCE)) {
        expectedChecksum = CodecUtil.retrieveChecksum(in);
        expectedLength = in.length();
      }

      AzureBlobBackupRepository verifyingRepo = newRepository(true);
      URI dirUri = getBaseUri().resolve("verify-checksum-dir");
      verifyingRepo.copyFileFrom(localDir, fileName, dirUri);

      try (IndexInput in = verifyingRepo.openInput(dirUri, fileName, IOContext.READONCE)) {
        assertEquals(
            "Length should match original after footer rewrite", expectedLength, in.length());
        assertEquals(
            "Checksum should match after verification and footer rewrite",
            expectedChecksum,
            CodecUtil.retrieveChecksum(in));
      }
    } finally {
      PathUtils.deleteDirectory(tempDir);
    }
  }

  @Test
  public void testCopyIndexFileFromThrowsOnTooSmallFile() throws IOException {
    Path tempDir = Files.createTempDirectory("blob-small-file");
    String fileName = "too-small.bin";
    try (Directory localDir = new MMapDirectory(tempDir)) {
      try (IndexOutput out = localDir.createOutput(fileName, IOContext.DEFAULT)) {
        // Smaller than CodecUtil.footerLength() (16 bytes).
        out.writeBytes(new byte[] {1, 2, 3, 4, 5}, 5);
      }

      AzureBlobBackupRepository verifyingRepo = newRepository(true);
      URI dirUri = getBaseUri().resolve("too-small-dir");
      expectThrows(
          CorruptIndexException.class,
          () -> verifyingRepo.copyFileFrom(localDir, fileName, dirUri));
    } finally {
      PathUtils.deleteDirectory(tempDir);
    }
  }

  @Test
  public void testCopyIndexFileFromDetectsCorruption() throws IOException {
    Path tempDir = Files.createTempDirectory("blob-corrupt-file");
    String fileName = "corrupt.bin";
    try (Directory localDir = new MMapDirectory(tempDir)) {
      writeLuceneFile(localDir, fileName);

      // Corrupt a byte in the data region (not the footer) so the stored CRC no longer matches.
      Path onDisk = tempDir.resolve(fileName);
      byte[] bytes = Files.readAllBytes(onDisk);
      int corruptIndex = bytes.length / 2;
      bytes[corruptIndex] = (byte) (bytes[corruptIndex] ^ 0xFF);
      Files.write(onDisk, bytes);

      AzureBlobBackupRepository verifyingRepo = newRepository(true);
      URI dirUri = getBaseUri().resolve("corrupt-dir");
      expectThrows(
          CorruptIndexException.class,
          () -> verifyingRepo.copyFileFrom(localDir, fileName, dirUri));
    } finally {
      PathUtils.deleteDirectory(tempDir);
    }
  }

  @Test
  public void testExistsVsGetPathTypeForExternalVirtualDirectory() throws IOException {
    // Simulate an external tool (e.g. azcopy) writing a child blob without an hdi_isfolder marker
    // for its parent "directory", bypassing this module's pushStream parent-marker creation.
    BlobServiceClient serviceClient =
        new BlobServiceClientBuilder().connectionString(getConnectionString()).buildClient();
    BlobContainerClient containerClient = serviceClient.getBlobContainerClient(containerName);
    containerClient
        .getBlobClient("external-dir/child.txt")
        .upload(BinaryData.fromString("external data"), true);

    URI dirUri = getBaseUri().resolve("external-dir/");

    // getPathType uses prefix listing, so the marker-less directory is reported as a DIRECTORY...
    assertEquals(
        "Marker-less directory should be detected as a directory",
        BackupRepository.PathType.DIRECTORY,
        repository.getPathType(dirUri));

    // ...but exists() resolves the exact (marker-less) blob and therefore returns false. This is
    // the documented asymmetry; the module is self-consistent because it always writes markers.
    assertFalse(
        "exists() returns false for a marker-less external directory", repository.exists(dirUri));
  }

  protected NamedList<Object> getBaseBackupRepositoryConfiguration() {
    NamedList<Object> config = new NamedList<>();
    config.add("azure.blob.container.name", CONTAINER_NAME);
    config.add("azure.blob.connection.string", getConnectionString());
    return config;
  }

  @Test
  public void testCanReadProvidedConfigValues() throws Exception {
    final NamedList<Object> config = getBaseBackupRepositoryConfiguration();
    config.add("configKey1", "configVal1");
    config.add("configKey2", "configVal2");
    config.add("location", "foo");
    try (BackupRepository repo = getRepository()) {
      repo.init(config);
      assertEquals("configVal1", repo.getConfigProperty("configKey1"));
      assertEquals("configVal2", repo.getConfigProperty("configKey2"));
    }
  }

  @Test
  public void testCanChooseDefaultOrOverrideLocationValue() throws Exception {
    final NamedList<Object> config = getBaseBackupRepositoryConfiguration();
    config.add("location", "foo");
    try (BackupRepository repo = getRepository()) {
      repo.init(config);
      assertEquals("foo", repo.getConfigProperty("location"));
    }
  }

  /** Builds a repository sharing the test client, with checksum verification explicitly set. */
  private AzureBlobBackupRepository newRepository(boolean verifyChecksum) {
    AzureBlobBackupRepository repo =
        new AzureBlobBackupRepository() {
          @Override
          public void init(NamedList<?> args) {
            this.config = args;
            this.shouldVerifyChecksum = verifyChecksum;
            setClient(AzureBlobBackupRepositoryTest.this.client);
          }
        };
    repo.init(getBaseBackupRepositoryConfiguration());
    return repo;
  }

  private static void writeLuceneFile(Directory dir, String fileName) throws IOException {
    try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
      CodecUtil.writeIndexHeader(out, "azure-blob-test", 1, new byte[16], "suffix");
      for (int i = 0; i < 10000; i++) {
        out.writeInt(i);
      }
      CodecUtil.writeFooter(out);
    }
  }
}
