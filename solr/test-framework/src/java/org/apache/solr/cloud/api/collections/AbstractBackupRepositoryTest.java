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

package org.apache.solr.cloud.api.collections;

import static org.apache.lucene.codecs.CodecUtil.FOOTER_MAGIC;
import static org.apache.lucene.codecs.CodecUtil.writeBEInt;
import static org.apache.lucene.codecs.CodecUtil.writeBELong;
import static org.apache.solr.core.backup.repository.DelegatingBackupRepository.PARAM_DELEGATE_REPOSITORY_NAME;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.repository.BackupRepositoryFactory;
import org.apache.solr.core.backup.repository.DelegatingBackupRepository;
import org.apache.solr.schema.FieldType;
import org.junit.Test;

public abstract class AbstractBackupRepositoryTest extends SolrTestCaseJ4 {

  protected abstract Class<? extends BackupRepository> getRepositoryClass();

  protected abstract BackupRepository getRepository();

  protected abstract URI getBaseUri() throws URISyntaxException;

  /**
   * Provide a base {@link BackupRepository} configuration for use by any tests that call {@link
   * BackupRepository#init(NamedList)} explicitly.
   *
   * <p>Useful for setting configuration properties required for specific BackupRepository
   * implementations.
   */
  protected NamedList<Object> getBaseBackupRepositoryConfiguration() {
    NamedList<Object> config = new NamedList<>();
    try {
      config.add(CoreAdminParams.BACKUP_LOCATION, Path.of(getBaseUri()).toString());
    } catch (URISyntaxException ignored) {}
    return config;
  }

  @Test
  public void testCanReadProvidedConfigValues() throws Exception {
    final NamedList<Object> config = getBaseBackupRepositoryConfiguration();
    config.add("configKey1", "configVal1");
    config.add("configKey2", "configVal2");
    try (BackupRepository repo = getRepository()) {
      repo.init(config);
      assertEquals("configVal1", repo.getConfigProperty("configKey1"));
      assertEquals("configVal2", repo.getConfigProperty("configKey2"));
    }
  }

  @Test
  public void testCanChooseDefaultOrOverrideLocationValue() throws Exception {
    final NamedList<Object> config = getBaseBackupRepositoryConfiguration();
    int locationIndex = config.indexOf(CoreAdminParams.BACKUP_LOCATION, 0);
    if (locationIndex == -1) {
      config.add(CoreAdminParams.BACKUP_LOCATION, "/someLocation");
    } else {
      config.setVal(locationIndex, "/someLocation");
    }
    try (BackupRepository repo = getRepository()) {
      repo.init(config);
      assertEquals("/someLocation", repo.getBackupLocation(null));
      assertEquals("/someLocation", repo.getBackupLocation(""));
      assertEquals("/someLocation/someOverridingLocation", repo.getBackupLocation("someOverridingLocation"));
      assertEquals("/someLocation/someLocation/someOverridingLocation", repo.getBackupLocation("someLocation/someOverridingLocation"));
      assertEquals("/someLocation/someOverridingLocation", repo.getBackupLocation("/someLocation/someOverridingLocation"));
      assertEquals("/someLocation/someOverridingLocation", repo.getBackupLocation("file:/someLocation/someOverridingLocation"));
      assertEquals("/someLocation/someOverridingLocation", repo.getBackupLocation("file:///someLocation/someOverridingLocation"));
      expectThrows(
          SolrException.class,
          () -> repo.getBackupLocation("/anotherLocation/someOverridingLocation"));
      expectThrows(
          SolrException.class,
          () -> repo.getBackupLocation("file:/anotherLocation/someOverridingLocation"));
      expectThrows(
          SolrException.class,
          () -> repo.getBackupLocation("file:///anotherLocation/someOverridingLocation"));
      expectThrows(
          SolrException.class,
          () -> repo.getBackupLocation("../someOverridingLocation"));
    }
  }

  @Test
  public void testCanDetermineWhetherFilesAndDirectoriesExist() throws Exception {
    try (BackupRepository repo = getRepository()) {
      // Create 'emptyDir/', 'nonEmptyDir/', and 'nonEmptyDir/file.txt'
      final URI emptyDirUri = repo.resolve(getBaseUri(), "emptyDir/");
      final URI nonEmptyDirUri = repo.resolve(getBaseUri(), "nonEmptyDir/");
      final URI nestedFileUri = repo.resolve(nonEmptyDirUri, "file.txt");
      repo.createDirectory(emptyDirUri);
      repo.createDirectory(nonEmptyDirUri);
      addFile(repo, nestedFileUri);

      assertTrue(repo.exists(emptyDirUri));
      assertTrue(repo.exists(nonEmptyDirUri));
      assertTrue(repo.exists(nestedFileUri));
      final URI nonexistedDirUri = repo.resolve(getBaseUri(), "nonexistentDir/");
      assertFalse(repo.exists(nonexistedDirUri));
    }
  }

  @Test
  public void testCanDistinguishBetweenFilesAndDirectories() throws Exception {
    try (BackupRepository repo = getRepository()) {
      final URI emptyDirUri = repo.resolve(getBaseUri(), "emptyDir/");
      final URI nonEmptyDirUri = repo.resolve(getBaseUri(), "nonEmptyDir/");
      final URI nestedFileUri = repo.resolve(nonEmptyDirUri, "file.txt");
      repo.createDirectory(emptyDirUri);
      repo.createDirectory(nonEmptyDirUri);
      addFile(repo, nestedFileUri);

      assertEquals(BackupRepository.PathType.DIRECTORY, repo.getPathType(emptyDirUri));
      assertEquals(BackupRepository.PathType.DIRECTORY, repo.getPathType(nonEmptyDirUri));
      assertEquals(BackupRepository.PathType.FILE, repo.getPathType(nestedFileUri));
    }
  }

  @Test
  public void testArbitraryFileDataCanBeStoredAndRetrieved() throws Exception {
    // create a BR
    // store some binary data in a file
    // retrieve that binary data
    // validate that sent == retrieved
    try (BackupRepository repo = getRepository()) {
      final URI fileUri = repo.resolve(getBaseUri(), "file.txt");
      final byte[] storedBytes = new byte[] {'h', 'e', 'l', 'l', 'o'};
      try (final OutputStream os = repo.createOutput(fileUri)) {
        os.write(storedBytes);
      }

      final int expectedNumBytes = storedBytes.length;
      final byte[] retrievedBytes = new byte[expectedNumBytes];
      try (final IndexInput is =
          repo.openInput(getBaseUri(), "file.txt", new IOContext(IOContext.Context.READ))) {
        assertEquals(expectedNumBytes, is.length());
        is.readBytes(retrievedBytes, 0, expectedNumBytes);
      }
      assertArrayEquals(storedBytes, retrievedBytes);
    }
  }

  // TODO JEGERLOW, create separate test for creation of nested directory when parent doesn't exist
  @Test
  public void testCanDeleteEmptyOrFullDirectories() throws Exception {
    try (BackupRepository repo = getRepository()) {
      // Test deletion of empty and full directories
      final URI emptyDirUri = repo.resolve(getBaseUri(), "emptyDir");
      final URI nonEmptyDirUri = repo.resolve(getBaseUri(), "nonEmptyDir");
      final URI fileUri = repo.resolve(nonEmptyDirUri, "file.txt");
      repo.createDirectory(emptyDirUri);
      repo.createDirectory(nonEmptyDirUri);
      addFile(repo, fileUri);
      repo.deleteDirectory(emptyDirUri);
      repo.deleteDirectory(nonEmptyDirUri);
      assertFalse(repo.exists(emptyDirUri));
      assertFalse(repo.exists(nonEmptyDirUri));
      assertFalse(repo.exists(fileUri));

      // Delete the middle directory in a deeply nested structure (/nest1/nest2/nest3/nest4)
      final URI level1DeeplyNestedUri = repo.resolve(getBaseUri(), "nest1/");
      final URI level2DeeplyNestedUri = repo.resolve(level1DeeplyNestedUri, "nest2/");
      final URI level3DeeplyNestedUri = repo.resolve(level2DeeplyNestedUri, "nest3/");
      final URI level4DeeplyNestedUri = repo.resolve(level3DeeplyNestedUri, "nest4/");
      repo.createDirectory(level1DeeplyNestedUri);
      repo.createDirectory(level2DeeplyNestedUri);
      repo.createDirectory(level3DeeplyNestedUri);
      repo.createDirectory(level4DeeplyNestedUri);
      repo.deleteDirectory(level3DeeplyNestedUri);
      assertTrue(repo.exists(level1DeeplyNestedUri));
      assertTrue(repo.exists(level2DeeplyNestedUri));
      assertFalse(repo.exists(level3DeeplyNestedUri));
      assertFalse(repo.exists(level4DeeplyNestedUri));
    }
  }

  @Test
  public void testDirectoryCreationFailsIfParentDoesntExist() throws Exception {
    try (BackupRepository repo = getRepository()) {
      final URI nonExistentParentUri = repo.resolve(getBaseUri(), "nonExistentParent");
      final URI nestedUri = repo.resolve(nonExistentParentUri, "childDirectoryToCreate");

      repo.createDirectory(nestedUri);
    }
  }

  @Test
  public void testCanDeleteIndividualFiles() throws Exception {
    try (BackupRepository repo = getRepository()) {
      final URI file1Uri = repo.resolve(getBaseUri(), "file1.txt");
      final URI file2Uri = repo.resolve(getBaseUri(), "file2.txt");
      final URI file3Uri = repo.resolve(getBaseUri(), "file3.txt");
      addFile(repo, file1Uri);
      addFile(repo, file2Uri);
      addFile(repo, file3Uri);

      // Ensure nonexistent files can be deleted
      final URI nonexistentFileUri = repo.resolve(getBaseUri(), "file4.txt");
      assertFalse(repo.exists(nonexistentFileUri));
      repo.delete(getBaseUri(), List.of("file4.txt"));

      // Delete existing files individually and in 'bulk'
      repo.delete(getBaseUri(), List.of("file1.txt"));
      repo.delete(getBaseUri(), List.of("file2.txt", "file3.txt"));
      assertFalse(repo.exists(file1Uri));
      assertFalse(repo.exists(file2Uri));
      assertFalse(repo.exists(file3Uri));
    }
  }

  @Test
  public void testCanListFullOrEmptyDirectories() throws Exception {
    try (BackupRepository repo = getRepository()) {
      final URI rootUri = repo.resolve(getBaseUri(), "containsOtherDirs");
      final URI otherDir1Uri = repo.resolve(rootUri, "otherDir1");
      final URI otherDir2Uri = repo.resolve(rootUri, "otherDir2");
      final URI otherDir3Uri = repo.resolve(rootUri, "otherDir3");
      final URI file1Uri = repo.resolve(otherDir3Uri, "file1.txt");
      final URI file2Uri = repo.resolve(otherDir3Uri, "file2.txt");
      repo.createDirectory(rootUri);
      repo.createDirectory(otherDir1Uri);
      repo.createDirectory(otherDir2Uri);
      repo.createDirectory(otherDir3Uri);
      addFile(repo, file1Uri);
      addFile(repo, file2Uri);

      final List<String> rootChildren = List.of(repo.listAll(rootUri));
      assertEquals(3, rootChildren.size());
      assertTrue(rootChildren.contains("otherDir1"));
      assertTrue(rootChildren.contains("otherDir2"));
      assertTrue(rootChildren.contains("otherDir3"));

      final String[] otherDir2Children = repo.listAll(otherDir2Uri);
      assertEquals(0, otherDir2Children.length);

      final List<String> otherDir3Children = List.of(repo.listAll(otherDir3Uri));
      assertEquals(2, otherDir3Children.size());
      assertTrue(otherDir3Children.contains("file1.txt"));
      assertTrue(otherDir3Children.contains("file2.txt"));
    }
  }

  @Test
  public void testDirectoryExistsWithDirectoryUri() throws Exception {
    try (BackupRepository repo = getRepository()) {
      repo.createDirectory(getBaseUri());
      assertTrue(repo.exists(repo.createDirectoryURI(getBaseUri().toString())));
    }
  }

  @Test
  public void testCanDisableChecksumVerification() throws Exception {
    // May contain test implementation specific initialization.
    getRepository();

    // Given two BackupRepository plugins:
    // - A standard BackupRepository plugin.
    // - A NoChecksumFilterBackupRepository that delegates to the previous one, and adds the
    // verifyChecksum=false parameter to the init args of the delegate.
    String repoName = "repo";
    String filterRepoName = "filterRepo";
    PluginInfo[] plugins =
        new PluginInfo[] {
          getPluginInfo(repoName, false),
          getNoChecksumFilterPluginInfo(filterRepoName, true, repoName),
        };
    Collections.shuffle(Arrays.asList(plugins), random());

    // Given a file on the local disk with an invalid checksum (e.g. could be encrypted).
    File sourceDir = createTempDir().toFile();
    String fileName = "source-file";
    String content = "content";
    try (OutputStream os = FileUtils.openOutputStream(new File(sourceDir, fileName));
        IndexOutput io = new OutputStreamIndexOutput("", "", os, Long.BYTES)) {
      byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
      io.writeBytes(bytes, bytes.length);
      // Instead of writing the checksum with CodecUtil.writeFooter(), write a footer with an
      // invalid checksum.
      writeBEInt(io, FOOTER_MAGIC);
      writeBEInt(io, 0);
      writeBELong(io, 1);
    }

    BackupRepositoryFactory repoFactory = new BackupRepositoryFactory(plugins);
    try (SolrResourceLoader resourceLoader = new SolrResourceLoader(sourceDir.toPath())) {

      // When we copy the local file with the standard BackupRepository,
      // then it fails because the checksum is invalid.
      expectThrows(
          CorruptIndexException.class,
          () -> copyFileToRepo(sourceDir, fileName, repoName, repoFactory, resourceLoader));
      File destinationDir = createTempDir().toFile();
      expectThrows(
          CorruptIndexException.class,
          () ->
              copyFileToDir(
                  sourceDir, fileName, destinationDir, repoName, repoFactory, resourceLoader));

      // When we copy the local file with the NoChecksumFilterBackupRepository,
      // then it succeeds because the checksum is not verified,
      // and the checksum is verified alternatively.
      NoChecksumDelegatingBackupRepository.checksumVerifiedAlternatively = false;
      copyFileToRepo(sourceDir, fileName, filterRepoName, repoFactory, resourceLoader);
      assertTrue(NoChecksumDelegatingBackupRepository.checksumVerifiedAlternatively);
      NoChecksumDelegatingBackupRepository.checksumVerifiedAlternatively = false;
      copyFileToDir(
          sourceDir, fileName, destinationDir, filterRepoName, repoFactory, resourceLoader);
      assertTrue(NoChecksumDelegatingBackupRepository.checksumVerifiedAlternatively);
    }
  }

  private void copyFileToRepo(
      File dir,
      String fileName,
      String repoName,
      BackupRepositoryFactory repoFactory,
      SolrResourceLoader resourceLoader)
      throws IOException, URISyntaxException {
    try (BackupRepository repo = repoFactory.newInstance(resourceLoader, repoName);
        Directory directory = new NIOFSDirectory(dir.toPath())) {
      URI destinationDir = repo.resolve(getBaseUri(), "destination-folder");
      repo.copyIndexFileFrom(directory, fileName, destinationDir, fileName);
    }
  }

  private void copyFileToDir(
      File sourceDir,
      String fileName,
      File destinationDir,
      String repoName,
      BackupRepositoryFactory repoFactory,
      SolrResourceLoader resourceLoader)
      throws IOException {
    try (BackupRepository repo = repoFactory.newInstance(resourceLoader, repoName);
        Directory sourceDirectory = new NIOFSDirectory(sourceDir.toPath());
        Directory destinationDirectory = new NIOFSDirectory(destinationDir.toPath())) {
      repo.copyIndexFileFrom(sourceDirectory, fileName, destinationDirectory, fileName);
    }
  }

  protected PluginInfo getPluginInfo(String pluginName, boolean isDefault) {
    return getPluginInfo(pluginName, isDefault, Map.of());
  }

  protected PluginInfo getPluginInfo(
      String pluginName, boolean isDefault, Map<String, String> attributes) {
    Map<String, String> attrs = new HashMap<>();
    attrs.put(CoreAdminParams.NAME, pluginName);
    attrs.put(FieldType.CLASS_NAME, getRepositoryClass().getName());
    attrs.put("default", Boolean.toString(isDefault));
    attrs.putAll(attributes);
    return new PluginInfo("repository", attrs, getBaseBackupRepositoryConfiguration(), null);
  }

  private PluginInfo getNoChecksumFilterPluginInfo(
      String pluginName, boolean isDefault, String delegateName) {
    Map<String, String> attrs = new HashMap<>();
    attrs.put(CoreAdminParams.NAME, pluginName);
    attrs.put(FieldType.CLASS_NAME, NoChecksumDelegatingBackupRepository.class.getName());
    attrs.put("default", Boolean.toString(isDefault));
    NamedList<Object> args = new NamedList<>();
    args.add(PARAM_DELEGATE_REPOSITORY_NAME, delegateName);
    return new PluginInfo("repository", attrs, args, null);
  }

  private void addFile(BackupRepository repo, URI file) throws IOException {
    try (OutputStream os = repo.createOutput(file)) {
      os.write(100);
      os.write(101);
      os.write(102);
    }
  }

  /**
   * Test implementation of a {@link DelegatingBackupRepository} that disables the checksum
   * verification on its delegate {@link BackupRepository}.
   *
   * <p>This test class is public to be instantiated by the {@link BackupRepositoryFactory}.
   */
  public static class NoChecksumDelegatingBackupRepository extends DelegatingBackupRepository {

    static volatile boolean checksumVerifiedAlternatively;

    @Override
    protected NamedList<?> getDelegateInitArgs(NamedList<?> initArgs) {
      NamedList<Object> newInitArgs = new NamedList<>(initArgs.size() + 1);
      newInitArgs.add(PARAM_VERIFY_CHECKSUM, Boolean.FALSE.toString());
      newInitArgs.addAll(initArgs);
      return newInitArgs;
    }

    @Override
    public void copyIndexFileFrom(
        Directory sourceDir, String sourceFileName, Directory destDir, String destFileName)
        throws IOException {
      // Verify the checksum with the original directory.
      verifyChecksum(sourceDir, sourceFileName);
      // Copy the index file with the unwrapped (delegate) directory.
      super.copyIndexFileFrom(
          FilterDirectory.unwrap(sourceDir), sourceFileName, destDir, destFileName);
    }

    @Override
    public void copyIndexFileFrom(
        Directory sourceDir, String sourceFileName, URI destDir, String destFileName)
        throws IOException {
      // Verify the checksum with the original directory.
      verifyChecksum(sourceDir, sourceFileName);
      // Copy the index file with the unwrapped (delegate) directory.
      super.copyIndexFileFrom(
          FilterDirectory.unwrap(sourceDir), sourceFileName, destDir, destFileName);
    }

    private void verifyChecksum(Directory sourceDir, String sourceFileName) {
      checksumVerifiedAlternatively = true;
    }
  }
}
