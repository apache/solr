package org.apache.solr.blob;

import org.apache.lucene.mockfile.FilterPath;
import org.apache.lucene.store.*;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.core.backup.repository.LocalFileSystemRepository;
import org.junit.*;

import java.io.IOException;
import java.net.URI;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;

public class BlobDirectoryTest extends SolrTestCaseJ4 {

    private static final String REPOSITORY_NAME = "local";

    private static final String SOLR_CONFIG = "<solr>" +
        " <backup>" +
        "  <repository name=\"" + REPOSITORY_NAME + "\"" +
        "   class=\"" + LocalFileSystemRepository.class.getName() + "\"/>" +
        " </backup>" +
        "</solr>";

    private static Path blobRootDir;
    private static NodeConfig nodeConfig;
    private static CoreContainer coreContainer;

    private BlobDirectoryFactory directoryFactory;
    private Directory directory;
    private String directoryPath;

    @BeforeClass
    public static void setupLoader() {
        Path solrHome = FilterPath.unwrap(createTempDir());
        blobRootDir = FilterPath.unwrap(createTempDir());
        nodeConfig = SolrXmlConfig.fromString(solrHome, SOLR_CONFIG);
        coreContainer = new CoreContainer(nodeConfig);
        coreContainer.load();
    }

    @AfterClass
    public static void cleanupLoader() {
        if (coreContainer != null) {
            coreContainer.shutdown();
        }
    }

    @Before
    public void setupDirectory() throws Exception {
        directoryFactory = new BlobDirectoryFactory();
        directoryFactory.initCoreContainer(coreContainer);
        NamedList<String> args = new NamedList<>();
        args.add(CoreAdminParams.BACKUP_REPOSITORY, REPOSITORY_NAME);
        args.add(CoreAdminParams.BACKUP_LOCATION, blobRootDir.toString());
        directoryFactory.init(args);
        directoryPath = nodeConfig.getCoreRootDirectory()
                .resolve("core").resolve("data").resolve("index").toAbsolutePath().toString();
        directory = directoryFactory.get(directoryPath,
                DirectoryFactory.DirContext.DEFAULT, DirectoryFactory.LOCK_TYPE_NATIVE);
        for (String file : directory.listAll()) {
            directory.deleteFile(file);
        }
        directory.syncMetaData();
        checkDirListing();
    }

    @After
    public void cleanupDirectory() throws Exception {
        try {
            directoryFactory.doneWithDirectory(directory);
            directoryFactory.release(directory);
        } finally {
            if (directoryFactory != null) {
                directoryFactory.close();
            }
        }
    }

    @Test
    public void testWriteRead() throws IOException {
        // Given some files to write.
        String testFileName1 = "test1";
        String testContent1 = "test content 1";
        String testFileName2 = "test2";
        String testContent2 = "test content 2";

        // When we write the files and sync the local filesystem directory.
        writeFileToLocalDir(testFileName1, testContent1);
        writeFileToLocalDir(testFileName2, testContent2);
        directory.sync(Arrays.asList(testFileName1, testFileName2));
        directory.syncMetaData();

        // Then in both local and blob store:
        // - The directory lists the files.
        checkDirListing(testFileName1, testFileName2);
        // - The first file content is correct.
        checkFileContent(testFileName1, testContent1);
        // - The second file content is correct.
        checkFileContent(testFileName2, testContent2);
    }

    @Test
    public void testDelete() throws IOException {
        // Given some files written in the local filesystem directory.
        String testFileName1 = "test1";
        String testFileName2 = "test2";
        String testFileName3 = "test3";
        writeFileToLocalDir(testFileName1, "_");
        writeFileToLocalDir(testFileName2, "_");
        writeFileToLocalDir(testFileName3, "_");

        // When we delete the second file.
        directory.deleteFile(testFileName2);

        // Then we cannot sync the second file.
        assertThrows(NoSuchFileException.class,
                () -> directory.sync(Arrays.asList(testFileName1, testFileName2, testFileName3)));
        // We can sync the first and third files.
        directory.sync(Arrays.asList(testFileName1, testFileName3));
        directory.syncMetaData();
        // The directory lists the first and third files.
        checkDirListing(testFileName1, testFileName3);

        // When we delete the third file and sync.
        directory.deleteFile(testFileName3);
        directory.syncMetaData();

        // Then in both local and blob store:
        // - The directory lists only the first file.
        checkDirListing(testFileName1);
    }

    @Test
    public void testRename() throws IOException {
        // Given some files written in the local filesystem directory.
        String testFileName1 = "test1";
        String testFileName2 = "test2";
        String testFileName3 = "test3";
        writeFileToLocalDir(testFileName1, "_");
        writeFileToLocalDir(testFileName2, "_");
        writeFileToLocalDir(testFileName3, "_");

        // When we rename the first and third files.
        String testFileName1Renamed = "test1_renamed";
        String testFileName3Renamed = "test3_renamed";
        directory.rename(testFileName1, testFileName1Renamed);
        directory.rename(testFileName3, testFileName3Renamed);

        // Then we cannot sync the old name of the files.
        assertThrows(NoSuchFileException.class,
                () -> directory.sync(Arrays.asList(testFileName1, testFileName2, testFileName3)));
        // We can sync the renamed files.
        directory.sync(Arrays.asList(testFileName1Renamed, testFileName2, testFileName3Renamed));
        directory.syncMetaData();
        // The directory lists the renamed files.
        checkDirListing(testFileName1Renamed, testFileName2, testFileName3Renamed);
    }

    private void writeFileToLocalDir(String name, String content) throws IOException {
        try (IndexOutput output = directory.createOutput(name, IOContext.DEFAULT)) {
            output.writeString(content);
        }
    }

    private void checkDirListing(String... expectedFileNames) throws IOException {
        // Check the file listing in the local filesystem directory.
        assertEquals(Arrays.asList(expectedFileNames), Arrays.asList(directory.listAll()));
        // Check the file listing in the blob store.
        URI blobDirUri = directoryFactory.resolveBlobPath(directoryFactory.getLocalRelativePath(directoryPath));
        assertEquals(Arrays.asList(expectedFileNames), Arrays.asList(directoryFactory.getRepository()
                .listAll(blobDirUri)));
    }

    private void checkFileContent(String name, String expectedContent) throws IOException {
        // Check the file content in the local file system directory.
        try (IndexInput input = directory.openInput(name, IOContext.READ)) {
            assertEquals(expectedContent, input.readString());
        }
        // Check the file content in the blob store.
        URI blobDirUri = directoryFactory.resolveBlobPath(directoryFactory.getLocalRelativePath(directoryPath));
        try (IndexInput input = directoryFactory.getRepository().openInput(blobDirUri, name, IOContext.READ)) {
            assertEquals(expectedContent, input.readString());
        }
    }
}
