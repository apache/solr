package org.apache.solr.blob;

import org.apache.lucene.mockfile.FilterPath;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.*;
import org.junit.*;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;

public class BlobDirectoryTest extends SolrTestCaseJ4 {

    private static final String SOLR_CONFIG = "<solr></solr>";

    private static Path blobRootDir;
    private static NodeConfig nodeConfig;
    private static CoreContainer coreContainer;

    private DirectoryFactory directoryFactory;
    private Directory directory;

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
        args.add("delegateFactory", MMapDirectoryFactory.class.getName());
        args.add("blobRootDir", blobRootDir.toString());
        directoryFactory.init(args);
        Path coreIndexPath = nodeConfig.getCoreRootDirectory().resolve("core").resolve("data").resolve("index");
        directory = directoryFactory.get(coreIndexPath.toString(), DirectoryFactory.DirContext.DEFAULT, DirectoryFactory.LOCK_TYPE_NATIVE);
        for (String file : directory.listAll()) {
            directory.deleteFile(file);
        }
        directory.syncMetaData();
        assertEquals(0, directory.listAll().length);
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

        // When we write the files and sync the directory.
        writeFile(testFileName1, testContent1);
        writeFile(testFileName2, testContent2);
        directory.sync(Arrays.asList(testFileName1, testFileName2));
        directory.syncMetaData();

        // Then the directory lists the files.
        checkList(testFileName1, testFileName2);
        // Then we read the first file content and it is correct.
        readFile(testFileName1, testContent1);
        // Then we read the second file content and it is correct.
        readFile(testFileName2, testContent2);
    }

    @Test
    public void testDelete() throws IOException {
        // Given some files written in the directory.
        String testFileName1 = "test1";
        String testFileName2 = "test2";
        String testFileName3 = "test3";
        writeFile(testFileName1, "_");
        writeFile(testFileName2, "_");
        writeFile(testFileName3, "_");

        // When we delete the second file.
        directory.deleteFile(testFileName2);

        // Then we cannot sync the second file.
        assertThrows(NoSuchFileException.class, () -> directory.sync(Arrays.asList(testFileName1, testFileName2, testFileName3)));
        // Then we can sync the first and third files.
        directory.sync(Arrays.asList(testFileName1, testFileName3));
        directory.syncMetaData();
        // Then the directory lists the first and third files.
        checkList(testFileName1, testFileName3);

        // When we delete the third file and sync.
        directory.deleteFile(testFileName3);
        directory.syncMetaData();
        // Then the directory lists the first file.
        checkList(testFileName1);
    }

    @Test
    public void testRename() throws IOException {
        //TODO
    }

    private void writeFile(String name, String content) throws IOException {
        try (IndexOutput output = directory.createOutput(name, IOContext.DEFAULT)) {
            output.writeString(content);
        }
    }

    private void readFile(String name, String expectedContent) throws IOException {
        try (IndexInput input = directory.openInput(name, IOContext.READ)) {
            assertEquals(expectedContent, input.readString());
        }
    }

    private void checkList(String... expectedFileNames) throws IOException {
        assertEquals(Arrays.asList(expectedFileNames), Arrays.asList(directory.listAll()));
    }
}
