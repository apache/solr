package org.apache.solr.blob;

import org.apache.lucene.mockfile.FilterPath;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.*;
import org.apache.solr.core.backup.repository.LocalFileSystemRepository;
import org.junit.*;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

public class BlobDirectoryFactoryTest extends SolrTestCaseJ4 {

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

    private BlobDirectoryFactory blobDirectoryFactory;

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
    public void setupDirectoryFactory() {
        blobDirectoryFactory = new BlobDirectoryFactory();
        blobDirectoryFactory.initCoreContainer(coreContainer);
    }

    @After
    public void cleanupDirectoryFactory() throws Exception {
        if (blobDirectoryFactory != null) {
            blobDirectoryFactory.close();
        }
    }

    @Test
    public void testInitArgs() {
        // Given no init args.
        NamedList<String> args = new NamedList<>();
        // When the factory is initialized.
        // Then it throws an exception.
        expectThrows(IllegalArgumentException.class, () -> blobDirectoryFactory.init(args));

        // Given only the 'repository' arg.
        args.add(CoreAdminParams.BACKUP_REPOSITORY, REPOSITORY_NAME);
        // When the factory is initialized.
        // Then it throws an exception.
        expectThrows(IllegalArgumentException.class, () -> blobDirectoryFactory.init(args));

        // Given the 'location' args is provided.
        args.add(CoreAdminParams.BACKUP_LOCATION, blobRootDir.toString());
        // Given other optional MMapDirectory params are provided.
        args.add("mmap.maxChunkSize", Integer.toString(10));
        args.add("mmap.unmap", Boolean.toString(false));
        args.add("mmap.preload", Boolean.toString(true));
        // When the factory is initialized.
        blobDirectoryFactory.init(args);
        // Then the BackupRepository is correctly initialized.
        assertTrue(blobDirectoryFactory.getRepository() instanceof LocalFileSystemRepository);
        // Then the backup location is correctly initialized.
        assertEquals(blobRootDir, Paths.get(blobDirectoryFactory.getRepositoryLocation()));
        // Then the optional MMapDirectory params are correctly initialized.
        assertEquals(10, blobDirectoryFactory.getMMapParams().maxChunk);
        assertFalse(blobDirectoryFactory.getMMapParams().unmap);
        assertTrue(blobDirectoryFactory.getMMapParams().preload);
    }

    @Test
    public void testCleanupOldIndexDirectories() throws Exception {
        // Given a correctly initialized BlobDirectoryFactory.
        NamedList<String> args = new NamedList<>();
        args.add(CoreAdminParams.BACKUP_REPOSITORY, REPOSITORY_NAME);
        args.add(CoreAdminParams.BACKUP_LOCATION, blobRootDir.toString());
        blobDirectoryFactory.init(args);

        // Given a mock Core with 3 mock indexes. The last one is the active index.
        Path coreDataPath = nodeConfig.getCoreRootDirectory().resolve("core").resolve("data");
        String index1 = "index.00000000000000001";
        String index2 = "index.00000000000000002";
        String index3 = "index.00000000000000003";
        createMockIndex(coreDataPath, index1);
        createMockIndex(coreDataPath, index2);
        Path index3Path = createMockIndex(coreDataPath, index3);
        assertIndexList(coreDataPath, index1, index2, index3);

        // When the old indexes are cleaned up after a core reload
        // (after a core reload the index just before the last index is not deleted).
        blobDirectoryFactory.cleanupOldIndexDirectories(coreDataPath.toString(), index3Path.toString(), true);
        // Then the oldest index 1 is deleted.
        assertIndexList(coreDataPath, index2, index3);

        // When the old indexes are cleaned up again after a core reload.
        blobDirectoryFactory.cleanupOldIndexDirectories(coreDataPath.toString(), index3Path.toString(), true);
        // Then this is a noop, there still remain index 2 and 3.
        assertIndexList(coreDataPath, index2, index3);

        // When the old indexes are cleaned up *not* after a core reload.
        blobDirectoryFactory.cleanupOldIndexDirectories(coreDataPath.toString(), index3Path.toString(), false);
        // Then the old index 2 is deleted.
        assertIndexList(coreDataPath, index3);

        // When the old indexes are cleaned up again *not* after a core reload.
        blobDirectoryFactory.cleanupOldIndexDirectories(coreDataPath.toString(), index3Path.toString(), false);
        // Then this is a noop, there still remain index 3.
        assertIndexList(coreDataPath, index3);
    }

    private Path createMockIndex(Path coreDataPath, String indexName) throws IOException {
        Path indexPath = coreDataPath.resolve(indexName);
        Directory directory = blobDirectoryFactory.get(indexPath.toString(), org.apache.solr.core.DirectoryFactory.DirContext.DEFAULT, org.apache.solr.core.DirectoryFactory.LOCK_TYPE_NATIVE);
        try {
            String testFileName = "test";
            String testContent = "test content";
            try (IndexOutput output = directory.createOutput(testFileName, IOContext.DEFAULT)) {
                output.writeString(testContent);
            }
            directory.sync(Collections.singletonList(testFileName));
            directory.syncMetaData();
            try (IndexInput input = directory.openInput(testFileName, IOContext.READ)) {
                assertEquals(testContent, input.readString());
            }
            assertEquals(Collections.singletonList(testFileName), Arrays.asList(directory.listAll()));
        } finally {
            blobDirectoryFactory.release(directory);
            blobDirectoryFactory.doneWithDirectory(directory);
        }
        return indexPath;
    }

    private void assertIndexList(Path coreDataPath, String... expectedIndexes) throws IOException {
        Directory directory = blobDirectoryFactory.get(coreDataPath.toString(), org.apache.solr.core.DirectoryFactory.DirContext.META_DATA, org.apache.solr.core.DirectoryFactory.LOCK_TYPE_NATIVE);
        try {
            assertEquals(Arrays.asList(expectedIndexes), Arrays.asList(directory.listAll()));
        } finally {
            blobDirectoryFactory.release(directory);
        }
    }
}
