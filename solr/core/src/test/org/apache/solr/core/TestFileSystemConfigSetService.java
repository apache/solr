package org.apache.solr.core;

import org.apache.commons.io.file.PathUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.apache.solr.core.FileSystemConfigSetService.METADATA_FILE;

public class TestFileSystemConfigSetService extends SolrTestCaseJ4 {
    private static Path configSetBase;
    private static FileSystemConfigSetService fileSystemConfigSetService;
    private final String CONFIGNAME = "testconfig";

    @BeforeClass
    public static void beforeClass() throws Exception {
        configSetBase = createTempDir();
        fileSystemConfigSetService = new FileSystemConfigSetService(configSetBase);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        PathUtils.deleteDirectory(configSetBase);
        fileSystemConfigSetService = null;
    }

    @Test
    public void testUploadConfig() throws IOException {

        fileSystemConfigSetService.uploadConfig(CONFIGNAME, configset("cloud-minimal"));

        assertEquals(fileSystemConfigSetService.listConfigs().size(), 1);
        assertTrue(fileSystemConfigSetService.checkConfigExists(CONFIGNAME));

        byte[] testdata = "test data".getBytes(StandardCharsets.UTF_8);
        fileSystemConfigSetService.uploadFileToConfig(CONFIGNAME, "testfile", testdata, true);


        // metadata is stored in .metadata.json
        fileSystemConfigSetService.setConfigMetadata(CONFIGNAME, Map.of("key1", "val1"));
        Map<String, Object> metadata = fileSystemConfigSetService.getConfigMetadata(CONFIGNAME);
        assertEquals(metadata.toString(), "{key1=val1}");

        List<String> allConfigFiles = fileSystemConfigSetService.getAllConfigFiles(CONFIGNAME);
        assertEquals(allConfigFiles.toString(), "[schema.xml, solrconfig.xml, testfile]");

        fileSystemConfigSetService.deleteFilesFromConfig(CONFIGNAME, List.of(METADATA_FILE, "testfile"));
        metadata = fileSystemConfigSetService.getConfigMetadata(CONFIGNAME);
        assertTrue(metadata.isEmpty());

        allConfigFiles = fileSystemConfigSetService.getAllConfigFiles(CONFIGNAME);
        assertEquals(allConfigFiles.toString(), "[schema.xml, solrconfig.xml]");

        fileSystemConfigSetService.copyConfig(CONFIGNAME, "copytestconfig");
        assertEquals(fileSystemConfigSetService.listConfigs().size(), 2);

        allConfigFiles = fileSystemConfigSetService.getAllConfigFiles("copytestconfig");
        assertEquals(allConfigFiles.toString(), "[schema.xml, solrconfig.xml]");
    }

    @Test
    public void testDeleteConfig() throws IOException {
        fileSystemConfigSetService.deleteConfig(CONFIGNAME);
        fileSystemConfigSetService.deleteConfig("copytestconfig");

        assertFalse(fileSystemConfigSetService.checkConfigExists(CONFIGNAME));
        assertFalse(fileSystemConfigSetService.checkConfigExists("copytestconfig"));
    }

}
