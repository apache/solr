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
        String configName = "testconfig";

        fileSystemConfigSetService.uploadConfig(configName, configset("cloud-minimal"));

        assertEquals(fileSystemConfigSetService.listConfigs().size(), 1);
        assertTrue(fileSystemConfigSetService.checkConfigExists(configName));

        byte[] testdata = "test data".getBytes(StandardCharsets.UTF_8);
        fileSystemConfigSetService.uploadFileToConfig(configName, "testfile", testdata, true);


        // metadata is stored in .metadata.json
        fileSystemConfigSetService.setConfigMetadata(configName, Map.of("key1", "val1"));
        Map<String, Object> metadata = fileSystemConfigSetService.getConfigMetadata(configName);
        assertEquals(metadata.toString(), "{key1=val1}");

        List<String> allConfigFiles = fileSystemConfigSetService.getAllConfigFiles(configName);
        assertEquals(allConfigFiles.toString(), "[schema.xml, solrconfig.xml, testfile]");

        fileSystemConfigSetService.deleteFilesFromConfig(configName, List.of(METADATA_FILE, "testfile"));
        metadata = fileSystemConfigSetService.getConfigMetadata(configName);
        assertTrue(metadata.isEmpty());

        allConfigFiles = fileSystemConfigSetService.getAllConfigFiles(configName);
        assertEquals(allConfigFiles.toString(), "[schema.xml, solrconfig.xml]");

        fileSystemConfigSetService.copyConfig(configName, "copytestconfig");
        assertEquals(fileSystemConfigSetService.listConfigs().size(), 2);

        allConfigFiles = fileSystemConfigSetService.getAllConfigFiles("copytestconfig");
        assertEquals(allConfigFiles.toString(), "[schema.xml, solrconfig.xml]");
    }

    @Test
    public void testDeleteConfig() throws IOException {
        String configName = "testconfig";

        fileSystemConfigSetService.deleteConfig(configName);
        fileSystemConfigSetService.deleteConfig("copytestconfig");

        assertFalse(fileSystemConfigSetService.checkConfigExists(configName));
        assertFalse(fileSystemConfigSetService.checkConfigExists("copytestconfig"));
    }

}
