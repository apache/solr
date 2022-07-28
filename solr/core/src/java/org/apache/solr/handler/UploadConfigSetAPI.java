package org.apache.solr.handler;


import org.apache.commons.io.IOUtils;
import org.apache.solr.api.EndPoint;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.PUT;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;

public class UploadConfigSetAPI extends ConfigSetAPI {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public UploadConfigSetAPI(CoreContainer coreContainer) {
        super(coreContainer);
    }

    @EndPoint(method = PUT, path = "/cluster/configs/{name}", permission = CONFIG_EDIT_PERM)
    public void uploadConfigSet(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        ensureConfigSetUploadEnabled();

        ConfigSetService configSetService = coreContainer.getConfigSetService();

        final String configSetName = req.getPathTemplateValues().get("name");
        boolean overwritesExisting = configSetService.checkConfigExists(configSetName);
        boolean requestIsTrusted = isTrusted(req.getUserPrincipal(), coreContainer.getAuthenticationPlugin());
        // Get upload parameters
        String singleFilePath = req.getParams().get(ConfigSetParams.FILE_PATH, "");
        boolean allowOverwrite = req.getParams().getBool(ConfigSetParams.OVERWRITE, false);
        boolean cleanup = req.getParams().getBool(ConfigSetParams.CLEANUP, false);
        final InputStream inputStream = ensureNonEmptyInputStream(req);

        if (overwritesExisting && !allowOverwrite) {
            throw new SolrException(
                    SolrException.ErrorCode.BAD_REQUEST,
                    "The configuration " + configSetName + " already exists in zookeeper");
        }

        List<String> filesToDelete;
        if (overwritesExisting && cleanup) {
            filesToDelete = configSetService.getAllConfigFiles(configSetName);
        } else {
            filesToDelete = Collections.emptyList();
        }

        // Create a node for the configuration in zookeeper
        // For creating the baseZnode, the cleanup parameter is only allowed to be true when
        // singleFilePath is not passed.
        createBaseNode(configSetService, overwritesExisting, requestIsTrusted, configSetName);

        try (ZipInputStream zis = new ZipInputStream(inputStream, StandardCharsets.UTF_8)) {
            boolean hasEntry = false;
            ZipEntry zipEntry;
            while ((zipEntry = zis.getNextEntry()) != null) {
                hasEntry = true;
                String filePath = zipEntry.getName();
                filesToDelete.remove(filePath);
                if (!zipEntry.isDirectory()) {
                    configSetService.uploadFileToConfig(
                            configSetName, filePath, IOUtils.toByteArray(zis), true);
                }
            }
            if (!hasEntry) {
                throw new SolrException(
                        SolrException.ErrorCode.BAD_REQUEST,
                        "Either empty zipped data, or non-zipped data was uploaded. In order to upload a configSet, you must zip a non-empty directory to upload.");
            }
        }
        deleteUnusedFiles(configSetService, configSetName, filesToDelete);

        // If the request is doing a full trusted overwrite of an untrusted configSet (overwrite=true,
        // cleanup=true), then trust the configSet.
        if (cleanup
                && requestIsTrusted
                && overwritesExisting
                && !isCurrentlyTrusted(configSetName)) {
            Map<String, Object> metadata = Collections.singletonMap("trusted", true);
            configSetService.setConfigMetadata(configSetName, metadata);
        }
    }

    private void deleteUnusedFiles(
            ConfigSetService configSetService, String configName, List<String> filesToDelete)
            throws IOException {
        if (!filesToDelete.isEmpty()) {
            if (log.isInfoEnabled()) {
                log.info("Cleaning up {} unused files", filesToDelete.size());
            }
            if (log.isDebugEnabled()) {
                log.debug("Cleaning up unused files: {}", filesToDelete);
            }
            configSetService.deleteFilesFromConfig(configName, filesToDelete);
        }
    }
}
