package org.apache.solr.handler;

import org.apache.commons.io.IOUtils;
import org.apache.solr.api.EndPoint;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.io.InputStream;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.PUT;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;

public class UpdateConfigSetFileAPI extends ConfigSetAPI {

    public UpdateConfigSetFileAPI(CoreContainer coreContainer) {
        super(coreContainer);
    }

    @EndPoint(method = PUT, path = "/cluster/configs/{name}/*", permission = CONFIG_EDIT_PERM)
    public void updateConfigSetFile(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
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

        String fixedSingleFilePath = singleFilePath;
        if (fixedSingleFilePath.charAt(0) == '/') {
            fixedSingleFilePath = fixedSingleFilePath.substring(1);
        }
        if (fixedSingleFilePath.isEmpty()) {
            throw new SolrException(
                    SolrException.ErrorCode.BAD_REQUEST,
                    "The file path provided for upload, '" + singleFilePath + "', is not valid.");
        } else if (cleanup) {
            // Cleanup is not allowed while using singleFilePath upload
            throw new SolrException(
                    SolrException.ErrorCode.BAD_REQUEST,
                    "ConfigSet uploads do not allow cleanup=true when file path is used.");
        } else {
            // Create a node for the configuration in config
            // For creating the baseNode, the cleanup parameter is only allowed to be true when
            // singleFilePath is not passed.
            createBaseNode(configSetService, overwritesExisting, requestIsTrusted, configSetName);
            configSetService.uploadFileToConfig(
                    configSetName, fixedSingleFilePath, IOUtils.toByteArray(inputStream), allowOverwrite);
        }
    }
}
