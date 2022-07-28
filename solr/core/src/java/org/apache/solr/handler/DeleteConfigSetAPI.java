package org.apache.solr.handler;

import com.google.common.collect.Maps;
import org.apache.solr.api.EndPoint;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.util.Map;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.DELETE;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;

public class DeleteConfigSetAPI extends ConfigSetAPI {

    public static final String CONFIGSET_NAME_PLACEHOLDER = "name";

    public DeleteConfigSetAPI(CoreContainer coreContainer) {
        super(coreContainer);
    }

    @EndPoint(method = DELETE, path = "/cluster/configs/{" + CONFIGSET_NAME_PLACEHOLDER + "}", permission = CONFIG_EDIT_PERM)
    public void deleteConfigSet(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
        final String configSetName = req.getPathTemplateValues().get("name");
        if (StringUtils.isEmpty(configSetName)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No configset name provided to delete");
        }
        final Map<String, Object> configsetCommandMsg = Maps.newHashMap();
        configsetCommandMsg.put(NAME, configSetName);

        runConfigSetCommand(rsp, ConfigSetsHandler.ConfigSetOperation.DELETE_OP, configsetCommandMsg);
    }
}
