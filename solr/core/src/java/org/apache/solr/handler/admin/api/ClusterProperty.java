package org.apache.solr.handler.admin.api;

import org.apache.solr.client.api.endpoint.ClusterPropertyApi;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.UpdateClusterPropertyRequestBody;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.PermissionNameProvider;

import javax.inject.Inject;
import java.util.Map;

/**
 * V2 API implementations for modifying cluster-level properties
 *
 * <p>These APIs are analogous to the v1 /admin/collections?action=CLUSTERPROP command.
 */
public class ClusterProperty extends AdminAPIBase implements ClusterPropertyApi {

    @Inject
    public ClusterProperty(
            CoreContainer coreContainer,
            SolrQueryRequest solrQueryRequest,
            SolrQueryResponse solrQueryResponse) {
        super(coreContainer, solrQueryRequest, solrQueryResponse);
    }

    @Override
    @PermissionName(PermissionNameProvider.Name.COLL_EDIT_PERM)
    public SolrJerseyResponse updateClusterProperty(String propName, UpdateClusterPropertyRequestBody requestBody) throws Exception {
        if (requestBody == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing required request body");
        }
        final SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);

        final var clusterProperties = readCurrentClusterProperties();
        clusterProperties.setClusterProperty(propName, requestBody.value);
        return response;
    }

    @Override
    @PermissionName(PermissionNameProvider.Name.COLL_EDIT_PERM)
    public SolrJerseyResponse deleteClusterProperty(String propName) throws Exception {
        final SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);

        final var clusterProperties = readCurrentClusterProperties();
        clusterProperties.setClusterProperty(propName, null);
        return response;
    }

    @Override
    @PermissionName(PermissionNameProvider.Name.COLL_EDIT_PERM)
    public SolrJerseyResponse updateClusterProperties(Map<String, Object> requestBody) throws Exception {
        if (requestBody == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing required request body");
        }
        final SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);

        final var clusterProperties = readCurrentClusterProperties();
        try {
            clusterProperties.setClusterProperties(requestBody);
        } catch (Exception e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error in API", e);
        }
        return response;
    }

    private ClusterProperties readCurrentClusterProperties() {
        return new ClusterProperties(coreContainer.getZkController().getZkClient());
    }
}
