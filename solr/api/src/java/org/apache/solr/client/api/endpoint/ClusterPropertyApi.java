package org.apache.solr.client.api.endpoint;

import io.swagger.v3.oas.annotations.Operation;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.UpdateClusterPropertyRequestBody;

import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.util.Map;

/** V2 API definitions for modifying cluster-level properties. */
@Path("/cluster/properties")
public interface ClusterPropertyApi {

    @PUT
    @Path("/{propName}")
    @Operation(
            summary = "Create or update a single cluster property",
            tags = {"cluster-properties"})
    SolrJerseyResponse updateClusterProperty(
            @PathParam("propName") String propName,
            UpdateClusterPropertyRequestBody requestBody)
            throws Exception;

    @DELETE
    @Path("/{propName}")
    @Operation(
            summary = "Delete a single cluster property",
            tags = {"cluster-properties"})
    SolrJerseyResponse deleteClusterProperty(@PathParam("propName") String propName)
            throws Exception;

    @PUT
    @Operation(
            summary = "Update one or more cluster properties",
            tags = {"cluster-properties"})
    SolrJerseyResponse updateClusterProperties(Map<String, Object> requestBody) throws Exception;
}
