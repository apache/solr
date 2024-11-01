package org.apache.solr.client.api.endpoint;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import org.apache.solr.client.api.model.ListClusterPropertiesResponse;
import org.apache.solr.client.api.model.SetClusterPropertyRequestBody;
import org.apache.solr.client.api.model.SetNestedClusterPropertyRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;

/** Definitions for v2 JAX-RS cluster properties APIs. */
@Path("/cluster/properties")
public interface ClusterPropertyApis {
  @GET
  @Operation(
      summary = "List all cluster properties in this Solr cluster.",
      tags = {"cluster-properties"})
  ListClusterPropertiesResponse listClusterProperties();

  @GET
  @Path("/{propertyName}")
  @Operation(
      summary = "Get a cluster property in this Solr cluster.",
      tags = {"cluster-properties"})
  SolrJerseyResponse getClusterProperty(
      @Parameter(description = "The name of the property being retrieved.", required = true)
          @PathParam("propertyName")
          String propertyName);

  @PUT
  @Path("/{propertyName}")
  @Operation(
      summary = "Set a single new or existing cluster property in this Solr cluster.",
      tags = {"cluster-properties"})
  SolrJerseyResponse createOrUpdateClusterProperty(
      @Parameter(description = "The name of the property being set.", required = true)
          @PathParam("propertyName")
          String propertyName,
      @RequestBody(description = "Value to set for the property", required = true)
          SetClusterPropertyRequestBody requestBody)
      throws Exception;

  @PUT
  @Operation(
      summary = "Set nested cluster properties in this Solr cluster.",
      tags = {"cluster-properties"})
  SolrJerseyResponse createOrUpdateNestedClusterProperty(
      @RequestBody(description = "Property/ies to be set", required = true)
          SetNestedClusterPropertyRequestBody requestBody)
      throws Exception;

  @DELETE
  @Path("/{propertyName}")
  @Operation(
      summary = "Delete a cluster property in this Solr cluster.",
      tags = {"cluster-properties"})
  SolrJerseyResponse deleteClusterProperty(
      @Parameter(description = "The name of the property being deleted.", required = true)
          @PathParam("propertyName")
          String propertyName);
}
