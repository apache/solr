package org.apache.solr.client.api.endpoint;

import static org.apache.solr.client.api.util.Constants.INDEX_PATH_PREFIX;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import org.apache.solr.client.api.model.ParamSetsResponse;
import org.apache.solr.client.api.util.StoreApiParameters;

public interface ParamSetsApi {

  @Path(INDEX_PATH_PREFIX + "/config/paramsets")
  interface Get {
    @GET
    @StoreApiParameters
    @Operation(
        summary = "Fetch all request param sets",
        tags = {"param-sets"})
    ParamSetsResponse getParamSets();

    @GET
    @Path("/{paramSetName}")
    @StoreApiParameters
    @Operation(
        summary = "Fetch a single request param set",
        tags = {"param-sets"})
    ParamSetsResponse getParamSets(
        @PathParam("paramSetName") @Parameter(description = "Name of the param set") String paramSetName);
  }
}
