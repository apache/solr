package org.apache.solr.client.api.endpoint;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import org.apache.solr.client.api.model.ListActiveTaskResponse;
import org.apache.solr.client.api.util.StoreApiParameters;

import static org.apache.solr.client.api.util.Constants.INDEX_PATH_PREFIX;

@Path(INDEX_PATH_PREFIX + "/tasks/listjalaz")
public interface ListActiveTasksApi {
  @GET
  @StoreApiParameters
  @Operation(
      summary = "Lists all the currently running tasks",
      tags = {"tasks"})
  ListActiveTaskResponse listActiveTasks(
      @QueryParam("taskUUID") String taskUUID) throws Exception;
}
