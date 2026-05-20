package org.apache.solr.client.api.endpoint;

import io.swagger.v3.oas.annotations.Operation;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import org.apache.solr.client.api.model.ListActiveTaskResponse;
import org.apache.solr.client.api.model.TaskStatusResponse;
import org.apache.solr.client.api.util.StoreApiParameters;

import static org.apache.solr.client.api.util.Constants.INDEX_PATH_PREFIX;

@Path(INDEX_PATH_PREFIX + "/tasks/list")
public interface ListActiveTasksApi {

  // Handles: .../tasks/list (Lists all)
  @GET
  @StoreApiParameters
  @Operation(summary = "Lists all the currently running tasks", tags = {"tasks"})
  ListActiveTaskResponse listAllActiveTasks() throws Exception;

  // Handles: .../tasks/list/slow-task-id (Lists specific)
  @GET
  @Path("/{taskUUID}")
  @StoreApiParameters
  @Operation(summary = "Status of a specific taskUUID passed as pathParam", tags = {"tasks"})
  TaskStatusResponse getTaskStatus(
      @PathParam("taskUUID") String taskUUID) throws Exception;
}
