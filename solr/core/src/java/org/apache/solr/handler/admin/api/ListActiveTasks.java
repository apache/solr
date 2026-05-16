package org.apache.solr.handler.admin.api;

import jakarta.inject.Inject;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.ListActiveTasksApi;
import org.apache.solr.client.api.model.ListActiveTaskResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;

import java.util.Map;

import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;
import static org.apache.solr.security.PermissionNameProvider.Name.READ_PERM;

public class ListActiveTasks extends JerseyResource implements ListActiveTasksApi {

  private final CoreContainer coreContainer;

  @Inject
  public ListActiveTasks(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  @Override
  @PermissionName(READ_PERM)
  public ListActiveTaskResponse listActiveTasks(String taskUUID) throws Exception {
    if (coreContainer == null || coreContainer.isShutDown()) {
      throw new SolrException(
          SERVER_ERROR, "CoreContainer is either not initialized or shutting down");
    }

    final ListActiveTaskResponse response = instantiateJerseyResponse(ListActiveTaskResponse.class);

    response.taskList = Map.of("xyz","jalaz");

    return response;

  }
}
