package org.apache.solr.handler.admin.api;

import jakarta.inject.Inject;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.ListActiveTasksApi;
import org.apache.solr.client.api.model.ListActiveTaskResponse;
import org.apache.solr.jersey.PermissionName;

import java.util.Map;

import static org.apache.solr.security.PermissionNameProvider.Name.READ_PERM;

public class ListActiveTasks extends JerseyResource implements ListActiveTasksApi {

  @Inject
  public ListActiveTasks() {

  }

  @Override
  @PermissionName(READ_PERM)
  public ListActiveTaskResponse listActiveTasks(String taskUUID) throws Exception {

    final ListActiveTaskResponse response = instantiateJerseyResponse(ListActiveTaskResponse.class);

    response.taskList = Map.of("xyz","jalaz", "mno", "pqr");

    return response;

  }
}
