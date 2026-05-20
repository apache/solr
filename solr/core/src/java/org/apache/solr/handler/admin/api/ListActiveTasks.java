package org.apache.solr.handler.admin.api;

import jakarta.inject.Inject;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.ListActiveTasksApi;
import org.apache.solr.client.api.model.ActiveTaskDetails;
import org.apache.solr.client.api.model.ListActiveTaskResponse;
import org.apache.solr.client.api.model.TaskStatusResponse;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.solr.security.PermissionNameProvider.Name.READ_PERM;

public class ListActiveTasks extends JerseyResource implements ListActiveTasksApi {

  private final SolrQueryRequest solrQueryRequest;

  @Inject
  public ListActiveTasks(
      SolrQueryRequest solrQueryRequest) {
    this.solrQueryRequest = solrQueryRequest;
  }

  @Override
  @PermissionName(READ_PERM)
  public ListActiveTaskResponse listAllActiveTasks() throws Exception {
    final ListActiveTaskResponse response = instantiateJerseyResponse(ListActiveTaskResponse.class);

    response.taskList = extractActiveTaskLists();

    return response;
  }

  @Override
  @PermissionName(READ_PERM)
  public TaskStatusResponse getTaskStatus(String taskUUID) throws Exception {
    final TaskStatusResponse response = instantiateJerseyResponse(TaskStatusResponse.class);

    response.taskStatus = solrQueryRequest.getCore().getCancellableQueryTracker().isQueryIdActive(taskUUID);

    return response;
  }

  private List<ActiveTaskDetails> extractActiveTaskLists() {
    Iterator<Map.Entry<String, String>> iterator = solrQueryRequest.getCore().getCancellableQueryTracker().getActiveQueriesGenerated();

    List<ActiveTaskDetails> activeTaskDetails = new ArrayList<>();
    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      activeTaskDetails.add(new ActiveTaskDetails(entry.getKey(), entry.getValue()));
    }

    return activeTaskDetails;
  }

}
