package org.apache.solr.handler.admin.api;

import jakarta.inject.Inject;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.ListActiveTasksApi;
import org.apache.solr.client.api.model.ListActiveTaskResponse;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.solr.security.PermissionNameProvider.Name.COLL_READ_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.READ_PERM;

public class ListActiveTasks extends JerseyResource implements ListActiveTasksApi {

  private final SolrQueryRequest solrQueryRequest;

  @Inject
  public ListActiveTasks(
      SolrQueryRequest solrQueryRequest) {
    this.solrQueryRequest = solrQueryRequest;
  }



  @Override
  @PermissionName(COLL_READ_PERM)
  public ListActiveTaskResponse listActiveTasks(String taskUUID) throws Exception {

    final ListActiveTaskResponse response = instantiateJerseyResponse(ListActiveTaskResponse.class);

    Iterator<Map.Entry<String, String>> iterator = solrQueryRequest.getCore().getCancellableQueryTracker().getActiveQueriesGenerated();

    Map<String, String> taskList = new HashMap<>();
    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      taskList.put(entry.getKey(), entry.getValue());
    }

    response.taskList = taskList;

    return response;

  }
}
