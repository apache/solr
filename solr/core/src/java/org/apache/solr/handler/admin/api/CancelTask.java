package org.apache.solr.handler.admin.api;

import static org.apache.solr.security.PermissionNameProvider.Name.READ_PERM;

import jakarta.inject.Inject;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.TasksApi;
import org.apache.solr.client.api.model.CancelTaskResponse;
import org.apache.solr.handler.component.ActiveTaskQuerySupport;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.CancellableCollector;

public class CancelTask extends JerseyResource implements TasksApi.Cancel {

  private final SolrQueryRequest solrQueryRequest;

  @Inject
  public CancelTask(SolrQueryRequest solrQueryRequest) {
    this.solrQueryRequest = solrQueryRequest;
  }

  @Override
  @PermissionName(READ_PERM)
  public CancelTaskResponse cancelRunningTask(String taskID) throws Exception {
    final CancelTaskResponse response = instantiateJerseyResponse(CancelTaskResponse.class);

    boolean isTaskCancelled = ActiveTaskQuerySupport.cancelTask(solrQueryRequest, taskID);

    response.status =
          (isTaskCancelled)
            ? CancelTaskResponse.CancellationStatus.SUCCESS
            : CancelTaskResponse.CancellationStatus.NOT_FOUND;

    return response;
  }

  public static boolean cancelTaskActiveOnThisShard(SolrQueryRequest solrQueryRequest, String taskId) {
    CancellableCollector cancellableTask = solrQueryRequest.getCore().getCancellableQueryTracker().getCancellableTask(taskId);
    if (cancellableTask != null) {
      cancellableTask.cancel();
      return true;
    }
    return false;
  }

}
