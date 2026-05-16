package org.apache.solr.handler.admin.api;

import jakarta.inject.Inject;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.ListActiveTasksApi;
import org.apache.solr.client.api.model.ListActiveTaskResponse;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.solr.security.PermissionNameProvider.Name.READ_PERM;

public class ListActiveTasks extends JerseyResource implements ListActiveTasksApi {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrQueryRequest solrQueryRequest;

  @Inject
  public ListActiveTasks(
      SolrQueryRequest solrQueryRequest) {
    this.solrQueryRequest = solrQueryRequest;
  }



  @Override
  @PermissionName(READ_PERM)
  public ListActiveTaskResponse listActiveTasks(String taskUUID) throws Exception {

    final ListActiveTaskResponse response = instantiateJerseyResponse(ListActiveTaskResponse.class);
    CoreContainer coreContainer = solrQueryRequest.getCoreContainer();

    if (coreContainer.isZooKeeperAware()) {
      if (log.isDebugEnabled()) {
        log.debug("solr cloud");
      }
      handleSolrCloudMode(response, taskUUID);
    } else {
      if (log.isDebugEnabled()) {
        log.debug("standalone solr");
      }
      handleStandAloneMode(response, taskUUID);
    }

    log.debug("something random");

    return response;

  }

  private void handleStandAloneMode(ListActiveTaskResponse response, String taskUUID) {
    Iterator<Map.Entry<String, String>> iterator = solrQueryRequest.getCore().getCancellableQueryTracker().getActiveQueriesGenerated();

    Map<String, String> taskList = new HashMap<>();
    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      taskList.put(entry.getKey(), entry.getValue());
    }

    response.taskList = taskList;
  }

  private void handleSolrCloudMode(ListActiveTaskResponse response, String taskUUID) {
    Iterator<Map.Entry<String, String>> iterator = solrQueryRequest.getCore().getCancellableQueryTracker().getActiveQueriesGenerated();

    Map<String, String> taskList = new HashMap<>();
    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      taskList.put(entry.getKey(), entry.getValue());
    }

    response.taskList = taskList;
  }
}
