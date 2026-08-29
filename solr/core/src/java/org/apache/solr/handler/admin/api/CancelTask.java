package org.apache.solr.handler.admin.api;

import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.TasksApi;
import org.apache.solr.client.api.model.TaskStatusResponse;

public class CancelTask extends JerseyResource implements TasksApi.Cancel {

  @Override
  public TaskStatusResponse cancelRunningTask(String taskID) throws Exception {
    return null;
  }
}
