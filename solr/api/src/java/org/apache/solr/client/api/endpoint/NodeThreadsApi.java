package org.apache.solr.client.api.endpoint;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import org.apache.solr.client.api.model.NodeThreadsResponse;

@Path("/node/threads")
public interface NodeThreadsApi {

  @GET
  NodeThreadsResponse getThreadDump();
}
