package org.apache.solr.handler.replication;

import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/** V2 endpoint for Backup API used for User-Managed clusters and Single-Node Installation. */
@Path("/cores/{cores}/replication/backups")
public class BackupAPI extends JerseyResource {
  private final SolrQueryRequest solrQueryRequest;
  private final SolrQueryResponse solrQueryResponse;
  private final ReplicationHandler replicationHandler;

  @PathParam("cores")
  private String coreName;

  @Inject
  public BackupAPI(
      CoreContainer cc, SolrQueryRequest solrQueryRequest, SolrQueryResponse solrQueryResponse) {
    this.replicationHandler =
        (ReplicationHandler) cc.getCore(coreName).getRequestHandler(ReplicationHandler.PATH);
    this.solrQueryRequest = solrQueryRequest;
    this.solrQueryResponse = solrQueryResponse;
  }
  /**
   * This API (POST /api/cores/coreName/replication/backups {...}) is analogous to the v1
   * /solr/coreName/replication?command=backup
   */
  @POST
  @Produces({MediaType.APPLICATION_JSON})
  @Operation(
      summary = "Backup command using ReplicationHandler",
      tags = {"cores"})
  @PermissionName(CORE_EDIT_PERM)
  public SolrJerseyResponse createBackup(
      @RequestBody BackupReplicationPayload backupReplicationPayload) throws Exception {
    final SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);
    final Map<String, Object> v1Params = backupReplicationPayload.toMap(new HashMap<>());
    v1Params.put(ReplicationHandler.COMMAND, ReplicationHandler.CMD_BACKUP);
    replicationHandler.handleRequestBody(wrapParams(solrQueryRequest, v1Params), solrQueryResponse);
    return response;
  }

  /* POJO for v2 endpoints request body.*/
  public static class BackupReplicationPayload implements JacksonReflectMapWriter {

    public BackupReplicationPayload() {}

    public BackupReplicationPayload(
        String location, String name, int numberToKeep, String repository, String commitName) {
      this.location = location;
      this.name = name;
      this.numberToKeep = numberToKeep;
      this.repository = repository;
      this.commitName = commitName;
    }

    @Schema(description = "The path where the backup will be created")
    @JsonProperty
    public String location;

    @Schema(description = "The backup will be created in a directory called snapshot.<name>")
    @JsonProperty
    public String name;

    @Schema(description = "The number of backups to keep.")
    @JsonProperty
    public int numberToKeep;

    @Schema(description = "The name of the repository to be used for e backup.")
    @JsonProperty
    public String repository;

    @Schema(
        description =
            "The name of the commit which was used while taking a snapshot using the CREATESNAPSHOT command.")
    @JsonProperty
    public String commitName;
  }
}
