package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import org.apache.solr.client.api.util.ReflectWritable;

/** Response body for the `GET /api/cores/coreName/replication/commits` API */
public class CommitsResponse extends SolrJerseyResponse {

  @JsonProperty("commits")
  @Schema(description = "Index commit points currently retained by the deletion policy")
  public List<CommitDetails> commits;

  @JsonProperty("status")
  @Schema(description = "Request status, typically OK")
  public String status;

  /** Details of a single index commit point. */
  public static class CommitDetails implements ReflectWritable {
    @JsonProperty("indexVersion")
    public Long indexVersion;

    @JsonProperty("generation")
    public Long generation;

    @JsonProperty("filelist")
    public List<String> filelist;

    public CommitDetails() {}

    public CommitDetails(long l, long l1, List<String> segments7) {
      this.indexVersion = l;
      this.generation = l1;
      this.filelist = segments7;
    }
  }
}
