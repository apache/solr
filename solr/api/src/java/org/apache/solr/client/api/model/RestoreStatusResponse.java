package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.solr.client.api.util.ReflectWritable;

/** Response body for the `GET /api/cores/coreName/replication/restore` API */
public class RestoreStatusResponse extends SolrJerseyResponse {

  @JsonProperty("restorestatus")
  @Schema(description = "State of the most recent restore operation")
  public RestoreStatus restoreStatus;

  @JsonProperty("status")
  @Schema(description = "Request status, typically OK")
  public String status;

  /** State of a restore operation. */
  public static class RestoreStatus implements ReflectWritable {
    @JsonProperty("snapshotName")
    public String snapshotName;

    @JsonProperty("status")
    @Schema(description = "State of the restore operation")
    public RestoreState status;

    @JsonProperty("exception")
    @Schema(description = "Failure message, present only when the restore failed with an exception")
    public String exception;
  }

  /** State of a restore operation. */
  public enum RestoreState {
    NOT_STARTED("No restore actions in progress"),
    IN_PROGRESS("In Progress"),
    SUCCESS("success"),
    FAILED("failed");

    private final String key;

    RestoreState(String key) {
      this.key = key;
    }

    @JsonValue
    public String getKey() {
      return key;
    }
  }
}
