/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import org.apache.solr.client.api.model.CommitsResponse.CommitDetails;
import org.apache.solr.client.api.util.ReflectWritable;

public class ReplicationDetailsResponse extends SolrJerseyResponse {

  @JsonProperty("status")
  @Schema(description = "Request status, typically OK")
  public String status;

  @JsonProperty("details")
  @Schema(description = "Replication configuration and status details")
  public ReplicationDetails details;

  public ReplicationDetailsResponse() {}

  public ReplicationDetailsResponse(ReplicationDetails details, String status) {
    this.details = details;
    this.status = status;
  }

  /** Top-level replication details payload (nested under {@code details} in the response). */
  public static class ReplicationDetails implements ReflectWritable {
    @JsonProperty("indexSize")
    public String indexSize;

    @JsonProperty("indexPath")
    public String indexPath;

    @JsonProperty("commits")
    public List<CommitDetails> commits;

    @JsonProperty("isLeader")
    public Boolean isLeader;

    @JsonProperty("isFollower")
    public Boolean isFollower;

    @JsonProperty("indexVersion")
    public Long indexVersion;

    @JsonProperty("generation")
    public Long generation;

    @JsonProperty("leader")
    public LeaderDetails leader;

    @JsonProperty("follower")
    public FollowerDetails follower;

    /** Optional snapshot/backup stats (heterogeneous NamedList from the handler). */
    @JsonProperty("backup")
    public Object backup;
  }

  public static class LeaderDetails implements ReflectWritable {
    @JsonProperty("confFiles")
    public String confFiles;

    @JsonProperty("replicateAfter")
    public List<String> replicateAfter;

    @JsonProperty("replicationEnabled")
    public Boolean replicationEnabled;

    @JsonProperty("replicableVersion")
    public Long replicableVersion;

    @JsonProperty("replicableGeneration")
    public Long replicableGeneration;
  }

  public static class FollowerDetails implements ReflectWritable {
    /** Nested leader details (remote NamedList or map), present when follower=true. */
    @JsonProperty("leaderDetails")
    public Object leaderDetails;

    @JsonProperty("ERROR")
    public String error;

    @JsonProperty("leaderUrl")
    public String leaderUrl;

    @JsonProperty("pollInterval")
    public String pollInterval;

    @JsonProperty("nextExecutionAt")
    public String nextExecutionAt;

    @JsonProperty("indexReplicatedAt")
    public String indexReplicatedAt;

    @JsonProperty("indexReplicatedAtList")
    public List<String> indexReplicatedAtList;

    @JsonProperty("replicationFailedAtList")
    public List<String> replicationFailedAtList;

    @JsonProperty("timesIndexReplicated")
    public Integer timesIndexReplicated;

    @JsonProperty("confFilesReplicated")
    public String confFilesReplicated;

    @JsonProperty("timesConfigReplicated")
    public Integer timesConfigReplicated;

    @JsonProperty("confFilesReplicatedAt")
    public String confFilesReplicatedAt;

    @JsonProperty("lastCycleBytesDownloaded")
    public Long lastCycleBytesDownloaded;

    @JsonProperty("timesFailed")
    public Integer timesFailed;

    @JsonProperty("replicationFailedAt")
    public String replicationFailedAt;

    @JsonProperty("previousCycleTimeInSeconds")
    public Long previousCycleTimeInSeconds;

    @JsonProperty("currentDate")
    public String currentDate;

    @JsonProperty("isPollingDisabled")
    public Boolean isPollingDisabled;

    @JsonProperty("isReplicating")
    public Boolean isReplicating;

    @JsonProperty("filesToDownload")
    public List<String> filesToDownload;

    @JsonProperty("numFilesToDownload")
    public String numFilesToDownload;

    @JsonProperty("bytesToDownload")
    public String bytesToDownload;

    @JsonProperty("filesDownloaded")
    public List<String> filesDownloaded;

    @JsonProperty("numFilesDownloaded")
    public String numFilesDownloaded;

    @JsonProperty("replicationStartTime")
    public String replicationStartTime;

    @JsonProperty("timeElapsed")
    public String timeElapsed;

    @JsonProperty("currentFile")
    public String currentFile;

    @JsonProperty("currentFileSize")
    public String currentFileSize;

    @JsonProperty("currentFileSizeDownloaded")
    public String currentFileSizeDownloaded;

    @JsonProperty("currentFileSizePercent")
    public String currentFileSizePercent;

    @JsonProperty("bytesDownloaded")
    public String bytesDownloaded;

    @JsonProperty("totalPercent")
    public String totalPercent;

    @JsonProperty("timeRemaining")
    public String timeRemaining;

    @JsonProperty("downloadSpeed")
    public String downloadSpeed;
  }
}
