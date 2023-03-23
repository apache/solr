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

package org.apache.solr.cloud.api.collections;

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Locale;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.zookeeper.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Overseer processing for the "install shard data" API.
 *
 * <p>Largely this overseer processing consists of ensuring that read-only mode is enabled for the
 * specified collection, identifying the core hosting the shard leader, and sending it a core- admin
 * 'install' request.
 */
public class InstallShardDataCmd implements CollApiCmds.CollectionApiCommand {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CollectionCommandContext ccc;

  public InstallShardDataCmd(CollectionCommandContext ccc) {
    this.ccc = ccc;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList<Object> results)
      throws Exception {
    final RemoteMessage typedMessage =
        new ObjectMapper().convertValue(message.getProperties(), RemoteMessage.class);
    final CollectionHandlingUtils.ShardRequestTracker shardRequestTracker =
        CollectionHandlingUtils.asyncRequestTracker(typedMessage.asyncId, ccc);
    final ClusterState clusterState = ccc.getZkStateReader().getClusterState();
    typedMessage.validate();

    // Fetch the specified Slice
    final DocCollection installCollection = clusterState.getCollection(typedMessage.collection);
    final Slice installSlice = installCollection.getSlice(typedMessage.shard);
    if (installSlice == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "The specified shard [" + typedMessage.shard + "] does not exist.");
    }

    // Build the core-admin request
    final ModifiableSolrParams coreApiParams = new ModifiableSolrParams();
    coreApiParams.set(
        CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.INSTALLCOREDATA.toString());
    typedMessage.toMap(new HashMap<>()).forEach((k, v) -> coreApiParams.set(k, v.toString()));

    // Send the core-admin request to each replica in the slice
    final ShardHandler shardHandler = ccc.newShardHandler();
    shardRequestTracker.sliceCmd(clusterState, coreApiParams, null, installSlice, shardHandler);
    final String errorMessage =
        String.format(
            Locale.ROOT,
            "Could not install data to collection [%s] and shard [%s]",
            typedMessage.collection,
            typedMessage.shard);
    shardRequestTracker.processResponses(new NamedList<>(), shardHandler, true, errorMessage);
  }

  /** A value-type representing the message received by {@link InstallShardDataCmd} */
  public static class RemoteMessage implements JacksonReflectMapWriter {

    @JsonProperty(QUEUE_OPERATION)
    public String operation = CollectionParams.CollectionAction.INSTALLSHARDDATA.toLower();

    @JsonProperty public String collection;

    @JsonProperty public String shard;

    @JsonProperty public String repository;

    @JsonProperty public String location;

    @JsonProperty(ASYNC)
    public String asyncId;

    public void validate() {
      if (StringUtils.isBlank(collection)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "The 'Install Shard Data' API requires a valid collection name to be provided");
      }
      if (StringUtils.isBlank(shard)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "The 'Install Shard Data' API requires a valid shard name to be provided");
      }
    }
  }
}
