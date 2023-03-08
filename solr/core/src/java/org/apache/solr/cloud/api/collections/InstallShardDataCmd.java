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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_LOCATION;
import static org.apache.solr.common.params.CoreAdminParams.CORE;

public class InstallShardDataCmd implements CollApiCmds.CollectionApiCommand {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final CollectionCommandContext ccc;

    public InstallShardDataCmd(CollectionCommandContext ccc) {
        this.ccc = ccc;
    }

    @Override
    public void call(ClusterState state, ZkNodeProps message, NamedList<Object> results) throws Exception {
        // TODO Check that collection and shard name exist
        // TODO Check that the backup repository (if specified) exists

        log.info("JEGERLOW: In the overseer command with location value {}", message.get(BACKUP_LOCATION));
        final RemoteMessage typedMessage = new ObjectMapper().convertValue(message.getProperties(), RemoteMessage.class);
        log.info("JEGERLOW: In the overseer command with location value {}", typedMessage.location);

        ClusterState clusterState = ccc.getZkStateReader().getClusterState();
        DocCollection installCollection = clusterState.getCollection(typedMessage.collection);

        enableReadOnly(clusterState, installCollection);
        try {
            final CollectionHandlingUtils.ShardRequestTracker shardRequestTracker = CollectionHandlingUtils.asyncRequestTracker(typedMessage.asyncId, ccc);

            // Build the core-admin request
            final Slice installSlice = installCollection.getSlice(typedMessage.shard);
            final Replica leaderReplica = installSlice.getLeader();
            final ModifiableSolrParams coreApiParams = new ModifiableSolrParams();
            coreApiParams.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.INSTALLCOREDATA.toString());
            coreApiParams.set(CORE, leaderReplica.core);
            typedMessage.toMap(new HashMap<>()).forEach((k, v) -> coreApiParams.set(k, v.toString()));

            // Send it to the node hosting the leader
            final ShardHandler shardHandler = ccc.newShardHandler();
            shardRequestTracker.sendShardRequest(leaderReplica.getNodeName(), coreApiParams, shardHandler);
            shardRequestTracker.processResponses(
                    new NamedList<>(), shardHandler, true, "Could not install data to shard");
        } finally {
            disableReadOnly(clusterState, installCollection);
        }
        log.info("JEGERLOW Hey look, we finished the overseer command");
    }

    // TODO Reuse the copies of these in RestoreCmd
    private void disableReadOnly(ClusterState clusterState, DocCollection restoreCollection)
            throws Exception {
        ZkNodeProps params =
                new ZkNodeProps(
                        QUEUE_OPERATION,
                        CollectionParams.CollectionAction.MODIFYCOLLECTION.toString(),
                        ZkStateReader.COLLECTION_PROP, restoreCollection.getName(),
                        ZkStateReader.READ_ONLY, null);
        new CollApiCmds.ModifyCollectionCmd(ccc).call(clusterState, params, new NamedList<>());
    }

    private void enableReadOnly(ClusterState clusterState, DocCollection restoreCollection)
            throws Exception {
        ZkNodeProps params =
                new ZkNodeProps(
                        QUEUE_OPERATION,
                        CollectionParams.CollectionAction.MODIFYCOLLECTION.toString(),
                        ZkStateReader.COLLECTION_PROP, restoreCollection.getName(),
                        ZkStateReader.READ_ONLY, "true");
        new CollApiCmds.ModifyCollectionCmd(ccc).call(clusterState, params, new NamedList<>());
    }

    public static class RemoteMessage implements JacksonReflectMapWriter {

        @JsonProperty(QUEUE_OPERATION)
        public String operation = CollectionParams.CollectionAction.INSTALLSHARDDATA.toLower();

        @JsonProperty
        public String collection;

        @JsonProperty
        public String shard;

        @JsonProperty
        public String repository;

        @JsonProperty
        public String location;

        @JsonProperty(ASYNC)
        public String asyncId;
    }
}
