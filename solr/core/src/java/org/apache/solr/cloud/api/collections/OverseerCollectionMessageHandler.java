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

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CommonParams.NAME;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.LockTree;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.OverseerMessageHandler;
import org.apache.solr.cloud.OverseerNodePrioritizer;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.Stats;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.logging.MDCLoggingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link OverseerMessageHandler} that handles Collections API related overseer messages.
 *
 * <p>A lot of the content that was in this class got moved to {@link CollectionHandlingUtils} and
 * {@link CollApiCmds}.
 *
 * <p>The equivalent of this class for distributed Collection API command execution is {@link
 * DistributedCollectionConfigSetCommandRunner}.
 */
public class OverseerCollectionMessageHandler implements OverseerMessageHandler, SolrCloseable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  Overseer overseer;
  HttpShardHandlerFactory shardHandlerFactory;
  String adminPath;
  ZkStateReader zkStateReader;
  SolrCloudManager cloudManager;
  String myId;
  Stats stats;
  TimeSource timeSource;

  // Set that tracks collections that are currently being processed by a running task.
  // This is used for handling mutual exclusion of the tasks.

  private final LockTree lockTree = new LockTree();
  ExecutorService tpe =
      new ExecutorUtil.MDCAwareThreadPoolExecutor(
          5,
          10,
          0L,
          TimeUnit.MILLISECONDS,
          new SynchronousQueue<>(),
          new SolrNamedThreadFactory("OverseerCollectionMessageHandlerThreadFactory"));

  private final CollApiCmds.CommandMap commandMapper;

  private volatile boolean isClosed;

  public OverseerCollectionMessageHandler(
      ZkStateReader zkStateReader,
      String myId,
      final HttpShardHandlerFactory shardHandlerFactory,
      String adminPath,
      Stats stats,
      Overseer overseer,
      OverseerNodePrioritizer overseerPrioritizer) {
    this.zkStateReader = zkStateReader;
    this.shardHandlerFactory = shardHandlerFactory;
    this.adminPath = adminPath;
    this.myId = myId;
    this.stats = stats;
    this.overseer = overseer;
    this.cloudManager = overseer.getSolrCloudManager();
    this.timeSource = cloudManager.getTimeSource();
    this.isClosed = false;
    commandMapper =
        new CollApiCmds.CommandMap(new OcmhCollectionCommandContext(this), overseerPrioritizer);
  }

  @Override
  public OverseerSolrResponse processMessage(ZkNodeProps message, String operation) {
    // sometimes overseer messages have the collection name in 'name' field, not 'collection'
    MDCLoggingContext.setCollection(
        message.getStr(COLLECTION_PROP) != null
            ? message.getStr(COLLECTION_PROP)
            : message.getStr(NAME));
    MDCLoggingContext.setShard(message.getStr(SHARD_ID_PROP));
    MDCLoggingContext.setReplica(message.getStr(REPLICA_PROP));
    log.debug("OverseerCollectionMessageHandler.processMessage : {} , {}", operation, message);

    NamedList<Object> results = new NamedList<>();
    try {
      CollectionAction action = getCollectionAction(operation);
      CollApiCmds.CollectionApiCommand command = commandMapper.getActionCommand(action);
      if (command != null) {
        command.call(cloudManager.getClusterState(), message, results);
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:" + operation);
      }
    } catch (Exception e) {
      String collName = message.getStr("collection");
      if (collName == null) collName = message.getStr(NAME);

      if (collName == null) {
        log.error("Operation {} failed", operation, e);
      } else {
        log.error("Collection {}}, operation {} failed", collName, operation, e);
      }

      results.add("Operation " + operation + " caused exception:", e);
      SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>();
      nl.add("msg", e.getMessage());
      nl.add("rspCode", e instanceof SolrException ? ((SolrException) e).code() : -1);
      results.add("exception", nl);
    }
    return new OverseerSolrResponse(results);
  }

  private CollectionAction getCollectionAction(String operation) {
    CollectionAction action = CollectionAction.get(operation);
    if (action == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:" + operation);
    }
    return action;
  }

  @Override
  public String getName() {
    return "Overseer Collection Message Handler";
  }

  @Override
  public String getTimerName(String operation) {
    return "collection_" + operation;
  }

  @Override
  public String getTaskKey(ZkNodeProps message) {
    return message.containsKey(COLLECTION_PROP)
        ? message.getStr(COLLECTION_PROP)
        : message.getStr(NAME);
  }

  // -1 is not a possible batchSessionId so -1 will force initialization of lockSession
  private long sessionId = -1;
  private LockTree.Session lockSession;

  /**
   * Grabs an exclusive lock for this particular task.
   *
   * @return <code>null</code> if locking is not possible. When locking is not possible, it will
   *     remain impossible for the passed value of <code>batchSessionId</code>. This is to guarantee
   *     tasks are executed in queue order (and a later task is not run earlier than its turn just
   *     because it happens that a lock got released).
   */
  @Override
  public Lock lockTask(ZkNodeProps message, long batchSessionId) {
    if (sessionId != batchSessionId) {
      // this is always called in the same thread.
      // Each batch is supposed to have a new taskBatch
      // So if taskBatch changes we must create a new Session
      lockSession = lockTree.getSession();
      sessionId = batchSessionId;
    }

    return lockSession.lock(
        getCollectionAction(message.getStr(Overseer.QUEUE_OPERATION)),
        Arrays.asList(
            getTaskKey(message),
            message.getStr(ZkStateReader.SHARD_ID_PROP),
            message.getStr(ZkStateReader.REPLICA_PROP)));
  }

  @Override
  public void close() throws IOException {
    this.isClosed = true;
    if (tpe != null) {
      if (!ExecutorUtil.isShutdown(tpe)) {
        ExecutorUtil.shutdownAndAwaitTermination(tpe);
      }
    }
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }
}
