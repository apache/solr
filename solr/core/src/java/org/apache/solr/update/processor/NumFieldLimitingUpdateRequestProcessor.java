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
package org.apache.solr.update.processor;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Locale;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.AddUpdateCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NumFieldLimitingUpdateRequestProcessor extends UpdateRequestProcessor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private SolrQueryRequest req;
  private int fieldThreshold;
  private int currentNumFields;
  private boolean warnOnly;

  public NumFieldLimitingUpdateRequestProcessor(
      SolrQueryRequest req,
      UpdateRequestProcessor next,
      int fieldThreshold,
      int currentNumFields,
      boolean warnOnly) {
    super(next);
    this.req = req;
    this.fieldThreshold = fieldThreshold;
    this.currentNumFields = currentNumFields;
    this.warnOnly = warnOnly;
  }

  public void processAdd(AddUpdateCommand cmd) throws IOException {
    if (!isCloudMode() || /* implicit isCloudMode==true */ isLeaderThatOwnsTheDoc(cmd)) {
      if (coreExceedsFieldLimit()) {
        throwExceptionOrLog(cmd.getPrintableId());
      } else {
        if (log.isDebugEnabled()) {
          log.debug(
              "Allowing document {}, since current core is under the 'maxFields' limit (numFields={}, maxFields={})",
              cmd.getPrintableId(),
              currentNumFields,
              fieldThreshold);
        }
      }
    }
    super.processAdd(cmd);
  }

  protected boolean coreExceedsFieldLimit() {
    return currentNumFields > fieldThreshold;
  }

  protected void throwExceptionOrLog(String id) {
    final String messageSuffix = warnOnly ? "Blocking update of document " + id : "";
    final String message =
        String.format(
            Locale.ROOT,
            "Current core has %d fields, exceeding the max-fields limit of %d.  %s",
            currentNumFields,
            fieldThreshold,
            messageSuffix);
    if (warnOnly) {
      log.warn(message);
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, message);
    }
  }

  private boolean isCloudMode() {
    return req.getCore().getCoreContainer().isZooKeeperAware();
  }

  private boolean isLeaderThatOwnsTheDoc(AddUpdateCommand cmd) {
    CoreDescriptor coreDesc = req.getCore().getCoreDescriptor();
    ZkController zkController = req.getCore().getCoreContainer().getZkController();
    String collection = coreDesc.getCollectionName();

    // Is the document targeting our shard?
    final var targetSlice = getTargetSliceForDocument(zkController, collection, cmd);
    final String ourShardName = coreDesc.getCloudDescriptor().getShardId();
    if (!StrUtils.equalsIgnoreCase(targetSlice.getName(), ourShardName)) {
      return false;
    }

    // The document targets the shard this core represents, but are we the leader?
    return isLeaderForShard(
        zkController, coreDesc.getCloudDescriptor(), collection, targetSlice.getName());
  }

  private Slice getTargetSliceForDocument(
      ZkController zkController, String collectionName, AddUpdateCommand cmd) {
    final var coll = zkController.getClusterState().getCollection(collectionName);
    return coll.getRouter()
        .getTargetSlice(
            cmd.getPrintableId(),
            cmd.getSolrInputDocument(),
            cmd.getRoute(),
            req.getParams(),
            coll);
  }

  private boolean isLeaderForShard(
      ZkController zkController,
      CloudDescriptor cloudDescriptor,
      String collection,
      String targetShardName) {
    try {
      Replica leaderReplica =
          zkController.getZkStateReader().getLeaderRetry(collection, targetShardName);
      return leaderReplica.getName().equals(cloudDescriptor.getCoreNodeName());
    } catch (InterruptedException e) {
      log.error("getLeaderRetry failed", e);
      Thread.currentThread().interrupt();
    }
    return false;
  }
}
