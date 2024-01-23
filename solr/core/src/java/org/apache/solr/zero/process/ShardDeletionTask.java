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

package org.apache.solr.zero.process;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.NoSuchFileException;
import java.util.Locale;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.zero.client.ZeroStoreClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A file deletion task that deletes all files from Zero store under given path */
public class ShardDeletionTask extends FilesDeletionTask {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final String shardName;

  public ShardDeletionTask(
      ZeroStoreClient zeroStoreClient,
      String collectionName,
      String shardName,
      boolean allowRetry,
      int maxRetryAttempt) {
    super(
        zeroStoreClient,
        collectionName,
        zeroStoreClient.listShardZeroFiles(collectionName, shardName),
        allowRetry,
        maxRetryAttempt);
    this.shardName = shardName;
  }

  @Override
  public String getActionName() {
    return "DELETE_SHARD_FILES";
  }

  @Override
  public void setMDCContext() {
    super.setMDCContext();
    MDCLoggingContext.setShard(shardName);
  }

  @Override
  public String toString() {
    return String.format(Locale.ROOT, "ShardDeletionTask %s shard=%s", super.toString(), shardName);
  }

  @Override
  public String getBasePath() {
    return zeroStoreClient.getShardURI(collectionName, shardName).toString();
  }

  @Override
  public DeleterTask.Result call() {
    DeleterTask.Result result = super.call();
    if (result.isSuccess()) {
      try {
        zeroStoreClient.deleteShardDirectory(collectionName, shardName);
      } catch (NoSuchFileException ex) {
        if (log.isWarnEnabled())
          log.warn(
              "Could not delete Zero store directory for collection={} shard={} as it does not exist",
              collectionName,
              shardName);
      } catch (IOException ex) {
        result.updateSuccess(false);
        if (log.isWarnEnabled())
          log.warn(
              "Could not delete Zero store directory for collection={} shard={} after all files have been deleted",
              collectionName,
              shardName);
      }
    }
    return result;
  }
}
