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
import org.apache.solr.zero.client.ZeroStoreClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectionDeletionTask extends FilesDeletionTask {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public CollectionDeletionTask(
      ZeroStoreClient zeroStoreClient,
      String collectionName,
      boolean allowRetry,
      int maxRetryAttempt) {
    super(
        zeroStoreClient,
        collectionName,
        zeroStoreClient.listCollectionZeroFiles(collectionName),
        allowRetry,
        maxRetryAttempt);
  }

  @Override
  public String getActionName() {
    return "DELETE_COLLECTION_FILES";
  }

  @Override
  public DeleterTask.Result call() {
    DeleterTask.Result result = super.call();
    if (result.isSuccess()) {
      try {
        zeroStoreClient.deleteCollectionDirectory(collectionName);
      } catch (NoSuchFileException ex) {
        if (log.isWarnEnabled())
          log.warn(
              "Could not delete Zero store directory for collection={} as it does not exists",
              collectionName);
      } catch (IOException ex) {
        result.updateSuccess(false);
        if (log.isWarnEnabled())
          log.warn(
              "Could not delete Zero store directory for collection={} after all files have been deleted",
              collectionName);
      }
    }
    return result;
  }
}
