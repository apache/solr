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

import java.util.Locale;
import java.util.Set;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.zero.client.ZeroFile;
import org.apache.solr.zero.client.ZeroStoreClient;

public class FilesDeletionTask extends DeleterTask {

  protected final String collectionName;

  public FilesDeletionTask(
      ZeroStoreClient zeroStoreClient,
      String collectionName,
      Set<ZeroFile> zeroFiles,
      boolean allowRetry,
      int maxRetryAttempt) {
    super(zeroStoreClient, zeroFiles, allowRetry, maxRetryAttempt);
    this.collectionName = collectionName;
  }

  @Override
  public String getActionName() {
    return "DELETE_FILES";
  }

  @Override
  public void setMDCContext() {
    MDCLoggingContext.setCollection(collectionName);
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT, "FilesDeletionTask %s collection=%s", super.toString(), collectionName);
  }

  @Override
  public String getBasePath() {
    return zeroStoreClient.getCollectionURI(collectionName).toString();
  }
}
