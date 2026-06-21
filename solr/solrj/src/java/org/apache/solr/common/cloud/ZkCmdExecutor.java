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
package org.apache.solr.common.cloud;

import org.apache.solr.cloud.ActionThrottle;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ConnectionManager.IsClosed;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

public class ZkCmdExecutor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final SolrZkClient solrZkClient;

  private int retryCount;
  private IsClosed isClosed;

  public ZkCmdExecutor(SolrZkClient solrZkClient, int retryCount) {
    this(solrZkClient, retryCount, null);
  }

  /**
   * TODO: At this point, this should probably take a SolrZkClient in
   * its constructor.
   *
   * @param retryCount number of retries on connectionloss
   */
  public ZkCmdExecutor(SolrZkClient solrZkClient, int retryCount, IsClosed isClosed) {
    this.retryCount = retryCount;
    this.isClosed = isClosed;
    this.solrZkClient = solrZkClient;
  }

  public static <T> T retryOperation(ZkCmdExecutor zkCmdExecutor, ZkOperation operation) throws KeeperException, InterruptedException {
    return retryOperation(zkCmdExecutor, operation, true);
  }

  /**
   * Perform the given operation, retrying if the connection fails
   */
  @SuppressWarnings("unchecked")
  public static <T> T retryOperation(ZkCmdExecutor zkCmdExecutor, ZkOperation operation, boolean retryOnSessionExp)
      throws KeeperException, InterruptedException {
    KeeperException exception = null;
    int tryCnt = 0;
    ActionThrottle retryThrottle = null;
    while (tryCnt < zkCmdExecutor.retryCount) {
      try {
        return (T) operation.execute();
      } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException e) {
        if (!retryOnSessionExp && e instanceof KeeperException.SessionExpiredException) {
          throw e;
        }
        log.info("retryOperation {}", e.getMessage());
        if (exception == null) {
          exception = e;
        }
        if (retryThrottle == null) {
          retryThrottle = new ActionThrottle("leader", Integer.getInteger("solr.zkCmdExecutor", 0));
        }

        retryThrottle.minimumWaitBetweenActions();
        retryThrottle.markAttemptingAction();


        zkCmdExecutor.retryDelay(tryCnt);

      }
      tryCnt++;
    }
    if (exception == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unexpected fail, we should have tracked the exception");
    }
    // Retries exhausted: wrap the original KeeperException in SolrException so callers
    // receive a consistent SolrException rather than a raw ZooKeeper exception.
    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
        "Could not complete ZooKeeper operation after " + zkCmdExecutor.retryCount + " retries", exception);
  }
  
  /**
   * Performs a retry delay if this is not the first attempt
   *
   * @param attemptCount
   *          the number of the attempts performed so far
   */
  protected void retryDelay(int attemptCount) throws InterruptedException {
//    if (isClosed != null && isClosed.isClosed()) {
//     throw new AlreadyClosedException();
//    }
    log.info("retry, attempt={}", attemptCount);

    // Use the client's session timeout as the wait bound so that a client with a short
    // session timeout (e.g. probing an invalid address) fails fast rather than waiting
    // the full 30 s default.  For a normal client the session timeout is >= 15 s which
    // is still a reasonable upper bound for a reconnect wait.
    int waitMs = solrZkClient.getZkClientTimeout();
    solrZkClient.getConnectionManager().waitForConnected(waitMs > 0 ? waitMs : Integer.getInteger("waitForZk", 30) * 1000);

  }

}
