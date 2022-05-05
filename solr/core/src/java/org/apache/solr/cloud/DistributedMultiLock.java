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

package org.apache.solr.cloud;

import com.google.common.annotations.VisibleForTesting;
import java.lang.invoke.MethodHandles;
import java.util.List;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A lock as acquired for running a single API command (Collection or Config Set or anything else in
 * the future). Internally it is composed of multiple {@link DistributedLock}'s.
 */
public class DistributedMultiLock {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final List<DistributedLock> locks;
  private volatile boolean isReleased = false;

  public DistributedMultiLock(List<DistributedLock> locks) {
    this.locks = locks;
  }

  public void waitUntilAcquired() {
    if (isReleased) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Released lock can't be waited upon");
    }

    for (DistributedLock lock : locks) {
      log.debug("DistributedMultiLock.waitUntilAcquired. About to wait on lock {}", lock);
      lock.waitUntilAcquired();
      log.debug("DistributedMultiLock.waitUntilAcquired. Acquired lock {}", lock);
    }
  }

  public void release() {
    isReleased = true;
    for (DistributedLock lock : locks) {
      lock.release();
    }
  }

  public boolean isAcquired() {
    if (isReleased) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Released lock can't be tested");
    }
    for (DistributedLock lock : locks) {
      if (!lock.isAcquired()) {
        return false;
      }
    }
    return true;
  }

  @VisibleForTesting
  public int getCountInternalLocks() {
    return locks.size();
  }
}
