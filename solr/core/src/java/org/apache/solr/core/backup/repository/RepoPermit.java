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

package org.apache.solr.core.backup.repository;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility {@link java.util.concurrent.Semaphore} in order to limit certain heavy I/O for
 * backup/restore.
 */
class RepoPermit implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Semaphore semaphore =
      new Semaphore(
          Integer.getInteger("solr.backupRepo.permits", 10), true // fair
          );

  RepoPermit(String operation) throws InterruptedException {
    if (semaphore.tryAcquire()) { // optimistic
      return;
    }
    log.info("Waiting to acquire permit for {}", operation);
    semaphore.acquire();
  }

  @Override
  public void close() {
    semaphore.release();
  }
}
