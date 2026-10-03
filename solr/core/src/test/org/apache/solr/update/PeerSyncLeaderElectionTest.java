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
package org.apache.solr.update;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCoreInitializationException;
import org.junit.Test;

public class PeerSyncLeaderElectionTest extends SolrTestCase {

  @Test
  public void testFailedCoreIsIgnoredDuringLeaderElectionVersionRequest() {
    SolrCoreInitializationException failedCore =
        new SolrCoreInitializationException("failed_core", new Exception("invalid config"));

    assertEquals(SolrException.ErrorCode.SERVICE_UNAVAILABLE.code, failedCore.code());
    assertTrue(
        PeerSync.isToleratedLeaderElectionException(
            failedCore, true, PeerSync.SHARD_REQUEST_PURPOSE_GET_VERSIONS));
    assertFalse(
        PeerSync.isToleratedLeaderElectionException(
            new SolrException(SolrException.ErrorCode.SERVER_ERROR, "generic failure"),
            true,
            PeerSync.SHARD_REQUEST_PURPOSE_GET_VERSIONS));
    assertFalse(
        PeerSync.isToleratedLeaderElectionException(
            failedCore, false, PeerSync.SHARD_REQUEST_PURPOSE_GET_VERSIONS));
    assertFalse(
        PeerSync.isToleratedLeaderElectionException(
            failedCore, true, PeerSync.SHARD_REQUEST_PURPOSE_GET_UPDATES));
  }
}
