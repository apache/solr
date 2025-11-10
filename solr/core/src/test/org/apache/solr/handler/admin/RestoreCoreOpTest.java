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

package org.apache.solr.handler.admin;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Test;

/** Unit tests for {@link RestoreCoreOp} */
public class RestoreCoreOpTest extends SolrTestCaseJ4 {

  @Test
  public void testConstructsValidV2RequestFromV1Params() {
    final var params = new ModifiableSolrParams();
    // 'name' and 'shardBackupId' are mutually exclusive in real requests, but include them both
    // here
    // to validate parameter mapping.
    params.add("name", "someName");
    params.add("shardBackupId", "someShardBackupId");
    params.add("repository", "someRepo");
    params.add("location", "someLocation");
    params.add("async", "someasyncid");

    final var requestBody = RestoreCoreOp.createRequestFromV1Params(params);

    assertEquals("someName", requestBody.name);
    assertEquals("someRepo", requestBody.backupRepository);
    assertEquals("someLocation", requestBody.location);
    assertEquals("someShardBackupId", requestBody.shardBackupId);
    assertNull("Expected 'async' parameter to be omitted", requestBody.async);
  }
}
