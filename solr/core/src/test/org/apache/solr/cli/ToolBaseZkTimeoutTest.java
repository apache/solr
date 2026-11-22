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
package org.apache.solr.cli;

import java.util.concurrent.TimeoutException;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.junit.Test;

public class ToolBaseZkTimeoutTest extends SolrTestCase {
  @Test
  public void testConnectTimeoutMessageAndOtherFailures() {
    var timeout =
        new TimeoutException("Timeout while waiting for Zookeeper Client to connect: 15000 ms");
    var exception = new SolrException(SolrException.ErrorCode.SERVER_ERROR, timeout);
    assertTrue(ToolBase.isZkConnectTimeout(exception));
    String message = ToolBase.errorMessage(exception);
    assertTrue(message.contains("ZooKeeper did not respond"));
    assertTrue(message.contains("15000 ms"));
    assertTrue(message.contains("Solr node hosting it"));

    var otherTimeout =
        new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, new TimeoutException("other timeout"));
    assertFalse(ToolBase.isZkConnectTimeout(otherTimeout));
    assertEquals(otherTimeout.getMessage(), ToolBase.errorMessage(otherTimeout));
    var otherFailure = new IllegalStateException("other failure");
    assertFalse(ToolBase.isZkConnectTimeout(otherFailure));
    assertEquals("other failure", ToolBase.errorMessage(otherFailure));
  }
}
