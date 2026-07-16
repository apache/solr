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
package org.apache.solr.client.solrj;

import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

/** Unit tests for {@link RemoteSolrException}. */
public class RemoteSolrExceptionTest extends SolrTestCase {

  /**
   * A null remoteError must not throw: it happens whenever the server's error body can't be parsed
   * (e.g. ConcurrentUpdateSolrClient passes a null error), and the constructor is expected to
   * tolerate it. The consumers of the resulting exception must also tolerate the null.
   */
  @Test
  public void testNullRemoteErrorDoesNotThrow() {
    RemoteSolrException e = new RemoteSolrException("http://host:8983/solr", 500, null);
    assertEquals(500, e.code());
    assertNull(e.getRemoteErrorObject());

    // consumers of the exception must tolerate the null remoteError entry
    assertNotNull(e.getMessage());
    List<Map<String, Object>> details = e.getDetails();
    assertEquals(1, details.size());
    assertEquals("http://host:8983/solr", details.get(0).get("remoteHost"));
    assertNull(details.get(0).get("remoteError"));
  }

  /** A non-null remoteError is still recorded and readable back. */
  @Test
  public void testRemoteErrorIsRecorded() {
    Map<String, Object> remoteError = Map.of("msg", "boom", "code", 500);
    RemoteSolrException e = new RemoteSolrException("http://host:8983/solr", 500, remoteError);
    assertEquals(500, e.code());
    assertEquals(remoteError, e.getRemoteErrorObject());
  }
}
