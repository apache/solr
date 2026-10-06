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

import static org.mockito.Mockito.mock;

import java.util.List;
import org.apache.solr.SolrTestCase;
import org.apache.solr.update.SolrCmdDistributor.SolrError;

public class StreamingSolrClientsTest extends SolrTestCase {

  private static SolrError error(int statusCode) {
    SolrError error = new SolrError();
    error.statusCode = statusCode;
    return error;
  }

  public void testGetErrorsIsASnapshot() {
    StreamingSolrClients clients = new StreamingSolrClients(mock(UpdateShardHandler.class));
    clients.errors.add(error(500));

    List<SolrError> snapshot = clients.getErrors();
    assertEquals(1, snapshot.size());

    // an error reported by a runner thread after the snapshot was taken is not part of it
    clients.errors.add(error(503));
    assertEquals(1, snapshot.size());
    assertEquals(2, clients.getErrors().size());

    // clearing the clients does not empty a list handed out earlier
    clients.clearErrors();
    assertEquals(1, snapshot.size());
    assertEquals(0, clients.getErrors().size());
  }

  public void testDrainErrorsReturnsAndClears() {
    StreamingSolrClients clients = new StreamingSolrClients(mock(UpdateShardHandler.class));
    clients.errors.add(error(500));
    clients.errors.add(error(503));

    List<SolrError> drained = clients.drainErrors();
    assertEquals(2, drained.size());
    assertEquals(500, drained.get(0).statusCode);
    assertEquals(503, drained.get(1).statusCode);
    assertTrue(clients.getErrors().isEmpty());

    // an error that arrives after the drain is kept for the next round of retries
    clients.errors.add(error(502));
    assertEquals(1, clients.drainErrors().size());
    assertTrue(clients.drainErrors().isEmpty());
  }
}
