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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.SocketException;
import java.util.List;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrCoreInitializationException;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.servlet.ResponseUtils;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link PeerSync#handleResponse}. These live outside {@link PeerSyncTest} because
 * that suite calls initCore from its constructor, so every additional test method in it leaks a
 * harness core. The core here only exists so a PeerSync can be constructed; handleResponse itself
 * reads nothing from it beyond the log prefix.
 */
public class PeerSyncHandleResponseTest extends SolrTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @ClassRule
  public static final EmbeddedSolrServerTestRule solrTestRule = new EmbeddedSolrServerTestRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    solrTestRule.startSolr(SolrTestCaseJ4.TEST_HOME());
    SolrTestCaseJ4.newRandomConfig();
    solrTestRule.newCollection().withConfigSet(SolrTestCaseJ4.TEST_COLL1_CONF()).create();
  }

  @Test
  public void testPeerSyncCountsCoreInitFailureFromVersionRequestAsSuccess() throws Exception {
    try (SolrCore core = solrTestRule.getCoreContainer().getCore("collection1");
        PeerSync peerSync = new PeerSync(core, List.of("http://example.com/solr/core"), 10, true)) {
      ShardResponse response =
          failedResponse(
              PeerSync.SHARD_REQUEST_PURPOSE_GET_VERSIONS,
              serverError(
                  new SolrCoreInitializationException("collection1", new IOException("boom"))));

      assertTrue(peerSync.handleResponse(response));
    }
  }

  @Test
  public void testPeerSyncStillFailsOtherServerErrorFromVersionRequest() throws Exception {
    try (SolrCore core = solrTestRule.getCoreContainer().getCore("collection1");
        PeerSync peerSync = new PeerSync(core, List.of("http://example.com/solr/core"), 10, true)) {
      ShardResponse response =
          failedResponse(
              PeerSync.SHARD_REQUEST_PURPOSE_GET_VERSIONS,
              serverError(new SolrException(SolrException.ErrorCode.SERVER_ERROR, "boom")));

      assertFalse(peerSync.handleResponse(response));
    }
  }

  @Test
  public void testPeerSyncStillFailsCoreInitFailureFromUpdateRequest() throws Exception {
    try (SolrCore core = solrTestRule.getCoreContainer().getCore("collection1");
        PeerSync peerSync = new PeerSync(core, List.of("http://example.com/solr/core"), 10, true)) {
      ShardResponse response =
          failedResponse(
              PeerSync.SHARD_REQUEST_PURPOSE_GET_UPDATES,
              serverError(
                  new SolrCoreInitializationException("collection1", new IOException("boom"))));

      assertFalse(peerSync.handleResponse(response));
    }
  }

  @Test
  public void testPeerSyncStillFailsCoreInitFailureWhenCantReachIsNotSuccess() throws Exception {
    assertFalse(
        handleVersionsFailure(
            false,
            serverError(
                new SolrCoreInitializationException("collection1", new IOException("boom")))));
  }

  @Test
  public void testPeerSyncCountsServiceUnavailableFromVersionRequestAsSuccess() throws Exception {
    assertTrue(
        handleVersionsFailure(
            true, new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "boom")));
  }

  @Test
  public void testPeerSyncStillFailsServiceUnavailableWhenCantReachIsNotSuccess() throws Exception {
    assertFalse(
        handleVersionsFailure(
            false, new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "boom")));
  }

  @Test
  public void testPeerSyncCountsRefusedConnectionFromVersionRequestAsSuccess() throws Exception {
    assertTrue(
        handleVersionsFailure(
            true, new SolrServerException(new SocketException("Connection refused"))));
  }

  @Test
  public void testPeerSyncStillFailsRefusedConnectionWhenCantReachIsNotSuccess() throws Exception {
    assertFalse(
        handleVersionsFailure(
            false, new SolrServerException(new SocketException("Connection refused"))));
  }

  private boolean handleVersionsFailure(boolean cantReachIsSuccess, Throwable exception)
      throws Exception {
    try (SolrCore core = solrTestRule.getCoreContainer().getCore("collection1");
        PeerSync peerSync =
            new PeerSync(core, List.of("http://example.com/solr/core"), 10, cantReachIsSuccess)) {
      return peerSync.handleResponse(
          failedResponse(PeerSync.SHARD_REQUEST_PURPOSE_GET_VERSIONS, exception));
    }
  }

  private static ShardResponse failedResponse(int purpose, Throwable exception) {
    ShardRequest request = new ShardRequest();
    request.purpose = purpose;

    // ShardResponse is final, so it cannot be mocked; build a real one.
    ShardResponse response = new ShardResponse();
    response.setShardRequest(request);
    response.setException(exception);
    response.setShardAddress("http://example.com/solr/core");
    return response;
  }

  // The server writes the exception class into the response metadata, so build the error the
  // same way the response writer does.
  @SuppressWarnings("unchecked")
  private static SolrException serverError(Throwable cause) {
    NamedList<Object> info = new SimpleOrderedMap<>();
    ResponseUtils.getErrorInfo(cause, info, log);
    SolrException exception = new SolrException(SolrException.ErrorCode.SERVER_ERROR, "boom");
    exception.setMetadata((NamedList<String>) info.get("metadata"));
    return exception;
  }
}
