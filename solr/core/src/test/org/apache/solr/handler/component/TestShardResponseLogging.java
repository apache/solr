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
package org.apache.solr.handler.component;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.LogListener;
import org.apache.solr.util.SolrResponseUtil;
import org.junit.Test;

/** What {@link SolrResponseUtil} logs when a shard response is missing an expected section. */
public class TestShardResponseLogging extends SolrTestCase {

  @Test
  public void testCorruptedResponseWarningDoesNotLogRequestOrResponseData() {
    final String secretTerm = "customer_secret_term";
    final String nodeName = "node1:8983_solr";

    final ShardRequest shardRequest = new ShardRequest();
    shardRequest.params = new ModifiableSolrParams();
    shardRequest.params.set("q", "name:" + secretTerm);
    shardRequest.purpose = ShardRequest.PURPOSE_GET_TOP_IDS;

    final NamedList<Object> header = new NamedList<>();
    header.add("status", 0);
    // The handler defaults echo the request params into the header (SolrCore adds them from
    // the original request params when echoParams resolves to explicit or all), so the
    // header a real shard response carries also holds the query text.
    header.add("params", new SimpleOrderedMap<>(shardRequest.params));
    final NamedList<Object> body = new NamedList<>();
    body.add(SolrQueryResponse.RESPONSE_HEADER_KEY, header);
    body.add("name:" + secretTerm, "private response value");
    final SolrResponseBase solrResponse = new SolrResponseBase();
    solrResponse.setResponse(body);

    final ShardResponse shardResponse = new ShardResponse();
    shardResponse.setShardRequest(shardRequest);
    shardResponse.setSolrResponse(solrResponse);
    shardResponse.setShard("shard1");
    shardResponse.setNodeName(nodeName);

    try (LogListener warnLog =
        LogListener.warn(SolrResponseUtil.class).substring("corrupted response")) {
      assertThrows(
          SolrException.class,
          () ->
              SolrResponseUtil.getSubsectionFromShardResponse(
                  null, shardResponse, "response", false));

      final String message = warnLog.pollMessage();
      assertNotNull("expected a corrupted response warning", message);
      assertTrue(message, message.contains(nodeName));
      assertFalse(
          "sensitive query/key value reached warning: " + message, message.contains(secretTerm));
      assertFalse(
          "response value reached warning: " + message, message.contains("private response value"));
      assertTrue(message, message.contains("missing response section response"));
      assertTrue(message, message.contains("response has 2 top-level entries"));
      assertTrue(
          message, message.contains("shard request purpose " + ShardRequest.PURPOSE_GET_TOP_IDS));
    }
  }

  @Test
  public void testCorruptedResponseWithNullNodeNameUsesSanitizedShardAddress() {
    final String shardAddress =
        "http://user:password@localhost:8983/solr?token=customer_secret_term";
    final String safeShardName = "localhost:8983";

    final ShardRequest shardRequest = new ShardRequest();
    shardRequest.params = new ModifiableSolrParams();
    shardRequest.params.set("q", "name:customer_secret_term");

    final NamedList<Object> header = new NamedList<>();
    header.add("status", 0);
    final NamedList<Object> body = new NamedList<>();
    body.add(SolrQueryResponse.RESPONSE_HEADER_KEY, header);
    final SolrResponseBase solrResponse = new SolrResponseBase();
    solrResponse.setResponse(body);

    final ShardResponse shardResponse = new ShardResponse();
    shardResponse.setShardRequest(shardRequest);
    shardResponse.setSolrResponse(solrResponse);
    shardResponse.setShard(shardAddress);
    shardResponse.setShardAddress(shardAddress);
    // The node name is left null, as it is once a node has stopped; both the warning and
    // the exception should fall back to the shard address instead of printing "null".

    try (LogListener warnLog =
        LogListener.warn(SolrResponseUtil.class).substring("corrupted response")) {
      final SolrException ex =
          assertThrows(
              SolrException.class,
              () ->
                  SolrResponseUtil.getSubsectionFromShardResponse(
                      null, shardResponse, "response", false));

      final String message = warnLog.pollMessage();
      assertNotNull("expected a corrupted response warning", message);
      assertTrue(message, message.contains(safeShardName));
      assertFalse(
          "sensitive shard URL reached warning: " + message, message.contains(shardAddress));
      assertFalse(
          "shard credentials reached warning: " + message, message.contains("user:password"));
      assertFalse(
          "shard query reached warning: " + message, message.contains("customer_secret_term"));
      assertTrue(ex.getMessage(), ex.getMessage().contains("shard: " + safeShardName));
      assertFalse(
          "sensitive shard URL reached exception: " + ex.getMessage(),
          ex.getMessage().contains("user:password"));
      assertFalse(
          "sensitive shard query reached exception: " + ex.getMessage(),
          ex.getMessage().contains("customer_secret_term"));

      // The method wraps the "absent" exception in its catch block, so the message that
      // names the node rides as the cause.
      assertNotNull("expected a cause on the thrown exception", ex.getCause());
      final String causeMessage = ex.getCause().getMessage();
      assertTrue(
          causeMessage, causeMessage.contains("is absent in response from " + safeShardName));
      assertFalse(causeMessage, causeMessage.contains("from null"));
      assertFalse(
          "shard credentials reached exception cause: " + causeMessage,
          causeMessage.contains("user:password"));
      assertFalse(
          "shard query reached exception cause: " + causeMessage,
          causeMessage.contains("customer_secret_term"));
    }
  }
}
