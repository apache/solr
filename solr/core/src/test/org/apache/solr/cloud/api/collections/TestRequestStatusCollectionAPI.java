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
package org.apache.solr.cloud.api.collections;

import static org.hamcrest.Matchers.containsString;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.cloud.BasicDistributedZkTest;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRequestStatusCollectionAPI extends BasicDistributedZkTest {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final int MAX_WAIT_TIMEOUT_SECONDS = 90;

  public TestRequestStatusCollectionAPI() {
    schemaString = "schema15.xml"; // we need a string id
  }

  @Override
  @Test
  public void test() {
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.set(CollectionParams.ACTION, CollectionParams.CollectionAction.CREATE.toString());
    params.set("name", "collection2");
    int numShards = 2;
    params.set("numShards", numShards);
    int replicationFactor = 1;
    params.set("replicationFactor", replicationFactor);
    params.set("collection.configName", "conf1");
    params.set(CommonAdminParams.ASYNC, "1000");
    try {
      sendRequest(params);
    } catch (SolrServerException | IOException e) {
      log.error("error sending request", e);
    }

    // Check for the request to be completed.

    NamedList<?> r = null;
    NamedList<?> status = null;
    String message = null;

    params = new ModifiableSolrParams();

    params.set("action", CollectionParams.CollectionAction.REQUESTSTATUS.toString());
    params.set(CollectionHandlingUtils.REQUESTID, "1000");

    NamedList<Object> createResponse = null;
    try {
      createResponse = sendStatusRequestWithRetry(params, MAX_WAIT_TIMEOUT_SECONDS);
      message = (String) createResponse.findRecursive("status", "msg");
    } catch (SolrServerException | IOException e) {
      log.error("error sending request", e);
    }

    assertEquals("found [1000] in completed tasks", message);
    assertEquals(
        "expecting " + numShards + " shard responses at " + createResponse,
        numShards,
        numResponsesCompleted(createResponse));

    // Check for a random (hopefully non-existent request id
    params = new ModifiableSolrParams();
    params.set(CollectionParams.ACTION, CollectionParams.CollectionAction.REQUESTSTATUS.toString());
    params.set(CollectionHandlingUtils.REQUESTID, "9999999");
    try {
      r = sendRequest(params);
      status = (NamedList<?>) r.get("status");
      message = (String) status.get("msg");
    } catch (SolrServerException | IOException e) {
      log.error("error sending request", e);
    }

    assertEquals("Did not find [9999999] in any tasks queue", message);

    params = new ModifiableSolrParams();
    params.set(CollectionParams.ACTION, CollectionParams.CollectionAction.SPLITSHARD.toString());
    params.set("collection", "collection2");
    params.set("shard", "shard1");
    params.set(CommonAdminParams.ASYNC, "1001");
    try {
      sendRequest(params);
    } catch (SolrServerException | IOException e) {
      log.error("error sending request", e);
    }

    // Check for the request to be completed.
    params = new ModifiableSolrParams();
    params.set("action", CollectionParams.CollectionAction.REQUESTSTATUS.toString());
    params.set(CollectionHandlingUtils.REQUESTID, "1001");
    NamedList<Object> splitResponse = null;
    try {
      splitResponse = sendStatusRequestWithRetry(params, MAX_WAIT_TIMEOUT_SECONDS);
      message = (String) splitResponse.findRecursive("status", "msg");
    } catch (SolrServerException | IOException e) {
      log.error("error sending request", e);
    }

    assertEquals("found [1001] in completed tasks", message);
    // create * 2 + preprecovery * 2 + split + req_apply_upd * 2 = 7
    assertEquals(
        "expecting " + (2 + 2 + 1 + 2) + " shard responses at " + splitResponse,
        (2 + 2 + 1 + 2),
        numResponsesCompleted(splitResponse));

    params = new ModifiableSolrParams();
    params.set(CollectionParams.ACTION, CollectionParams.CollectionAction.CREATE.toString());
    params.set("name", "collection2");
    params.set("numShards", 2);
    params.set("replicationFactor", 1);
    params.set("collection.configName", "conf1");
    params.set(CommonAdminParams.ASYNC, "1002");
    try {
      sendRequest(params);
    } catch (SolrServerException | IOException e) {
      log.error("error sending request", e);
    }

    params = new ModifiableSolrParams();

    params.set("action", CollectionParams.CollectionAction.REQUESTSTATUS.toString());
    params.set(CollectionHandlingUtils.REQUESTID, "1002");

    try {
      NamedList<Object> response = sendStatusRequestWithRetry(params, MAX_WAIT_TIMEOUT_SECONDS);
      message = (String) response.findRecursive("status", "msg");
    } catch (SolrServerException | IOException e) {
      log.error("error sending request", e);
    }

    assertEquals("found [1002] in failed tasks", message);

    final var duplicateRequestIdParams = new ModifiableSolrParams();
    duplicateRequestIdParams.set(
        CollectionParams.ACTION, CollectionParams.CollectionAction.CREATE.toString());
    duplicateRequestIdParams.set("name", "collection3");
    duplicateRequestIdParams.set("numShards", 1);
    duplicateRequestIdParams.set("replicationFactor", 1);
    duplicateRequestIdParams.set("collection.configName", "conf1");
    duplicateRequestIdParams.set(CommonAdminParams.ASYNC, "1002");

    final BaseHttpSolrClient.RemoteSolrException thrown =
        expectThrows(
            BaseHttpSolrClient.RemoteSolrException.class,
            () -> {
              sendRequest(duplicateRequestIdParams);
            });
    MatcherAssert.assertThat(
        thrown.getMessage(), containsString("Task with the same requestid already exists. (1002)"));
  }

  @SuppressWarnings("unchecked")
  private int numResponsesCompleted(NamedList<Object> response) {
    int sum = 0;
    for (String key : Arrays.asList("success", "failure")) {
      NamedList<Object> allStatuses = (NamedList<Object>) response.get(key);
      if (allStatuses != null) {
        for (Map.Entry<String, Object> tuple : allStatuses) {
          NamedList<Object> statusResponse = (NamedList<Object>) tuple.getValue();
          if (statusResponse.indexOf("STATUS", 0) >= 0) {
            sum += 1;
          }
        }
      }
    }
    return sum;
  }

  /**
   * Helper method to send a status request with specific retry limit and return the message/null
   * from the success response.
   */
  private NamedList<Object> sendStatusRequestWithRetry(ModifiableSolrParams params, int maxCounter)
      throws SolrServerException, IOException {
    NamedList<Object> r = null;
    while (maxCounter-- > 0) {
      r = sendRequest(params);
      @SuppressWarnings("unchecked")
      final NamedList<Object> status = (NamedList<Object>) r.get("status");
      final RequestStatusState state = RequestStatusState.fromKey((String) status.get("state"));

      if (state == RequestStatusState.COMPLETED || state == RequestStatusState.FAILED) {
        return r;
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        break;
      }
    }
    // Return last state?
    return r;
  }

  protected NamedList<Object> sendRequest(ModifiableSolrParams params)
      throws SolrServerException, IOException {
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    String baseUrl = shardToJetty.get(SHARD1).get(0).jetty.getBaseUrl().toString();

    try (SolrClient baseServer =
        new HttpSolrClient.Builder(baseUrl)
            .withConnectionTimeout(15000, TimeUnit.MILLISECONDS)
            .build()) {
      return baseServer.request(request);
    }
  }
}
