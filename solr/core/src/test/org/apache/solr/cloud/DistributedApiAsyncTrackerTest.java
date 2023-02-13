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

import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

/** Test async id tracking scenarios as used when Collection API is distributed. */
public class DistributedApiAsyncTrackerTest extends SolrTestCaseJ4 {

  protected ZkTestServer zkServer;
  protected SolrZkClient zkClient;

  @Override
  public void setUp() throws Exception {
    super.setUp();

    zkServer = new ZkTestServer(createTempDir("zookeeperDir"));
    zkServer.run();
    zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkHost())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();

    zkServer.shutdown();
    zkClient.close();
  }

  @Test
  public void testBasic() throws Exception {
    DistributedApiAsyncTracker daat = new DistributedApiAsyncTracker(zkClient, "/basic");

    final String asyncId = "mario";

    assertTrue("Could not create async task " + asyncId, daat.createNewAsyncJobTracker(asyncId));
    assertFalse(
        "Should not have been able to create duplicate task " + asyncId,
        daat.createNewAsyncJobTracker(asyncId));

    daat.cancelAsyncId(asyncId);
    assertEquals(RequestStatusState.NOT_FOUND, daat.getAsyncTaskRequestStatus(asyncId).first());
    assertTrue(
        "Could not create async task after cancel " + asyncId,
        daat.createNewAsyncJobTracker(asyncId));

    assertEquals(RequestStatusState.SUBMITTED, daat.getAsyncTaskRequestStatus(asyncId).first());
    assertFalse(
        "Can't delete a non completed/failed task " + asyncId, daat.deleteSingleAsyncId(asyncId));
    daat.deleteAllAsyncIds();
    assertEquals(
        "Task should still be here because couldn't be deleted " + asyncId,
        RequestStatusState.SUBMITTED,
        daat.getAsyncTaskRequestStatus(asyncId).first());

    daat.setTaskRunning(asyncId);
    assertEquals(RequestStatusState.RUNNING, daat.getAsyncTaskRequestStatus(asyncId).first());

    NamedList<Object> nl = new NamedList<>();
    nl.add("MyList", "myValue");
    daat.setTaskCompleted(asyncId, new OverseerSolrResponse(nl));
    assertEquals(RequestStatusState.COMPLETED, daat.getAsyncTaskRequestStatus(asyncId).first());
    assertEquals(
        "Did not retrieve correct completed OverseerSolrResponse " + asyncId,
        "myValue",
        daat.getAsyncTaskRequestStatus(asyncId).second().getResponse().get("MyList"));

    assertTrue(
        "Should be able to delete a completed task " + asyncId, daat.deleteSingleAsyncId(asyncId));
    assertEquals(
        "A completed task should not be found",
        RequestStatusState.NOT_FOUND,
        daat.getAsyncTaskRequestStatus(asyncId).first());
  }

  @Test
  public void testDisconnect() throws Exception {
    final String TRACKER_ROOT = "/disconnect";
    DistributedApiAsyncTracker permanentDaat =
        new DistributedApiAsyncTracker(zkClient, TRACKER_ROOT);
    DistributedApiAsyncTracker permanentDaat2 =
        new DistributedApiAsyncTracker(zkClient, TRACKER_ROOT);

    final String asyncPermanent = "permanentAsync";
    final String asyncTransient = "transientAsync";

    assertTrue(
        "Could not create async task " + asyncPermanent,
        permanentDaat.createNewAsyncJobTracker(asyncPermanent));

    try (SolrZkClient transientZkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkHost())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      DistributedApiAsyncTracker transientDaat =
          new DistributedApiAsyncTracker(transientZkClient, TRACKER_ROOT);
      assertTrue(
          "Could not create async task " + asyncTransient,
          transientDaat.createNewAsyncJobTracker(asyncTransient));

      assertEquals(
          "permanentDaat can't see " + asyncPermanent,
          RequestStatusState.SUBMITTED,
          permanentDaat.getAsyncTaskRequestStatus(asyncPermanent).first());
      assertEquals(
          "permanentDaat2 can't see " + asyncPermanent,
          RequestStatusState.SUBMITTED,
          permanentDaat2.getAsyncTaskRequestStatus(asyncPermanent).first());
      assertEquals(
          "transientDaat can't see " + asyncPermanent,
          RequestStatusState.SUBMITTED,
          transientDaat.getAsyncTaskRequestStatus(asyncPermanent).first());
      assertEquals(
          "permanentDaat can't see " + asyncTransient,
          RequestStatusState.SUBMITTED,
          permanentDaat.getAsyncTaskRequestStatus(asyncTransient).first());
      assertEquals(
          "permanentDaat2 can't see " + asyncTransient,
          RequestStatusState.SUBMITTED,
          permanentDaat2.getAsyncTaskRequestStatus(asyncTransient).first());
      assertEquals(
          "transientDaat can't see " + asyncTransient,
          RequestStatusState.SUBMITTED,
          transientDaat.getAsyncTaskRequestStatus(asyncTransient).first());
    }

    // transientDaat connection closed, doesn't change a thing for asyncPermanent...
    assertEquals(
        "permanentDaat can't see " + asyncPermanent,
        RequestStatusState.SUBMITTED,
        permanentDaat.getAsyncTaskRequestStatus(asyncPermanent).first());
    assertEquals(
        "permanentDaat2 can't see " + asyncPermanent,
        RequestStatusState.SUBMITTED,
        permanentDaat2.getAsyncTaskRequestStatus(asyncPermanent).first());

    // ...but asyncTransient is now failed.
    assertEquals(
        "permanentDaat can't see " + asyncTransient,
        RequestStatusState.FAILED,
        permanentDaat.getAsyncTaskRequestStatus(asyncTransient).first());
    assertEquals(
        "permanentDaat2 can't see " + asyncTransient,
        RequestStatusState.FAILED,
        permanentDaat2.getAsyncTaskRequestStatus(asyncTransient).first());
  }

  @Test
  public void testDisconnectAfterCompletion() throws Exception {
    final String TRACKER_ROOT = "/multiConn";
    DistributedApiAsyncTracker daat = new DistributedApiAsyncTracker(zkClient, TRACKER_ROOT);

    final String asyncId = "theId";

    try (SolrZkClient transientZkClient =
        new SolrZkClient.Builder()
            .withUrl(zkServer.getZkHost())
            .withTimeout(AbstractZkTestCase.TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      DistributedApiAsyncTracker transientDaat =
          new DistributedApiAsyncTracker(transientZkClient, TRACKER_ROOT);
      assertTrue(
          "Could not create async task " + asyncId,
          transientDaat.createNewAsyncJobTracker(asyncId));

      // The task completes, then connection lost
      NamedList<Object> nl = new NamedList<>();
      nl.add("status", "I made it");
      transientDaat.setTaskCompleted(asyncId, new OverseerSolrResponse(nl));

      assertEquals(
          "transientDaat can't see " + asyncId,
          RequestStatusState.COMPLETED,
          transientDaat.getAsyncTaskRequestStatus(asyncId).first());
      assertEquals(
          "transientDaat can't retrieve correct completed OverseerSolrResponse " + asyncId,
          "I made it",
          transientDaat.getAsyncTaskRequestStatus(asyncId).second().getResponse().get("status"));
      assertEquals(
          "daat can't retrieve correct completed OverseerSolrResponse " + asyncId,
          "I made it",
          daat.getAsyncTaskRequestStatus(asyncId).second().getResponse().get("status"));
    }

    // Even though connection was closed, the task should still be completed, and we should be able
    // to retrieve its response
    assertEquals(
        "daat can't see " + asyncId,
        RequestStatusState.COMPLETED,
        daat.getAsyncTaskRequestStatus(asyncId).first());
    assertEquals(
        "Did not retrieve correct completed OverseerSolrResponse after other connection closes "
            + asyncId,
        "I made it",
        daat.getAsyncTaskRequestStatus(asyncId).second().getResponse().get("status"));

    // And given it completed, it should be deleted
    daat.deleteAllAsyncIds();
    assertEquals(
        "task was not deleted " + asyncId,
        RequestStatusState.NOT_FOUND,
        daat.getAsyncTaskRequestStatus(asyncId).first());
  }

  @Test
  public void testIdCleanup() throws Exception {
    final int maxTasks = 30; // When cleaning up, 3 async id's will be removed
    DistributedApiAsyncTracker daat =
        new DistributedApiAsyncTracker(zkClient, "/manyIds", maxTasks);

    for (int asyncId = 1; asyncId <= maxTasks; asyncId++) {
      assertTrue(daat.createNewAsyncJobTracker(Integer.toString(asyncId)));
    }

    // All ids should be tracked
    assertEquals(
        RequestStatusState.SUBMITTED, daat.getAsyncTaskRequestStatus(Integer.toString(1)).first());
    assertEquals(
        RequestStatusState.SUBMITTED,
        daat.getAsyncTaskRequestStatus(Integer.toString(maxTasks)).first());

    // Adding one more should trigger cleanup of earlier id's
    assertTrue(daat.createNewAsyncJobTracker("straw"));

    String cleanedUpId1 = Integer.toString(1);

    assertEquals(
        "Cleanup should have been triggered and removed the first task",
        RequestStatusState.NOT_FOUND,
        daat.getAsyncTaskRequestStatus(cleanedUpId1).first());
    assertEquals(
        "Last task should not have been cleaned up",
        RequestStatusState.SUBMITTED,
        daat.getAsyncTaskRequestStatus(Integer.toString(maxTasks)).first());
    assertEquals(
        "Task having triggered cleanup should not have been cleaned up",
        RequestStatusState.SUBMITTED,
        daat.getAsyncTaskRequestStatus("straw").first());

    // Identical to the test 3 lines above but repeated to be considered in context
    assertEquals(
        "Cleaned up ID (1) should no longer be visible",
        RequestStatusState.NOT_FOUND,
        daat.getAsyncTaskRequestStatus(cleanedUpId1).first());
    // Creating a new task with same id fails but "revives" the ID if the task is still in progress
    assertFalse(
        "Should not be able to create a task with same id as task in progress",
        daat.createNewAsyncJobTracker(cleanedUpId1));
    assertEquals(
        "Cleaned up ID now visible again",
        RequestStatusState.SUBMITTED,
        daat.getAsyncTaskRequestStatus(cleanedUpId1).first());

    // 2 is also in progress and was also cleaned up. It should be markable complete without issues
    // (since that's what will happen when it completed).
    String cleanedUpId2 = Integer.toString(2);
    assertEquals(
        "Another cleaned up ID (2) should not be visible",
        RequestStatusState.NOT_FOUND,
        daat.getAsyncTaskRequestStatus(cleanedUpId2).first());
    NamedList<Object> nl = new NamedList<>();
    nl.add("code", "da vinci");
    daat.setTaskCompleted(cleanedUpId2, new OverseerSolrResponse(nl));

    assertEquals(
        "task should now be completed " + cleanedUpId2,
        RequestStatusState.COMPLETED,
        daat.getAsyncTaskRequestStatus(cleanedUpId2).first());
    assertEquals(
        "task should have correct OverseerSolrResponse " + cleanedUpId2,
        "da vinci",
        daat.getAsyncTaskRequestStatus(cleanedUpId2).second().getResponse().get("code"));

    // 3 is also in progress and was also cleaned up. It should be markable running without issues,
    // but that doesn't revive it (still not found).
    // (if we want setTaskRunning to revive a removed task, we'd have to check the persistent node
    // existence each time, which is likely not worth it)
    String cleanedUpId3 = Integer.toString(3);
    assertEquals(
        "Another cleaned up ID (3) should not be visible",
        RequestStatusState.NOT_FOUND,
        daat.getAsyncTaskRequestStatus(cleanedUpId3).first());
    daat.setTaskRunning(cleanedUpId3);
    assertEquals(
        "task should now be running " + cleanedUpId3,
        RequestStatusState.NOT_FOUND,
        daat.getAsyncTaskRequestStatus(cleanedUpId3).first());
  }
}
