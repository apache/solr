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
package org.apache.solr.common.util;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.stringContainsInOrder;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.SolrTestCaseJ4;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

public class TestObjectReleaseTracker extends SolrTestCaseJ4 {

  @Test
  public void testReleaseObject() {
    Object obj = new Object();
    ObjectReleaseTracker.track(obj);
    ObjectReleaseTracker.release(obj);
    assertNull(ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty());

    Object obj1 = new Object();
    ObjectReleaseTracker.track(obj1);
    Object obj2 = new Object();
    ObjectReleaseTracker.track(obj2);
    Object obj3 = new Object();
    ObjectReleaseTracker.track(obj3);

    ObjectReleaseTracker.release(obj1);
    ObjectReleaseTracker.release(obj2);
    ObjectReleaseTracker.release(obj3);
    assertNull(ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty());
  }

  @Test
  public void testUnreleased() {
    Object obj1 = new Object();
    Object obj2 = new Object();
    Object obj3 = new Object();

    ObjectReleaseTracker.track(obj1);
    ObjectReleaseTracker.track(obj2);
    ObjectReleaseTracker.track(obj3);

    ObjectReleaseTracker.release(obj1);
    ObjectReleaseTracker.release(obj2);
    // ObjectReleaseTracker.release(obj3);

    assertNotNull(ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty());
    assertNull(ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty());
  }

  @Test
  public void testReleaseDifferentObject() {
    ObjectReleaseTracker.track(new Object());
    ObjectReleaseTracker.release(new Object());
    assertNotNull(ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty());
    assertNull(ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty());
  }

  @Test
  public void testAnonymousClasses() {
    ObjectReleaseTracker.track(new Object() {});
    String message = ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty();
    MatcherAssert.assertThat(message, containsString("[Object]"));
  }

  @Test
  public void testAsyncTracking() throws InterruptedException, ExecutionException {
    ExecutorService es =
        ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrNamedThreadFactory("TestExec"));
    Object trackable = new Object();

    Future<?> result =
        es.submit(
            () -> {
              ObjectReleaseTracker.track(trackable);
            });

    result.get(); // make sure that track has been called
    String message = ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty();
    MatcherAssert.assertThat(
        message,
        stringContainsInOrder(
            ObjectReleaseTracker.ObjectTrackerException.class.getName(),
            "Exception: Submitter stack trace",
            getClassName() + "." + getTestName()));

    // Test the grandparent submitter case
    AtomicReference<Future<?>> indirectResult = new AtomicReference<>();
    result =
        es.submit(
            () ->
                indirectResult.set(
                    es.submit(
                        () -> {
                          ObjectReleaseTracker.track(trackable);
                        })));

    result.get();
    indirectResult.get().get();
    message = ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty();
    MatcherAssert.assertThat(
        message,
        stringContainsInOrder(
            ObjectReleaseTracker.ObjectTrackerException.class.getName(),
            "Exception: Submitter stack trace",
            "Exception: Submitter stack trace",
            getClassName() + "." + getTestName()));

    // Now test great-grandparent, which we don't explicitly account for, but should have been
    // recursively set
    AtomicReference<Future<?>> indirectIndirect = new AtomicReference<>();
    result =
        es.submit(
            () ->
                indirectResult.set(
                    es.submit(
                        () ->
                            indirectIndirect.set(
                                es.submit(
                                    () -> {
                                      ObjectReleaseTracker.track(trackable);
                                    })))));

    result.get();
    indirectResult.get().get();
    indirectIndirect.get().get();
    message = ObjectReleaseTracker.clearObjectTrackerAndCheckEmpty();
    MatcherAssert.assertThat(
        message,
        stringContainsInOrder(
            ObjectReleaseTracker.ObjectTrackerException.class.getName(),
            "Exception: Submitter stack trace",
            "Exception: Submitter stack trace",
            "Exception: Submitter stack trace",
            getClassName() + "." + getTestName()));

    es.shutdown();
    assertTrue(es.awaitTermination(1, TimeUnit.SECONDS));
  }
}
