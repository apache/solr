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

import java.util.List;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.junit.Test;

/** Tests for {@link DistributedMultiLock}. */
public class DistributedMultiLockTest extends SolrTestCase {

  @Test
  public void testReleaseFailureStillReleasesOtherLocks() {
    StubLock failing =
        new StubLock(new SolrException(SolrException.ErrorCode.SERVER_ERROR, "first"));
    StubLock alsoFailing =
        new StubLock(new SolrException(SolrException.ErrorCode.SERVER_ERROR, "second"));
    StubLock healthy = new StubLock(null);
    DistributedMultiLock multiLock =
        new DistributedMultiLock(List.of(failing, alsoFailing, healthy));

    SolrException e = expectThrows(SolrException.class, multiLock::release);

    assertEquals("first", e.getMessage());
    assertEquals(1, e.getSuppressed().length);
    assertEquals("second", e.getSuppressed()[0].getMessage());
    assertTrue(failing.releaseCalled);
    assertTrue(alsoFailing.releaseCalled);
    assertTrue(healthy.releaseCalled);
  }

  private static class StubLock implements DistributedLock {
    private final SolrException releaseFailure;
    private boolean releaseCalled;

    StubLock(SolrException releaseFailure) {
      this.releaseFailure = releaseFailure;
    }

    @Override
    public void waitUntilAcquired() {}

    @Override
    public void release() {
      releaseCalled = true;
      if (releaseFailure != null) {
        throw releaseFailure;
      }
    }

    @Override
    public String getLockId() {
      return null;
    }

    @Override
    public boolean isMirroringLock() {
      return false;
    }
  }
}
