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
package org.apache.solr.client.solrj.request;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.solr.common.params.UpdateParams;
import org.junit.Test;

public class CommitOptionsTest {

  @Test
  public void testDefaultOptions() {
    CommitOptions options = new CommitOptions();
    assertTrue(options.waitSearcher());
    assertTrue(options.openSearcher());
    assertFalse(options.softCommit());
    assertFalse(options.expungeDeletes());
    assertEquals(Integer.MAX_VALUE, options.maxOptimizeSegments());
  }

  @Test
  public void testFluentAPI() {
    CommitOptions options =
        new CommitOptions()
            .withWaitSearcher(false)
            .withOpenSearcher(false)
            .withSoftCommit(true)
            .withExpungeDeletes(true)
            .withMaxOptimizeSegments(5);

    assertFalse(options.waitSearcher());
    assertFalse(options.openSearcher());
    assertTrue(options.softCommit());
    assertTrue(options.expungeDeletes());
    assertEquals(5, options.maxOptimizeSegments());
  }

  @Test
  public void testFactoryMethods() {
    // Hard commit
    CommitOptions hardCommit = CommitOptions.forHardCommit();
    assertFalse(hardCommit.softCommit());
    assertTrue(hardCommit.waitSearcher());

    // Soft commit
    CommitOptions softCommit = CommitOptions.forSoftCommit();
    assertTrue(softCommit.softCommit());
    assertTrue(softCommit.waitSearcher());

    // Optimize
    CommitOptions optimize = CommitOptions.forOptimize();
    assertTrue(optimize.expungeDeletes());
    assertEquals(1, optimize.maxOptimizeSegments());

    // Optimize with max segments
    CommitOptions optimizeWithMaxSegments = CommitOptions.forOptimize(3);
    assertTrue(optimizeWithMaxSegments.expungeDeletes());
    assertEquals(3, optimizeWithMaxSegments.maxOptimizeSegments());
  }

  @Test
  public void testMaxOptimizeSegmentsValidation() {
    CommitOptions options = new CommitOptions();

    // Valid values should work
    options = options.withMaxOptimizeSegments(1);
    assertEquals(1, options.maxOptimizeSegments());

    options = options.withMaxOptimizeSegments(100);
    assertEquals(100, options.maxOptimizeSegments());

    // Invalid values should throw exception
    try {
      options.withMaxOptimizeSegments(0);
      fail("Expected IllegalArgumentException for maxOptimizeSegments = 0");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("maxOptimizeSegments must be >= 1"));
    }

    try {
      options.withMaxOptimizeSegments(-1);
      fail("Expected IllegalArgumentException for maxOptimizeSegments = -1");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("maxOptimizeSegments must be >= 1"));
    }
  }

  @Test
  public void testEqualsAndHashCode() {
    // While Records automatically generate equals() and hashCode(), we test them here
    // to verify the behavior and catch any regressions
    CommitOptions options1 =
        new CommitOptions().withWaitSearcher(false).withSoftCommit(true).withMaxOptimizeSegments(5);

    CommitOptions options2 =
        new CommitOptions().withWaitSearcher(false).withSoftCommit(true).withMaxOptimizeSegments(5);

    CommitOptions options3 =
        new CommitOptions().withWaitSearcher(true).withSoftCommit(true).withMaxOptimizeSegments(5);

    assertEquals(options1, options2);
    assertEquals(options1.hashCode(), options2.hashCode());

    assertNotEquals(options1, options3);
    assertNotEquals(options1.hashCode(), options3.hashCode());
  }

  @Test
  public void testToString() {
    // While Records automatically generate toString(), we test the format here
    // for documentation and regression testing purposes
    CommitOptions options =
        new CommitOptions()
            .withWaitSearcher(false)
            .withSoftCommit(true)
            .withExpungeDeletes(true)
            .withMaxOptimizeSegments(3);

    String str = options.toString();
    assertTrue(str.contains("waitSearcher=false"));
    assertTrue(str.contains("softCommit=true"));
    assertTrue(str.contains("expungeDeletes=true"));
    assertTrue(str.contains("maxOptimizeSegments=3"));
  }

  @Test
  public void testUpdateRequestIntegration() {
    UpdateRequest updateRequest = new UpdateRequest();

    // Test with hard commit
    CommitOptions hardCommit =
        CommitOptions.forHardCommit().withWaitSearcher(false).withOpenSearcher(true);

    updateRequest.setAction(AbstractUpdateRequest.ACTION.COMMIT, hardCommit);

    assertEquals("true", updateRequest.getParams().get(UpdateParams.COMMIT));
    assertEquals("false", updateRequest.getParams().get(UpdateParams.SOFT_COMMIT));
    assertEquals("false", updateRequest.getParams().get(UpdateParams.WAIT_SEARCHER));
    assertEquals("true", updateRequest.getParams().get(UpdateParams.OPEN_SEARCHER));

    // Test with optimize
    UpdateRequest optimizeRequest = new UpdateRequest();
    CommitOptions optimize =
        CommitOptions.forOptimize(5).withWaitSearcher(true).withExpungeDeletes(false);

    optimizeRequest.setAction(AbstractUpdateRequest.ACTION.OPTIMIZE, optimize);

    assertEquals("true", optimizeRequest.getParams().get(UpdateParams.OPTIMIZE));
    assertEquals("5", optimizeRequest.getParams().get(UpdateParams.MAX_OPTIMIZE_SEGMENTS));
    assertEquals("true", optimizeRequest.getParams().get(UpdateParams.WAIT_SEARCHER));
    assertEquals("false", optimizeRequest.getParams().get(UpdateParams.EXPUNGE_DELETES));
  }

  @Test
  public void testBackwardCompatibilityWithDeprecatedMethods() {
    UpdateRequest updateRequest = new UpdateRequest();

    // Test that the deprecated method still works
    @SuppressWarnings("deprecation")
    AbstractUpdateRequest result =
        updateRequest.setAction(
            AbstractUpdateRequest.ACTION.COMMIT,
            false, // waitFlush (ignored)
            true // waitSearcher
            );

    assertSame(updateRequest, result);
    assertEquals("true", updateRequest.getParams().get(UpdateParams.COMMIT));
    assertEquals("true", updateRequest.getParams().get(UpdateParams.WAIT_SEARCHER));
  }
}
