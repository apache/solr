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
    assertTrue(options.getWaitSearcher());
    assertTrue(options.getOpenSearcher());
    assertFalse(options.getSoftCommit());
    assertFalse(options.getExpungeDeletes());
    assertEquals(Integer.MAX_VALUE, options.getMaxOptimizeSegments());
  }

  @Test
  public void testFluentAPI() {
    CommitOptions options =
        new CommitOptions()
            .waitSearcher(false)
            .openSearcher(false)
            .softCommit(true)
            .expungeDeletes(true)
            .maxOptimizeSegments(5);

    assertFalse(options.getWaitSearcher());
    assertFalse(options.getOpenSearcher());
    assertTrue(options.getSoftCommit());
    assertTrue(options.getExpungeDeletes());
    assertEquals(5, options.getMaxOptimizeSegments());
  }

  @Test
  public void testFactoryMethods() {
    // Hard commit
    CommitOptions hardCommit = CommitOptions.hardCommit();
    assertFalse(hardCommit.getSoftCommit());
    assertTrue(hardCommit.getWaitSearcher());

    // Soft commit
    CommitOptions softCommit = CommitOptions.softCommit();
    assertTrue(softCommit.getSoftCommit());
    assertTrue(softCommit.getWaitSearcher());

    // Optimize
    CommitOptions optimize = CommitOptions.optimize();
    assertTrue(optimize.getExpungeDeletes());
    assertEquals(Integer.MAX_VALUE, optimize.getMaxOptimizeSegments());

    // Optimize with max segments
    CommitOptions optimizeWithMaxSegments = CommitOptions.optimize(3);
    assertTrue(optimizeWithMaxSegments.getExpungeDeletes());
    assertEquals(3, optimizeWithMaxSegments.getMaxOptimizeSegments());
  }

  @Test
  public void testMaxOptimizeSegmentsValidation() {
    CommitOptions options = new CommitOptions();

    // Valid values should work
    options.maxOptimizeSegments(1);
    assertEquals(1, options.getMaxOptimizeSegments());

    options.maxOptimizeSegments(100);
    assertEquals(100, options.getMaxOptimizeSegments());

    // Invalid values should throw exception
    try {
      options.maxOptimizeSegments(0);
      fail("Expected IllegalArgumentException for maxOptimizeSegments = 0");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("maxOptimizeSegments must be >= 1"));
    }

    try {
      options.maxOptimizeSegments(-1);
      fail("Expected IllegalArgumentException for maxOptimizeSegments = -1");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("maxOptimizeSegments must be >= 1"));
    }
  }

  @Test
  public void testEqualsAndHashCode() {
    CommitOptions options1 =
        new CommitOptions().waitSearcher(false).softCommit(true).maxOptimizeSegments(5);

    CommitOptions options2 =
        new CommitOptions().waitSearcher(false).softCommit(true).maxOptimizeSegments(5);

    CommitOptions options3 =
        new CommitOptions().waitSearcher(true).softCommit(true).maxOptimizeSegments(5);

    assertEquals(options1, options2);
    assertEquals(options1.hashCode(), options2.hashCode());

    assertNotEquals(options1, options3);
    assertNotEquals(options1.hashCode(), options3.hashCode());
  }

  @Test
  public void testToString() {
    CommitOptions options =
        new CommitOptions()
            .waitSearcher(false)
            .softCommit(true)
            .expungeDeletes(true)
            .maxOptimizeSegments(3);

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
    CommitOptions hardCommit = CommitOptions.hardCommit().waitSearcher(false).openSearcher(true);

    updateRequest.setAction(AbstractUpdateRequest.ACTION.COMMIT, hardCommit);

    assertEquals("true", updateRequest.getParams().get(UpdateParams.COMMIT));
    assertEquals("false", updateRequest.getParams().get(UpdateParams.SOFT_COMMIT));
    assertEquals("false", updateRequest.getParams().get(UpdateParams.WAIT_SEARCHER));
    assertEquals("true", updateRequest.getParams().get(UpdateParams.OPEN_SEARCHER));

    // Test with optimize
    UpdateRequest optimizeRequest = new UpdateRequest();
    CommitOptions optimize = CommitOptions.optimize(5).waitSearcher(true).expungeDeletes(false);

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
