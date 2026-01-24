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

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.params.UpdateParams;
import org.junit.Test;

public class CommitOptionsTest extends SolrTestCase {

  @Test
  public void testMaxOptimizeSegmentsValidation() {
    CommitOptions options = new CommitOptions();

    // Valid values should work
    options = options.withMaxOptimizeSegments(1);
    assertEquals(1, options.maxOptimizeSegments());

    options = options.withMaxOptimizeSegments(100);
    assertEquals(100, options.maxOptimizeSegments());

    // Invalid values should throw exception
    CommitOptions finalOptions = options;
    IllegalArgumentException e1 =
        expectThrows(IllegalArgumentException.class, () -> finalOptions.withMaxOptimizeSegments(0));
    assertTrue(e1.getMessage().contains("maxOptimizeSegments must be >= 1"));

    CommitOptions finalOptions1 = options;
    IllegalArgumentException e2 =
        expectThrows(
            IllegalArgumentException.class, () -> finalOptions1.withMaxOptimizeSegments(-1));
    assertTrue(e2.getMessage().contains("maxOptimizeSegments must be >= 1"));
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
