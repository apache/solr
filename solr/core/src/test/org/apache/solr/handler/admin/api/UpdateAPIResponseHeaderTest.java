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
package org.apache.solr.handler.admin.api;

import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.api.model.ToleratedUpdateError;
import org.apache.solr.client.api.model.UpdateResponse;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.UpdateRequestHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Covers a bug flagged in review of SOLR-18457 (PR #4177): {@link UpdateAPI#handleUpdate} used to
 * unconditionally remove the entire legacy {@code responseHeader} after delegating to {@link
 * UpdateRequestHandler}, rather than just the duplicate {@code status}/{@code QTime} it was written
 * to suppress. Two real processors write payload into that header while handling a request --
 * {@code TolerantUpdateProcessor} ({@code errors}/{@code maxErrors}) and {@code
 * DistributedZkUpdateProcessor} ({@code rf}, the achieved replication factor) -- so {@link
 * UpdateAPI#handleUpdate} now copies that payload onto {@link UpdateResponse} before discarding the
 * header.
 *
 * <p>This test doesn't exercise the real processors (that's TolerantUpdateProcessorTest's job); it
 * fakes the handler to write exactly what they would, isolating the fix to {@code
 * UpdateAPI.handleUpdate} itself.
 */
public class UpdateAPIResponseHeaderTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }

  @Test
  public void testTolerantErrorsAndReplicationFactorSurviveResponseHeaderRemoval()
      throws Exception {
    final SolrQueryRequest req = req();
    final SolrQueryResponse rsp = new SolrQueryResponse();
    final UpdateAPI api =
        new UpdateAPI(
            new UpdateAPI.UpdateRequestHandlerConfig(new HeaderStuffingUpdateRequestHandler()),
            req,
            rsp);

    try {
      final UpdateResponse response = api.update(null, null, null, null, null, null);

      assertNull(
          "the legacy responseHeader itself should still be gone -- its payload should have "
              + "been copied onto the typed response instead, not left behind",
          rsp.getValues().get("responseHeader"));

      assertEquals(Integer.valueOf(1), response.rf);
      assertEquals(Integer.valueOf(10), response.maxErrors);
      assertNotNull(response.errors);
      assertEquals(1, response.errors.size());
      final ToleratedUpdateError error = response.errors.get(0);
      assertEquals("ADD", error.type);
      assertEquals("bad-doc-1", error.id);
      assertEquals("ERROR: failed to add doc", error.message);
    } finally {
      req.close();
    }
  }

  /**
   * Simulates what {@code TolerantUpdateProcessor.finish()} (errors/maxErrors) and {@code
   * DistributedZkUpdateProcessor} (rf) do to a real request's response header, without needing a
   * real tolerant-update chain or SolrCloud cluster.
   */
  private static class HeaderStuffingUpdateRequestHandler extends UpdateRequestHandler {
    @Override
    public boolean handleRequestWithoutMetrics(SolrQueryRequest req, SolrQueryResponse rsp) {
      // TolerantUpdateProcessor.java:276,279
      final SimpleOrderedMap<String> toleratedError = new SimpleOrderedMap<>();
      toleratedError.add("type", "ADD");
      toleratedError.add("id", "bad-doc-1");
      toleratedError.add("message", "ERROR: failed to add doc");
      rsp.getResponseHeader().add("errors", List.of(toleratedError));
      rsp.getResponseHeader().add("maxErrors", 10);

      // DistributedZkUpdateProcessor.java:1391
      rsp.getResponseHeader().add("rf", 1);
      return true;
    }
  }
}
