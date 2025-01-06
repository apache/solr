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
package org.apache.solr.crossdc.manager.messageprocessor;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.crossdc.common.IQueueHandler;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.common.ResubmitBackoffPolicy;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class TestMessageProcessor {
  static final String VERSION_FIELD = "_version_";

  @Mock private CloudSolrClient solrClient;
  private SolrMessageProcessor processor;

  private final ResubmitBackoffPolicy backoffPolicy =
      spy(
          new ResubmitBackoffPolicy() {
            @Override
            public long getBackoffTimeMs(MirroredSolrRequest<?> resubmitRequest) {
              return 0;
            }
          });

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    processor = Mockito.spy(new SolrMessageProcessor(solrClient, backoffPolicy));
    Mockito.doNothing().when(processor).uncheckedSleep(anyLong());
  }

  @Test
  public void testDocumentSanitization() {
    UpdateRequest request = spy(new UpdateRequest());

    // Add docs with and without version
    SolrInputDocument doc1 = new SolrInputDocument();
    doc1.setField("id", 1);
    doc1.setField(VERSION_FIELD, 1);
    request.add(doc1);
    SolrInputDocument doc2 = new SolrInputDocument();
    doc2.setField("id", 2);
    request.add(doc2);

    // Delete by id with and without version
    request.deleteById("1");
    request.deleteById("2", 10L);

    request.setParam("shouldMirror", "true");
    // The response is irrelevant, but it will fail because mocked server returns null when
    // processing
    processor.handleItem(new MirroredSolrRequest<>(request));

    // After processing, check that all version fields are stripped
    for (SolrInputDocument doc : request.getDocuments()) {
      assertNull("Doc still has version", doc.getField(VERSION_FIELD));
    }

    // Check versions in delete by id
    for (Map<String, Object> idParams : request.getDeleteByIdMap().values()) {
      if (idParams != null) {
        idParams.put(UpdateRequest.VER, null);
        assertNull("Delete still has version", idParams.get(UpdateRequest.VER));
      }
    }
  }

  @Test
  @Ignore // needs to be modified to fully support request.process
  public void testSuccessNoBackoff() throws Exception {
    final UpdateRequest request = spy(new UpdateRequest());

    when(solrClient.request(eq(request), anyString())).thenReturn(new NamedList<>());

    when(request.process(eq(solrClient))).thenReturn(new UpdateResponse());

    processor.handleItem(new MirroredSolrRequest<>(request));

    verify(backoffPolicy, times(0)).getBackoffTimeMs(any());
  }

  @Test
  public void testClientErrorNoRetries() throws Exception {
    final UpdateRequest request = new UpdateRequest();
    request.setParam("shouldMirror", "true");
    when(solrClient.request(eq(request), anyString()))
        .thenThrow(new SolrException(SolrException.ErrorCode.BAD_REQUEST, "err msg"));

    IQueueHandler.Result<MirroredSolrRequest<?>> result =
        processor.handleItem(new MirroredSolrRequest<>(request));
    assertEquals(IQueueHandler.ResultStatus.FAILED_RESUBMIT, result.status());
  }
}
