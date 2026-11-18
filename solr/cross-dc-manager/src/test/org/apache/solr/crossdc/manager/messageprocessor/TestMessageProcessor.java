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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.crossdc.common.IQueueHandler;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.common.ResubmitBackoffPolicy;
import org.apache.solr.crossdc.manager.CrossDcMockUtils;
import org.apache.solr.crossdc.manager.consumer.ConsumerMetrics;
import org.apache.solr.crossdc.manager.consumer.OtelMetrics;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

public class TestMessageProcessor {
  private CloudSolrClient solrClient;
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
    solrClient = CrossDcMockUtils.mockCloudSolrClientWithClusterStateProvider();

    ConsumerMetrics metrics = Mockito.mock(OtelMetrics.class);
    processor = Mockito.spy(new SolrMessageProcessor(metrics, () -> solrClient, backoffPolicy));
    Mockito.doNothing().when(processor).uncheckedSleep(anyLong());
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
