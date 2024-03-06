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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.crossdc.common.IQueueHandler;
import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.common.ResubmitBackoffPolicy;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class SolrMessageProcessorTest {
    private SolrMessageProcessor solrMessageProcessor;
    private CloudSolrClient client;
    private ResubmitBackoffPolicy resubmitBackoffPolicy;

    @BeforeClass
    public static void ensureWorkingMockito() {
        assumeWorkingMockito();
    }

    @Before
    public void setUp() {
        client = mock(CloudSolrClient.class);
        resubmitBackoffPolicy = mock(ResubmitBackoffPolicy.class);
        solrMessageProcessor = new SolrMessageProcessor(client, resubmitBackoffPolicy);
    }

    /**
     * Should handle MirroredSolrRequest and return a failed result with no retry
     */
    @Test
    public void handleItemWithFailedResultNoRetry() throws SolrServerException, IOException {
        MirroredSolrRequest mirroredSolrRequest = mock(MirroredSolrRequest.class);
        SolrRequest solrRequest = mock(SolrRequest.class);
        when(mirroredSolrRequest.getType()).thenReturn(MirroredSolrRequest.Type.UPDATE);
        when(mirroredSolrRequest.getSolrRequest()).thenReturn(solrRequest);

        SolrResponseBase solrResponseBase = mock(SolrResponseBase.class);
        when(solrRequest.process(client)).thenReturn(solrResponseBase);
        when(solrResponseBase.getResponse()).thenReturn(new NamedList<>());
        when(solrResponseBase.getStatus()).thenReturn(ErrorCode.BAD_REQUEST.code);
        when(client.request(any(SolrRequest.class))).thenReturn(new NamedList<>());

        IQueueHandler.Result<MirroredSolrRequest> result = solrMessageProcessor.handleItem(mirroredSolrRequest);

        assertEquals(IQueueHandler.ResultStatus.FAILED_RESUBMIT, result.status());
    }

    /**
     * Should handle MirroredSolrRequest and return a failed result with resubmit
     */
    @Test
    public void handleItemWithFailedResultResubmit() throws SolrServerException, IOException {
        MirroredSolrRequest mirroredSolrRequest = mock(MirroredSolrRequest.class);
        SolrRequest solrRequest = mock(SolrRequest.class);
        when(mirroredSolrRequest.getType()).thenReturn(MirroredSolrRequest.Type.UPDATE);
        when(mirroredSolrRequest.getSolrRequest()).thenReturn(solrRequest);
        when(solrRequest.process(client))
                .thenThrow(new SolrException(ErrorCode.SERVER_ERROR, "Server error"));

        IQueueHandler.Result<MirroredSolrRequest> result = solrMessageProcessor.handleItem(mirroredSolrRequest);

        assertEquals(IQueueHandler.ResultStatus.FAILED_RESUBMIT, result.status());
        assertEquals(mirroredSolrRequest, result.getItem());
    }

    /**
     * Should handle MirroredSolrRequest and return a successful result
     */
    @Test
    public void handleItemWithSuccessfulResult() throws SolrServerException, IOException {
        MirroredSolrRequest mirroredSolrRequest = mock(MirroredSolrRequest.class);
        SolrRequest solrRequest = mock(SolrRequest.class);
        SolrResponseBase solrResponse = mock(SolrResponseBase.class);

        when(mirroredSolrRequest.getType()).thenReturn(MirroredSolrRequest.Type.UPDATE);
        when(mirroredSolrRequest.getSolrRequest()).thenReturn(solrRequest);
        when(solrRequest.process(client)).thenReturn(solrResponse);
        when(solrResponse.getStatus()).thenReturn(0);

        IQueueHandler.Result<MirroredSolrRequest> result = solrMessageProcessor.handleItem(mirroredSolrRequest);

        assertEquals(IQueueHandler.ResultStatus.HANDLED, result.status());
        assertNull(result.getItem());
    }

    /**
     * Should connect to Solr if not connected and process the request
     */
    @Test
    public void connectToSolrIfNeededAndProcessRequest() throws SolrServerException, IOException {
        MirroredSolrRequest mirroredSolrRequest = mock(MirroredSolrRequest.class);
        SolrRequest solrRequest = mock(SolrRequest.class);
        SolrResponseBase solrResponse = mock(SolrResponseBase.class);

        when(mirroredSolrRequest.getType()).thenReturn(MirroredSolrRequest.Type.UPDATE);
        when(mirroredSolrRequest.getSolrRequest()).thenReturn(solrRequest);
        when(solrRequest.process(client)).thenReturn(solrResponse);
        when(solrResponse.getStatus()).thenReturn(0);

        IQueueHandler.Result<MirroredSolrRequest> result = solrMessageProcessor.handleItem(mirroredSolrRequest);

        assertEquals(IQueueHandler.ResultStatus.HANDLED, result.status());
        verify(client, times(1)).connect();
        verify(solrRequest, times(1)).process(client);
    }
}