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
package org.apache.solr.crossdc.update.processor;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.crossdc.common.CrossDcConf;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MirroringUpdateProcessorTest extends SolrTestCaseJ4 {

    private UpdateRequestProcessor next;
    private MirroringUpdateProcessor processor;
    private RequestMirroringHandler requestMirroringHandler;
    private AddUpdateCommand addUpdateCommand;
    private DeleteUpdateCommand deleteUpdateCommand;
    private CommitUpdateCommand commitUpdateCommand;
    private SolrQueryRequestBase req;
    UpdateRequest requestMock;
    private UpdateRequestProcessor nextProcessor;
    private SolrCore core;
    private HttpSolrClient.Builder builder = mock(HttpSolrClient.Builder.class);
    private HttpSolrClient client = mock(HttpSolrClient.class);
    private CloudDescriptor cloudDesc;
    private ZkStateReader zkStateReader;
    private Replica replica;
    private ProducerMetrics producerMetrics;

    @BeforeClass
    public static void ensureWorkingMockito() {
        assumeWorkingMockito();
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        addUpdateCommand = new AddUpdateCommand(req);
        addUpdateCommand.solrDoc = new SolrInputDocument();
        addUpdateCommand.solrDoc.addField("id", "test");
        req = mock(SolrQueryRequestBase.class);
        when(req.getParams()).thenReturn(new ModifiableSolrParams());

        requestMock = mock(UpdateRequest.class);
        addUpdateCommand.setReq(req);

        nextProcessor = mock(UpdateRequestProcessor.class);

        IndexSchema schema = mock(IndexSchema.class);
        when(req.getSchema()).thenReturn(schema);=

        deleteUpdateCommand = new DeleteUpdateCommand(req);
        deleteUpdateCommand.query = "*:*";

        // set all flags to verify they are passed on
        commitUpdateCommand = new CommitUpdateCommand(req, true);
        commitUpdateCommand.prepareCommit = true;
        commitUpdateCommand.maxOptimizeSegments = 10;
        commitUpdateCommand.softCommit = true;
        commitUpdateCommand.expungeDeletes = true;
        commitUpdateCommand.openSearcher = true;
        commitUpdateCommand.waitSearcher = true;

        producerMetrics = spy(new ProducerMetrics(mock(SolrMetricsContext.class), mock(SolrCore.class)) {
            private final Counter counterMock = mock(Counter.class);

            public Counter getLocal() {
                return counterMock;
            }

            public Counter getLocalError() {
                return counterMock;
            }

            public Counter getSubmitted() {
                return counterMock;
            }

            public Counter getDocumentTooLarge() {
                return counterMock;
            }

            public Counter getSubmitError() {
                return counterMock;
            }

            public Histogram getDocumentSize() {
                return mock(Histogram.class);
            }
        });

        next = mock(UpdateRequestProcessor.class);
        requestMirroringHandler = mock(RequestMirroringHandler.class);
        processor =
                new MirroringUpdateProcessor(
                        next,
                        true,
                        true,
                        true,
                        CrossDcConf.ExpandDbq.EXPAND,
                        1000L,
                        new ModifiableSolrParams(),
                        DistributedUpdateProcessor.DistribPhase.NONE,
                        requestMirroringHandler,
                        producerMetrics) {
                    UpdateRequest createMirrorRequest() {
                        return requestMock;
                    }
                };

        core = mock(SolrCore.class);
        CoreDescriptor coreDesc = mock(CoreDescriptor.class);
        cloudDesc = mock(CloudDescriptor.class);
        when(cloudDesc.getShardId()).thenReturn("shard1");
        CoreContainer coreContainer = mock(CoreContainer.class);
        ZkController zkController = mock(ZkController.class);
        ClusterState clusterState = mock(ClusterState.class);
        DocCollection docCollection = mock(DocCollection.class);
        DocRouter docRouter = mock(DocRouter.class);
        Slice slice = mock(Slice.class);
        when(slice.getName()).thenReturn("shard1");
        zkStateReader = mock(ZkStateReader.class);
        replica = mock(Replica.class);

        when(replica.getName()).thenReturn("replica1");
        when(zkStateReader.getLeaderRetry(any(), any()))
            .thenReturn(replica);
        when(zkController.getZkStateReader()).thenReturn(zkStateReader);
        when(coreDesc.getCloudDescriptor()).thenReturn(cloudDesc);
        when(clusterState.getCollection(any())).thenReturn(docCollection);
        when(docCollection.getRouter()).thenReturn(docRouter);
        when(
            docRouter.getTargetSlice(
                any(),
                any(),
                any(),
                any(),
                any()))
            .thenReturn(slice);
        when(docCollection.getSlicesMap()).thenReturn(Map.of("shard1", slice));
        when(zkController.getClusterState()).thenReturn(clusterState);
        when(coreContainer.getZkController()).thenReturn(zkController);
        when(core.getCoreContainer()).thenReturn(coreContainer);
        when(core.getCoreDescriptor()).thenReturn(coreDesc);
        when(req.getCore()).thenReturn(core);
    }

    /**
     * Should process delete command and mirror the document when the distribPhase is NONE and
     * deleteById is false
     */
    @Test
    public void processDeleteWhenDistribPhaseIsNoneAndDeleteByIdIsFalse() {
        try {
            processor.processDelete(deleteUpdateCommand);
            verify(requestMirroringHandler, times(1)).mirror(requestMock);
        } catch (Exception e) {
            fail("IOException should not be thrown");
        }
    }

    /**
     * Should process add command and mirror the document when the document size is within the limit
     * and the node is a leader
     */
    @Test
    public void processAddWhenDocSizeWithinLimitAndNodeIsLeader() {
        try {
            when(cloudDesc.getCoreNodeName()).thenReturn("replica1");
            processor.processAdd(addUpdateCommand);
            verify(requestMirroringHandler, times(1)).mirror(requestMock);
        } catch (IOException e) {
            fail("IOException should not be thrown");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Should process delete command and mirror the document when the node is a leader and
     * deleteById is true
     */
    @Test
    public void processDeleteWhenNodeIsLeaderAndDeleteByIdIsTrue() {
        try {
            when(cloudDesc.getCoreNodeName()).thenReturn("replica1");
            deleteUpdateCommand.setId("test");
            processor.processDelete(deleteUpdateCommand);
            verify(requestMirroringHandler, times(1)).mirror(requestMock);
        } catch (Exception e) {
            fail("IOException should not be thrown");
        }
    }

    @Test
    public void processCommitOnlyAnotherShard() {
        try {
            // should skip if processing in other shard than the first
            when(cloudDesc.getShardId()).thenReturn("shard2");
            processor.processCommit(commitUpdateCommand);
            verify(next).processCommit(commitUpdateCommand);
            verify(requestMirroringHandler, times(0)).mirror(requestMock);
        } catch (Exception e) {
            fail("IOException should not be thrown: " + e);
        }
    }

    @Test
    public void processCommitOnlyNonLeader() {
        try {
            // should skip if processing on non-leader replica
            when(replica.getName()).thenReturn("foobar");
            when(cloudDesc.getCoreNodeName()).thenReturn("replica1");
            processor.processCommit(commitUpdateCommand);
            verify(next).processCommit(commitUpdateCommand);
            verify(requestMirroringHandler, times(0)).mirror(requestMock);
        } catch (Exception e) {
            fail("IOException should not be thrown: " + e);
        }
    }

    @Test
    public void processCommitOnlyLeader() {
        try {
            when(cloudDesc.getCoreNodeName()).thenReturn("replica1");
            // don't override createMirrorRequest, call actual method
            processor =
                new MirroringUpdateProcessor(
                    next,
                    true,
                    true,
                    true,
                    CrossDcConf.ExpandDbq.EXPAND,
                    1000L,
                    new ModifiableSolrParams(),
                    DistributedUpdateProcessor.DistribPhase.NONE,
                    requestMirroringHandler,
                    producerMetrics);
            ArgumentCaptor<UpdateRequest> captor = ArgumentCaptor.forClass(UpdateRequest.class);
            processor.processCommit(commitUpdateCommand);
            verify(next).processCommit(commitUpdateCommand);
            verify(requestMirroringHandler, times(1)).mirror(captor.capture());
            UpdateRequest req = captor.getValue();
            assertNotNull(req.getParams());
            SolrParams params = req.getParams();
            assertEquals("true", params.get(UpdateParams.COMMIT));
            assertEquals("true", params.get(UpdateParams.OPTIMIZE));
            assertEquals("true", params.get(UpdateParams.SOFT_COMMIT));
            assertEquals("true", params.get(UpdateParams.PREPARE_COMMIT));
            assertEquals("true", params.get(UpdateParams.WAIT_SEARCHER));
            assertEquals("true", params.get(UpdateParams.OPEN_SEARCHER));
            assertEquals("true", params.get(UpdateParams.EXPUNGE_DELETES));
            assertEquals("10", params.get(UpdateParams.MAX_OPTIMIZE_SEGMENTS));
        } catch (Exception e) {
            fail("Exception should not be thrown: " + e);
        }
    }

    @Test
    public void processCommitNoMirroring() {
        try {
            when(cloudDesc.getCoreNodeName()).thenReturn("replica1");
            // don't override createMirrorRequest, call actual method
            processor =
                new MirroringUpdateProcessor(
                    next,
                    true,
                    true,
                    false,
                    CrossDcConf.ExpandDbq.EXPAND,
                    1000L,
                    new ModifiableSolrParams(),
                    DistributedUpdateProcessor.DistribPhase.NONE,
                    requestMirroringHandler,
                    producerMetrics);
            processor.processCommit(commitUpdateCommand);
            verify(next).processCommit(commitUpdateCommand);
            verify(requestMirroringHandler, times(0)).mirror(requestMock);
        } catch (Exception e) {
            fail("Exception should not be thrown: " + e);
        }
    }

    @Test
    public void testProcessAddWithinLimit() throws Exception {
        when(cloudDesc.getCoreNodeName()).thenReturn("replica1");
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "1");
        AddUpdateCommand cmd = new AddUpdateCommand(req);
        cmd.solrDoc = doc;
        cmd.commitWithin = 1000;
        cmd.overwrite = true;
        processor.processAdd(cmd);
        verify(next).processAdd(cmd);
        verify(requestMirroringHandler).mirror(requestMock);
    }

    @Test
    public void testProcessAddExceedsLimit() {
        AddUpdateCommand addUpdateCommand = new AddUpdateCommand(req);
        SolrInputDocument solrInputDocument = new SolrInputDocument();
        solrInputDocument.addField("id", "123");
        solrInputDocument.addField("large_field", "Test ".repeat(10000));
        addUpdateCommand.solrDoc = solrInputDocument;

        when(req.getCore()).thenReturn(core);
        when(req.getCore().getCoreDescriptor()).thenReturn(mock(CoreDescriptor.class));
        when(req.getCore().getCoreDescriptor().getCloudDescriptor()).thenReturn(mock(CloudDescriptor.class));
        when(req.getCore().getCoreContainer()).thenReturn(mock(CoreContainer.class));
        when(req.getCore().getCoreContainer().getZkController()).thenReturn(mock(ZkController.class));
        when(req.getCore().getCoreContainer().getZkController().getClusterState()).thenReturn(mock(ClusterState.class));

        SolrParams mirrorParams = new ModifiableSolrParams();
        MirroringUpdateProcessor mirroringUpdateProcessorWithLimit = new MirroringUpdateProcessor(nextProcessor, true, false, // indexUnmirrorableDocs set to false
                true, CrossDcConf.ExpandDbq.EXPAND, 50000, mirrorParams, DistributedUpdateProcessor.DistribPhase.NONE, requestMirroringHandler, producerMetrics);

        assertThrows(SolrException.class, () -> mirroringUpdateProcessorWithLimit.processAdd(addUpdateCommand));
    }

    @Test
    public void testProcessAddLeader() throws Exception {
        when(cloudDesc.getCoreNodeName()).thenReturn("replica1");
        processor.processAdd(addUpdateCommand);
        verify(requestMirroringHandler, times(1)).mirror(any());
    }

    @Test
    public void testProcessAddNotLeader() throws Exception {
        when(cloudDesc.getCoreNodeName()).thenReturn("replica2");
        processor.processAdd(addUpdateCommand);
        verify(requestMirroringHandler, times(0)).mirror(any());
    }

    @Test
    public void testProcessDelete() throws Exception {
        when(cloudDesc.getCoreNodeName()).thenReturn("replica1");
        processor.processDelete(deleteUpdateCommand);
        verify(requestMirroringHandler, times(1)).mirror(any());
    }

    @Test
    public void testExpandDbq() throws Exception {
        when(cloudDesc.getCoreNodeName()).thenReturn("replica1");
        deleteUpdateCommand.query = "id:test*";
        UpdateRequest updateRequest = new UpdateRequest();
        processor =
            new MirroringUpdateProcessor(
                next,
                true,
                true,
                true,
                CrossDcConf.ExpandDbq.NONE,
                1000L,
                new ModifiableSolrParams(),
                DistributedUpdateProcessor.DistribPhase.NONE,
                requestMirroringHandler,
                producerMetrics) {
                UpdateRequest createMirrorRequest() {
                    return updateRequest;
                }
            };

        processor.processDelete(deleteUpdateCommand);
        verify(requestMirroringHandler, times(1)).mirror(updateRequest);
        assertEquals("missing dbq", 1, updateRequest.getDeleteQuery().size());
        assertEquals("dbq value", "id:test*", updateRequest.getDeleteQuery().get(0));
    }

    @Test
    public void testProcessDBQResults() throws Exception {
        when(cloudDesc.getCoreNodeName()).thenReturn("replica1");
        when(builder.build()).thenReturn(client);
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "test");
        addUpdateCommand.solrDoc = doc;
        processor.processAdd(addUpdateCommand);

        SolrQuery query = new SolrQuery();
        query.setQuery("*:*");
        query.setRows(1000);
        query.setSort("id", SolrQuery.ORDER.asc);

        processor.processDelete(deleteUpdateCommand);
    }

    @Test
    public void testFinish() throws IOException {
        processor.finish();
    }
}