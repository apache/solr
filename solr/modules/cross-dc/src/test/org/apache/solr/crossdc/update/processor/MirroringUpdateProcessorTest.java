package org.apache.solr.crossdc.update.processor;

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
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Map;

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
    private HttpSolrClient.Builder builder = Mockito.mock(HttpSolrClient.Builder.class);
    private HttpSolrClient client = Mockito.mock(HttpSolrClient.class);
    private CloudDescriptor cloudDesc;
    private ZkStateReader zkStateReader;
    private Replica replica;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        req = Mockito.mock(SolrQueryRequestBase.class);
        Mockito.when(req.getParams()).thenReturn(new ModifiableSolrParams());

        addUpdateCommand = new AddUpdateCommand(req);
        addUpdateCommand.solrDoc = new SolrInputDocument();
        addUpdateCommand.solrDoc.addField("id", "test");

        requestMock = Mockito.mock(UpdateRequest.class);

        nextProcessor = Mockito.mock(UpdateRequestProcessor.class);

        IndexSchema schema = Mockito.mock(IndexSchema.class);
        Mockito.when(req.getSchema()).thenReturn(schema);

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

        next = Mockito.mock(UpdateRequestProcessor.class);
        requestMirroringHandler = Mockito.mock(RequestMirroringHandler.class);
        processor =
                new MirroringUpdateProcessor(
                        next,
                        true,
                        true,
                        true,
                        1000L,
                        new ModifiableSolrParams(),
                        DistributedUpdateProcessor.DistribPhase.NONE,
                        requestMirroringHandler) {
                    UpdateRequest createMirrorRequest() {
                        return requestMock;
                    }
                };

        core = Mockito.mock(SolrCore.class);
        CoreDescriptor coreDesc = Mockito.mock(CoreDescriptor.class);
        cloudDesc = Mockito.mock(CloudDescriptor.class);
        Mockito.when(cloudDesc.getShardId()).thenReturn("shard1");
        CoreContainer coreContainer = Mockito.mock(CoreContainer.class);
        ZkController zkController = Mockito.mock(ZkController.class);
        ClusterState clusterState = Mockito.mock(ClusterState.class);
        DocCollection docCollection = Mockito.mock(DocCollection.class);
        DocRouter docRouter = Mockito.mock(DocRouter.class);
        Slice slice = Mockito.mock(Slice.class);
        Mockito.when(slice.getName()).thenReturn("shard1");
        zkStateReader = Mockito.mock(ZkStateReader.class);
        replica = Mockito.mock(Replica.class);

        Mockito.when(replica.getName()).thenReturn("replica1");
        Mockito.when(zkStateReader.getLeaderRetry(Mockito.any(), Mockito.any()))
                .thenReturn(replica);
        Mockito.when(zkController.getZkStateReader()).thenReturn(zkStateReader);
        Mockito.when(coreDesc.getCloudDescriptor()).thenReturn(cloudDesc);
        Mockito.when(clusterState.getCollection(Mockito.any())).thenReturn(docCollection);
        Mockito.when(docCollection.getRouter()).thenReturn(docRouter);
        Mockito.when(
                        docRouter.getTargetSlice(
                                Mockito.any(),
                                Mockito.any(),
                                Mockito.any(),
                                Mockito.any(),
                                Mockito.any()))
                .thenReturn(slice);
        Mockito.when(docCollection.getSlicesMap()).thenReturn(Map.of("shard1", slice));
        Mockito.when(zkController.getClusterState()).thenReturn(clusterState);
        Mockito.when(coreContainer.getZkController()).thenReturn(zkController);
        Mockito.when(core.getCoreContainer()).thenReturn(coreContainer);
        Mockito.when(core.getCoreDescriptor()).thenReturn(coreDesc);
        Mockito.when(req.getCore()).thenReturn(core);
    }

    /**
     * Should process delete command and mirror the document when the distribPhase is NONE and
     * deleteById is false
     */
    @Test
    public void processDeleteWhenDistribPhaseIsNoneAndDeleteByIdIsFalse() {
        try {
            processor.processDelete(deleteUpdateCommand);
            Mockito.verify(requestMirroringHandler, Mockito.times(1)).mirror(requestMock);
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
            Mockito.when(cloudDesc.getCoreNodeName()).thenReturn("replica1");
            processor.processAdd(addUpdateCommand);
            Mockito.verify(requestMirroringHandler, Mockito.times(1)).mirror(requestMock);
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
            Mockito.when(cloudDesc.getCoreNodeName()).thenReturn("replica1");
            deleteUpdateCommand.setId("test");
            processor.processDelete(deleteUpdateCommand);
            Mockito.verify(requestMirroringHandler, Mockito.times(1)).mirror(requestMock);
        } catch (Exception e) {
            fail("IOException should not be thrown");
        }
    }

    @Test
    public void processCommitOnlyAnotherShard() {
        try {
            // should skip if processing in other shard than the first
            Mockito.when(cloudDesc.getShardId()).thenReturn("shard2");
            processor.processCommit(commitUpdateCommand);
            Mockito.verify(next).processCommit(commitUpdateCommand);
            Mockito.verify(requestMirroringHandler, Mockito.times(0)).mirror(requestMock);
        } catch (Exception e) {
            fail("IOException should not be thrown: " + e);
        }
    }

    @Test
    public void processCommitOnlyNonLeader() {
        try {
            // should skip if processing on non-leader replica
            Mockito.when(replica.getName()).thenReturn("foobar");
            Mockito.when(cloudDesc.getCoreNodeName()).thenReturn("replica1");
            processor.processCommit(commitUpdateCommand);
            Mockito.verify(next).processCommit(commitUpdateCommand);
            Mockito.verify(requestMirroringHandler, Mockito.times(0)).mirror(requestMock);
        } catch (Exception e) {
            fail("IOException should not be thrown: " + e);
        }
    }

    @Test
    public void processCommitOnlyLeader() {
        try {
            Mockito.when(cloudDesc.getCoreNodeName()).thenReturn("replica1");
            // don't override createMirrorRequest, call actual method
            processor =
                new MirroringUpdateProcessor(
                    next,
                    true,
                    true,
                    true,
                    1000L,
                    new ModifiableSolrParams(),
                    DistributedUpdateProcessor.DistribPhase.NONE,
                    requestMirroringHandler);
            ArgumentCaptor<UpdateRequest> captor = ArgumentCaptor.forClass(UpdateRequest.class);
            processor.processCommit(commitUpdateCommand);
            Mockito.verify(next).processCommit(commitUpdateCommand);
            Mockito.verify(requestMirroringHandler, Mockito.times(1)).mirror(captor.capture());
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
            Mockito.when(cloudDesc.getCoreNodeName()).thenReturn("replica1");
            // don't override createMirrorRequest, call actual method
            processor =
                new MirroringUpdateProcessor(
                    next,
                    true,
                    true,
                    false,
                    1000L,
                    new ModifiableSolrParams(),
                    DistributedUpdateProcessor.DistribPhase.NONE,
                    requestMirroringHandler);
            processor.processCommit(commitUpdateCommand);
            Mockito.verify(next).processCommit(commitUpdateCommand);
            Mockito.verify(requestMirroringHandler, Mockito.times(0)).mirror(requestMock);
        } catch (Exception e) {
            fail("Exception should not be thrown: " + e);
        }
    }

    @Test
    public void testProcessAddWithinLimit() throws Exception {
        Mockito.when(cloudDesc.getCoreNodeName()).thenReturn("replica1");
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "1");
        AddUpdateCommand cmd = new AddUpdateCommand(req);
        cmd.solrDoc = doc;
        cmd.commitWithin = 1000;
        cmd.overwrite = true;
        processor.processAdd(cmd);
        Mockito.verify(next).processAdd(cmd);
        Mockito.verify(requestMirroringHandler).mirror(requestMock);
    }

    @Test
    public void testProcessAddExceedsLimit() {
        AddUpdateCommand addUpdateCommand = new AddUpdateCommand(req);
        SolrInputDocument solrInputDocument = new SolrInputDocument();
        solrInputDocument.addField("id", "123");
        solrInputDocument.addField("large_field", "Test ".repeat(10000));
        addUpdateCommand.solrDoc = solrInputDocument;

        Mockito.when(req.getCore()).thenReturn(core);
        Mockito.when(req.getCore().getCoreDescriptor()).thenReturn(Mockito.mock(CoreDescriptor.class));
        Mockito.when(req.getCore().getCoreDescriptor().getCloudDescriptor()).thenReturn(Mockito.mock(CloudDescriptor.class));
        Mockito.when(req.getCore().getCoreContainer()).thenReturn(Mockito.mock(CoreContainer.class));
        Mockito.when(req.getCore().getCoreContainer().getZkController()).thenReturn(Mockito.mock(ZkController.class));
        Mockito.when(req.getCore().getCoreContainer().getZkController().getClusterState()).thenReturn(Mockito.mock(ClusterState.class));

        SolrParams mirrorParams = new ModifiableSolrParams();
        MirroringUpdateProcessor mirroringUpdateProcessorWithLimit = new MirroringUpdateProcessor(nextProcessor, true, false, // indexUnmirrorableDocs set to false
                true, 50000, mirrorParams, DistributedUpdateProcessor.DistribPhase.NONE, requestMirroringHandler);

        assertThrows(SolrException.class, () -> mirroringUpdateProcessorWithLimit.processAdd(addUpdateCommand));
    }

    @Test
    public void testProcessAddLeader() throws Exception {
        Mockito.when(cloudDesc.getCoreNodeName()).thenReturn("replica1");
        processor.processAdd(addUpdateCommand);
        Mockito.verify(requestMirroringHandler, Mockito.times(1)).mirror(Mockito.any());
    }

    @Test
    public void testProcessAddNotLeader() throws Exception {
        Mockito.when(cloudDesc.getCoreNodeName()).thenReturn("replica2");
        processor.processAdd(addUpdateCommand);
        Mockito.verify(requestMirroringHandler, Mockito.times(0)).mirror(Mockito.any());
    }

    @Test
    public void testProcessDelete() throws Exception {
        Mockito.when(cloudDesc.getCoreNodeName()).thenReturn("replica1");
        processor.processDelete(deleteUpdateCommand);
        Mockito.verify(requestMirroringHandler, Mockito.times(1)).mirror(Mockito.any());
    }

    @Test
    public void testProcessDBQResults() throws Exception {
        Mockito.when(cloudDesc.getCoreNodeName()).thenReturn("replica1");
        Mockito.when(builder.build()).thenReturn(client);
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