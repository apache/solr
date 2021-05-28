package org.apache.solr.handler.admin;

import com.google.common.collect.Maps;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.core.backup.BackupManager;
import org.apache.solr.handler.CollectionsAPI;
import org.apache.solr.handler.SpecificCollectionAPI;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for the API mappings found in {@link org.apache.solr.handler.SpecificCollectionAPI}.
 *
 * This test bears many similarities to {@link TestCollectionAPIs} which appears to test the mappings indirectly by
 * checking message sent to the ZK overseer (which is similar, but not identical to the v1 param list).  If there's no
 * particular benefit to testing the mappings in this way (there very well may be), then we should combine these two
 * test classes at some point in the future using the simpler approach here.
 *
 * Note that the V2 requests made by these tests are not necessarily semantically valid.  They shouldn't be taken as
 * examples. In several instances, mutually exclusive JSON parameters are provided.  This is done to exercise conversion
 * of all parameters, even if particular combinations are never expected in the same request.
 */
public class V2SpecificCollectionAPIMappingTest extends SolrTestCaseJ4 {
  private ApiBag apiBag;

  private ArgumentCaptor<SolrQueryRequest> queryRequestCaptor;
  private CollectionsHandler mockCollectionsHandler;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setupApiBag() throws Exception {
    mockCollectionsHandler = mock(CollectionsHandler.class);
    queryRequestCaptor = ArgumentCaptor.forClass(SolrQueryRequest.class);

    apiBag = new ApiBag(false);
    final SpecificCollectionAPI specificCollectionAPI = new SpecificCollectionAPI(mockCollectionsHandler);
    apiBag.registerObject(specificCollectionAPI);
    apiBag.registerObject(specificCollectionAPI.specificCollectionCommands);
  }

  @Test
  public void testModifyCollectionAllProperties() throws Exception {
    final SolrParams v1Params = captureConvertedV1Params("/collections/collName", "POST",
            "{ 'modify': {" +
                    "'replicationFactor': 123, " +
                    "'readOnly': true, " +
                    "'config': 'techproducts_config', " +
                    "'async': 'requestTrackingId', " +
                    "'properties': {" +
                    "     'foo': 'bar', " +
                    "     'baz': 456 " +
                    "}" +
                    "}}");

    assertEquals(CollectionParams.CollectionAction.MODIFYCOLLECTION.lowerName, v1Params.get(ACTION));
    assertEquals("collName", v1Params.get(COLLECTION));
    assertEquals(123, v1Params.getPrimitiveInt(ZkStateReader.REPLICATION_FACTOR));
    assertEquals(true, v1Params.getPrimitiveBool(ZkStateReader.READ_ONLY));
    assertEquals("techproducts_config", v1Params.get(COLL_CONF));
    assertEquals("requestTrackingId", v1Params.get(ASYNC));
    assertEquals("bar", v1Params.get("property.foo"));
    assertEquals(456, v1Params.getPrimitiveInt("property.baz"));
  }

  @Test
  public void testReloadCollectionAllProperties() throws Exception {
    final SolrParams v1Params = captureConvertedV1Params("/collections/collName", "POST",
            "{ 'reload': {'async': 'requestTrackingId'}}");

    assertEquals(CollectionParams.CollectionAction.RELOAD.lowerName, v1Params.get(ACTION));
    assertEquals("collName", v1Params.get(COLLECTION));
    assertEquals("requestTrackingId", v1Params.get(ASYNC));
  }

  @Test
  public void testMoveReplicaAllProperties() throws Exception {
    final SolrParams v1Params = captureConvertedV1Params("/collections/collName", "POST",
            "{ 'move-replica': {" +
                    "'sourceNode': 'someSourceNode', " +
                    "'targetNode': 'someTargetNode', " +
                    "'replica': 'someReplica', " +
                    "'shard': 'someShard', " +
                    "'waitForFinalState': true, " +
                    "'timeout': 123, " +
                    "'inPlaceMove': true, " +
                    "'followAliases': true " +
                    "}}");

    assertEquals(CollectionParams.CollectionAction.MOVEREPLICA.lowerName, v1Params.get(ACTION));
    assertEquals("collName", v1Params.get(COLLECTION));
    assertEquals("someSourceNode", v1Params.get("sourceNode"));
    assertEquals("someTargetNode", v1Params.get("targetNode"));
    assertEquals("someReplica", v1Params.get("replica"));
    assertEquals("someShard", v1Params.get("shard"));
    assertEquals(true, v1Params.getPrimitiveBool("waitForFinalState"));
    assertEquals(123, v1Params.getPrimitiveInt("timeout"));
    assertEquals(true, v1Params.getPrimitiveBool("inPlaceMove"));
    assertEquals(true, v1Params.getPrimitiveBool("followAliases"));
  }

  @Test
  public void testMigrateDocsAllProperties() throws Exception {
    final SolrParams v1Params = captureConvertedV1Params("/collections/collName", "POST",
            "{ 'migrate-docs': {" +
                    "'target': 'someTargetCollection', " +
                    "'splitKey': 'someSplitKey', " +
                    "'forwardTimeout': 123, " +
                    "'followAliases': true, " +
                    "'async': 'requestTrackingId' " +
                    "}}");

    assertEquals(CollectionParams.CollectionAction.MIGRATE.lowerName, v1Params.get(ACTION));
    assertEquals("collName", v1Params.get(COLLECTION));
    assertEquals("someTargetCollection", v1Params.get("target.collection"));
    assertEquals("someSplitKey", v1Params.get("split.key"));
    assertEquals(123, v1Params.getPrimitiveInt("forward.timeout"));
    assertEquals(true, v1Params.getPrimitiveBool("followAliases"));
    assertEquals("requestTrackingId", v1Params.get(ASYNC));
  }

  @Test
  public void testBalanceShardUniqueAllProperties() throws Exception {
    final SolrParams v1Params = captureConvertedV1Params("/collections/collName", "POST",
            "{ 'balance-shard-unique': {" +
                    "'property': 'somePropertyToBalance', " +
                    "'onlyactivenodes': false, " +
                    "'shardUnique': true" +
                    "}}");

    assertEquals(CollectionParams.CollectionAction.BALANCESHARDUNIQUE.lowerName, v1Params.get(ACTION));
    assertEquals("collName", v1Params.get(COLLECTION));
    assertEquals("somePropertyToBalance", v1Params.get("property"));
    assertEquals(false, v1Params.getPrimitiveBool("onlyactivenodes"));
    assertEquals(true, v1Params.getPrimitiveBool("shardUnique"));
  }

  @Test
  public void testRebalanceLeadersAllProperties() throws Exception {
    final SolrParams v1Params = captureConvertedV1Params("/collections/collName", "POST",
            "{ 'rebalance-leaders': {" +
                    "'maxAtOnce': 123, " +
                    "'maxWaitSeconds': 456" +
                    "}}");

    assertEquals(CollectionParams.CollectionAction.REBALANCELEADERS.lowerName, v1Params.get(ACTION));
    assertEquals("collName", v1Params.get(COLLECTION));
    assertEquals(123, v1Params.getPrimitiveInt("maxAtOnce"));
    assertEquals(456, v1Params.getPrimitiveInt("maxWaitSeconds"));
  }

  // TODO There's no hurdle on these last few tests.  Just ran out of time before going afk.

  @Test
  public void testAddReplicaPropertyAllProperties() throws Exception {

  }

  @Test
  public void testDeleteReplicaPropertyAllProperties() throws Exception {

  }

  @Test
  public void testSetCollectionPropertyAllProperties() throws Exception {

  }

  private SolrParams captureConvertedV1Params(String path, String method, String v2RequestBody) throws Exception {
    final HashMap<String, String> parts = new HashMap<>();
    final Api api = apiBag.lookup(path, method, parts);
    final SolrQueryResponse rsp = new SolrQueryResponse();
    final LocalSolrQueryRequest req = new LocalSolrQueryRequest(null, Maps.newHashMap()) {
      @Override
      public List<CommandOperation> getCommands(boolean validateInput) {
        if (v2RequestBody == null) return Collections.emptyList();
        return ApiBag.getCommandOperations(new ContentStreamBase.StringStream(v2RequestBody), api.getCommandSchema(), true);
      }

      @Override
      public Map<String, String> getPathTemplateValues() {
        return parts;
      }

      @Override
      public String getHttpMethod() {
        return method;
      }
    };


    api.call(req, rsp);
    verify(mockCollectionsHandler).handleRequestBody(queryRequestCaptor.capture(), any());
    return queryRequestCaptor.getValue().getParams();
  }
}
