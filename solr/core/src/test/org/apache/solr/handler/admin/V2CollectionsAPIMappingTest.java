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
package org.apache.solr.handler.admin;

import static org.apache.solr.common.params.CommonParams.ACTION;

import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.backup.BackupManager;
import org.apache.solr.handler.admin.api.RestoreCollectionAPI;
import org.junit.Test;

/**
 * Unit tests for the "create collection", "create alias" and "restore collection" v2 APIs.
 *
 * <p>This test bears many similarities to {@link TestCollectionAPIs} which appears to test the
 * mappings indirectly by checking message sent to the ZK overseer (which is similar, but not
 * identical to the v1 param list). If there's no particular benefit to testing the mappings in this
 * way (there very well may be), then we should combine these two test classes at some point in the
 * future using the simpler approach here.
 *
 * <p>Note that the V2 requests made by these tests are not necessarily semantically valid. They
 * shouldn't be taken as examples. In several instances, mutually exclusive JSON parameters are
 * provided. This is done to exercise conversion of all parameters, even if particular combinations
 * are never expected in the same request.
 */
public class V2CollectionsAPIMappingTest extends V2ApiMappingTest<CollectionsHandler> {

  @Override
  public void populateApiBag() {
    apiBag.registerObject(new RestoreCollectionAPI(getRequestHandler()));
  }

  @Override
  public CollectionsHandler createUnderlyingRequestHandler() {
    return createMock(CollectionsHandler.class);
  }

  @Override
  public boolean isCoreSpecific() {
    return false;
  }

  @Test
  public void testRestoreAllProperties() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/backups/backupName",
            "POST",
            "{'restore-collection': {"
                + "'collection': 'collectionName', "
                + "'location': '/some/location/uri', "
                + "'repository': 'someRepository', "
                + "'backupId': 123, "
                + "'async': 'requestTrackingId', "
                + "'create-collection': {"
                + "     'numShards': 1, "
                + "     'properties': {'foo': 'bar', 'foo2': 'bar2'}, "
                + "     'replicationFactor': 3 "
                + "}"
                + "}}");

    assertEquals(CollectionParams.CollectionAction.RESTORE.lowerName, v1Params.get(ACTION));
    assertEquals("backupName", v1Params.get(CommonParams.NAME));
    assertEquals("collectionName", v1Params.get(BackupManager.COLLECTION_NAME_PROP));
    assertEquals("/some/location/uri", v1Params.get(CoreAdminParams.BACKUP_LOCATION));
    assertEquals("someRepository", v1Params.get(CoreAdminParams.BACKUP_REPOSITORY));
    assertEquals(123, v1Params.getPrimitiveInt(CoreAdminParams.BACKUP_ID));
    assertEquals("requestTrackingId", v1Params.get(CommonAdminParams.ASYNC));
    // NOTE: Unlike other v2 APIs that have a nested object for collection-creation params,
    // the restore API's v1 equivalent for these properties doesn't have a "create-collection."
    // prefix.
    assertEquals(1, v1Params.getPrimitiveInt(CollectionAdminParams.NUM_SHARDS));
    assertEquals("bar", v1Params.get("property.foo"));
    assertEquals("bar2", v1Params.get("property.foo2"));
    assertEquals(3, v1Params.getPrimitiveInt(ZkStateReader.REPLICATION_FACTOR));
  }
}
