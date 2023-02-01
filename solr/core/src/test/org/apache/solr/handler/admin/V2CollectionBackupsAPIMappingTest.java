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
import static org.apache.solr.common.params.CommonParams.NAME;

import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.CollectionBackupsAPI;
import org.junit.Test;

public class V2CollectionBackupsAPIMappingTest extends V2ApiMappingTest<CollectionsHandler> {
  @Override
  public void populateApiBag() {
    final CollectionBackupsAPI collBackupsAPI = new CollectionBackupsAPI(getRequestHandler());
    apiBag.registerObject(collBackupsAPI);
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
  public void testDeleteBackupsAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/collections/backups",
            "POST",
            "{'delete-backups': {"
                + "'name': 'backupName', "
                + "'collection': 'collectionName', "
                + "'location': '/some/location/uri', "
                + "'repository': 'someRepository', "
                + "'backupId': 123, "
                + "'maxNumBackupPoints': 456, "
                + "'purgeUnused': true, "
                + "'async': 'requestTrackingId'"
                + "}}");

    assertEquals(CollectionParams.CollectionAction.DELETEBACKUP.lowerName, v1Params.get(ACTION));
    assertEquals("backupName", v1Params.get(NAME));
    assertEquals("/some/location/uri", v1Params.get(CoreAdminParams.BACKUP_LOCATION));
    assertEquals("someRepository", v1Params.get(CoreAdminParams.BACKUP_REPOSITORY));
    assertEquals(123, v1Params.getPrimitiveInt(CoreAdminParams.BACKUP_ID));
    assertEquals(456, v1Params.getPrimitiveInt(CoreAdminParams.MAX_NUM_BACKUP_POINTS));
    assertTrue(v1Params.getPrimitiveBool(CoreAdminParams.BACKUP_PURGE_UNUSED));
    assertEquals("requestTrackingId", v1Params.get(CommonAdminParams.ASYNC));
  }

  @Test
  public void testListBackupsAllParams() throws Exception {
    final SolrParams v1Params =
        captureConvertedV1Params(
            "/collections/backups",
            "POST",
            "{'list-backups': {"
                + "'name': 'backupName', "
                + "'location': '/some/location/uri', "
                + "'repository': 'someRepository' "
                + "}}");

    assertEquals(CollectionParams.CollectionAction.LISTBACKUP.lowerName, v1Params.get(ACTION));
    assertEquals("backupName", v1Params.get(NAME));
    assertEquals("/some/location/uri", v1Params.get(CoreAdminParams.BACKUP_LOCATION));
    assertEquals("someRepository", v1Params.get(CoreAdminParams.BACKUP_REPOSITORY));
  }
}
