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

package org.apache.solr.gcs;

import static org.apache.solr.common.params.CoreAdminParams.BACKUP_LOCATION;
import static org.apache.solr.gcs.GCSConfigParser.GCS_BUCKET_ENV_VAR_NAME;
import static org.apache.solr.gcs.GCSConfigParser.GCS_CREDENTIAL_ENV_VAR_NAME;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.cloud.api.collections.AbstractBackupRepositoryTest;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.junit.AfterClass;
import org.junit.Test;

/** Unit tests for {@link GCSBackupRepository} that use an in-memory Storage object */
public class GCSBackupRepositoryTest extends AbstractBackupRepositoryTest {

  @AfterClass
  public static void tearDownClass() throws Exception {
    LocalStorageGCSBackupRepository.clearStashedStorage();
  }

  @Override
  protected BackupRepository getRepository() {
    final NamedList<Object> config = new NamedList<>();
    config.add(BACKUP_LOCATION, "backup1");
    final GCSBackupRepository repository = new LocalStorageGCSBackupRepository();
    repository.init(config);

    return repository;
  }

  @Override
  protected URI getBaseUri() throws URISyntaxException {
    return new URI("tmp");
  }

  @Test
  public void testInitStoreDoesNotFailWithMissingCredentials() {
    Map<String, String> config = new HashMap<>();
    config.put(GCS_BUCKET_ENV_VAR_NAME, "a_bucket_name");
    // explicitly setting credential name to null; will work inside google-cloud project
    config.put(GCS_CREDENTIAL_ENV_VAR_NAME, null);
    config.put(BACKUP_LOCATION, "/==");

    BackupRepository gcsBackupRepository = getRepository();

    gcsBackupRepository.init(new NamedList<>(config));
  }
}
