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

import java.net.URI;
import java.net.URISyntaxException;

import com.google.cloud.storage.Storage;
import org.apache.solr.cloud.api.collections.AbstractBackupRepositoryTest;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.junit.AfterClass;

/**
 * Unit tests for {@link GCSBackupRepository} that use an in-memory Storage object
 */
public class GCSBackupRepositoryTest extends AbstractBackupRepositoryTest {

    @AfterClass
    public static void tearDownClass() throws Exception {
        LocalStorageGCSBackupRepository.clearStashedStorage();
    }

    @Override
    @SuppressWarnings("rawtypes")
    protected BackupRepository getRepository() {
        final NamedList<Object> config = new NamedList<>();
        config.add(CoreAdminParams.BACKUP_LOCATION, "backup1");
        final GCSBackupRepository repository = new LocalStorageGCSBackupRepository();
        repository.init(config);

        return repository;
    }

    @Override
    protected URI getBaseUri() throws URISyntaxException {
        return new URI("tmp");
    }
}
