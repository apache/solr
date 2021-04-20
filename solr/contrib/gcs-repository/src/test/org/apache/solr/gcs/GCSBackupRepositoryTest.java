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

import org.apache.solr.cloud.api.collections.AbstractBackupRepositoryTest;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.repository.BackupRepository;

public class GCSBackupRepositoryTest extends AbstractBackupRepositoryTest {
    @Override
    @SuppressWarnings("rawtypes")
    protected BackupRepository getRepository() {
        GCSBackupRepository repository = new GCSBackupRepository();
        repository.init(new NamedList());
        return repository;
    }

    @Override
    protected URI getBaseUri() throws URISyntaxException {
        return new URI("/tmp");
    }
}
