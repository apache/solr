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
package org.apache.solr.core.backup.repository;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.solr.cloud.api.collections.AbstractBackupRepositoryTest;
import org.junit.BeforeClass;

/** {@link LocalFileSystemRepository} test. */
public class LocalFileSystemRepositoryTest extends AbstractBackupRepositoryTest {

  private static URI baseUri;

  @BeforeClass
  public static void setupBaseDir() {
    baseUri = createTempDir().toUri();
  }

  @Override
  protected Class<? extends BackupRepository> getRepositoryClass() {
    return LocalFileSystemRepository.class;
  }

  @Override
  protected BackupRepository getRepository() {
    LocalFileSystemRepository repo = new LocalFileSystemRepository();
    repo.init(getBaseBackupRepositoryConfiguration());
    return repo;
  }

  @Override
  protected URI getBaseUri() throws URISyntaxException {
    return baseUri;
  }
}
