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

package org.apache.solr.shell;

import org.junit.BeforeClass;
import org.junit.Test;

/** Port of {@code test_delete_collection.bats}. */
public class DeleteCollectionTest extends SolrShellTestCase {

  @BeforeClass
  public static void startSolr() {
    // The node lives for the whole class and is stopped automatically.
    classCli.run("solr start");
  }

  @Test
  public void canDeleteCollections() {
    run("solr create -c COLL_DEFAULT");
    assertTrue(collectionExists("COLL_DEFAULT"));

    run("solr delete -c COLL_DEFAULT");
    assertFalse(collectionExists("COLL_DEFAULT"));
    assertTrue(configExists("COLL_DEFAULT"));
  }

  @Test
  public void canDeleteCollectionsWithSolrUrl() {
    run("solr create -c COLL_SOLR_URL");
    assertTrue(collectionExists("COLL_SOLR_URL"));

    run("solr delete -c COLL_SOLR_URL --solr-url http://localhost:${SOLR_PORT}");
    assertFalse(collectionExists("COLL_SOLR_URL"));
    assertTrue(configExists("COLL_SOLR_URL"));
  }

  @Test
  public void canDeleteCollectionsWithSolrConnection() {
    run("solr create -c COLL_SOLR_CONNECTION");
    assertTrue(collectionExists("COLL_SOLR_CONNECTION"));

    run("solr delete -c COLL_SOLR_CONNECTION --solr-connection http://localhost:${SOLR_PORT}/solr");
    assertFalse(collectionExists("COLL_SOLR_CONNECTION"));
    assertTrue(configExists("COLL_SOLR_CONNECTION"));
  }

  @Test
  public void collectionDeleteAlsoDeletesZkConfig() {
    run("solr create -c COLL_DELETE_CONFIG");
    assertTrue(configExists("COLL_DELETE_CONFIG"));

    run("solr delete -c COLL_DELETE_CONFIG --delete-config");
    assertFalse(configExists("COLL_DELETE_CONFIG"));
  }

  @Test
  public void deletesAccompanyingZkConfigWithNondefaultName() {
    run("solr create -c COLL_NONDEFAULT -n NONDEFAULT_CONFIG_NAME");
    assertTrue(configExists("NONDEFAULT_CONFIG_NAME"));

    run("solr delete -c COLL_NONDEFAULT --delete-config");
    assertFalse(configExists("NONDEFAULT_CONFIG_NAME"));
  }

  @Test
  public void deleteConfigOptionCanOptToLeaveConfigInZk() {
    run("solr create -c COLL_KEEP_CONFIG");
    assertTrue(configExists("COLL_KEEP_CONFIG"));

    run("solr delete -c COLL_KEEP_CONFIG");
    assertTrue(configExists("COLL_KEEP_CONFIG"));
  }
}
