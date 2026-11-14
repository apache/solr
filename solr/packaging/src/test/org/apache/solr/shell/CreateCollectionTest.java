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

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Port of {@code test_create_collection.bats}.
 *
 * <p>The invalid-config-dir path is matched with either separator: the tool echoes it back through
 * {@code Path}, which normalizes it on Windows.
 */
public class CreateCollectionTest extends SolrCatsTestCase {

  @BeforeClass
  public static void startSolr() {
    // The node lives for the whole class and is stopped automatically.
    classCli.run("solr start");
  }

  @Test
  public void createCollection() {
    run("solr create -c coll_default")
        .assertContains("Created collection 'coll_default'")
        .assertContains("assuming solr url is http://localhost:${SOLR_PORT}");
  }

  @Test
  public void createCollectionUsingSolrUrl() {
    run("solr create -c coll_solr_url --solr-url http://localhost:${SOLR_PORT}")
        .assertContains("Created collection 'coll_solr_url'")
        .assertNotContains("assuming solr url is http://localhost:${SOLR_PORT}");
  }

  @Test
  public void createCollectionUsingLegacySolrUrl() {
    run("solr create -c coll_legacy_solr_url --solr-url http://localhost:${SOLR_PORT}/solr")
        .assertContains("Created collection 'coll_legacy_solr_url'")
        .assertContains("needn't include Solr's context-root")
        .assertNotContains("assuming solr url is http://localhost:${SOLR_PORT}");
  }

  @Test
  public void createCollectionUsingZookeeper() {
    run("solr create -c coll_zk -z localhost:${ZK_PORT}")
        .assertContains("Created collection 'coll_zk'");
  }

  @Test
  public void rejectDOptionWithInvalidConfigDir() {
    runFail(
            "solr create -c coll_invalid_config_dir -d /asdf"
                + "  --solr-url http://localhost:${SOLR_PORT}")
        .assertMatches("Specified configuration directory [/\\\\]asdf not found!");
  }

  @Test
  public void acceptDOptionWithBuiltinConfig() {
    run("solr create -c coll_builtin_config -d sample_techproducts_configs"
            + " --solr-url http://localhost:${SOLR_PORT}")
        .assertContains("Created collection 'coll_builtin_config'");
  }

  @Test
  public void acceptDOptionWithExplicitPathToConfig() {
    Path sourceConfigsetDir =
        harnessEnv.solrTip().resolve("server/solr/configsets/sample_techproducts_configs");
    assertTrue(Files.isDirectory(sourceConfigsetDir));
    Path destConfigsetDir = copy(sourceConfigsetDir.toString(), "config");

    run("solr create -c coll_explicit_config_path -d \""
            + destConfigsetDir
            + "\" --solr-url http://localhost:${SOLR_PORT}")
        .assertContains("Created collection 'coll_explicit_config_path'");
  }

  @Test
  public void acceptNOptionAsConfigName() {
    run("solr create -c coll_conf_name -n other_conf_name"
            + " --solr-url http://localhost:${SOLR_PORT}")
        .assertContains("Created collection 'coll_conf_name'")
        .assertContains("config-set 'other_conf_name'");
  }

  @Test
  public void allowConfigReuseWhenNOptionSpecifiesSameConfig() {
    run("solr create -c coll_shared_config_1 -n shared_config"
            + " --solr-url http://localhost:${SOLR_PORT}")
        .assertContains("Created collection 'coll_shared_config_1'")
        .assertContains("config-set 'shared_config'");

    run("solr create -c coll_shared_config_2 -n shared_config"
            + " --solr-url http://localhost:${SOLR_PORT}")
        .assertContains("Created collection 'coll_shared_config_2'")
        .assertContains("config-set 'shared_config'");
  }

  @Test
  public void createMultishardedCollectionsWhenSProvided() {
    run("solr create -c coll_multishard --shards 2 --solr-url http://localhost:${SOLR_PORT}")
        .assertContains("Created collection 'coll_multishard'")
        .assertContains("2 shard(s)");
  }

  @Test
  public void createReplicatedCollectionsWhenRfProvided() {
    run("solr create -c coll_replicated -rf 2 --solr-url http://localhost:${SOLR_PORT}")
        .assertContains("Created collection 'coll_replicated'")
        .assertContains("2 replica(s)");
  }
}
