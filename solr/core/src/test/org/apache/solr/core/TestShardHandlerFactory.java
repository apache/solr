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
package org.apache.solr.core;

import java.nio.file.Path;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;

/** Tests specifying a custom ShardHandlerFactory */
public class TestShardHandlerFactory extends SolrTestCaseJ4 {

  public void testXML() {
    Path home = TEST_PATH();
    CoreContainer cc = CoreContainer.createAndLoad(home, home.resolve("solr-shardhandler.xml"));
    ShardHandlerFactory factory = cc.getShardHandlerFactory();
    assertTrue(factory instanceof MockShardHandlerFactory);
    NamedList<?> args = ((MockShardHandlerFactory) factory).args;
    assertEquals("myMagicRequiredValue", args.get("myMagicRequiredParameter"));
    factory.close();
    cc.shutdown();
  }

  /** Test {@link ShardHandler#setShardAttributesToParams} */
  public void testSetShardAttributesToParams() {
    // NOTE: the value of this test is really questionable; we should feel free to remove it
    ModifiableSolrParams modifiable = new ModifiableSolrParams();
    var dummyIndent = "Dummy-Indent";

    modifiable.set(ShardParams.SHARDS, "dummyValue");
    modifiable.set(CommonParams.HEADER_ECHO_PARAMS, "dummyValue");
    modifiable.set(CommonParams.INDENT, dummyIndent);

    ShardHandler.setShardAttributesToParams(modifiable, 2);

    assertEquals(Boolean.FALSE.toString(), modifiable.get(CommonParams.DISTRIB));
    assertEquals("2", modifiable.get(ShardParams.SHARDS_PURPOSE));
    assertEquals(Boolean.FALSE.toString(), modifiable.get(CommonParams.OMIT_HEADER));
    assertEquals(Boolean.TRUE.toString(), modifiable.get(ShardParams.IS_SHARD));

    assertNull(modifiable.get(CommonParams.HEADER_ECHO_PARAMS));
    assertNull(modifiable.get(ShardParams.SHARDS));
    assertNull(modifiable.get(CommonParams.INDENT));
  }
}
