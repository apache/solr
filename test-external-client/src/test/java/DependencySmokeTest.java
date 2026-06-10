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

import java.io.IOException;
import java.nio.file.Path;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.apache.solr.util.SolrClientTestRule;
import org.junit.ClassRule;
import org.junit.Test;

public class DependencySmokeTest extends SolrTestCase {

  @ClassRule
  public static SolrClientTestRule solrRule = new EmbeddedSolrServerTestRule();

  @Test
  public void testSolrjAndTestFrameworkAreUsable() throws SolrServerException, IOException {
    solrRule.startSolr();
    // TODO solr-test-framework.jar or maybe solr-core.jar ought to include the _default configSet
    //  and maybe a cloud-minimal configset.  Once it does, then let's see if we can enhance
    //  withConfigSet to handle JAR Paths, and add a static getFile perhaps to make it easy.
    Path configSet = Path.of("../solr/server/solr/configsets/_default/").toAbsolutePath();
    solrRule.newCollection().withConfigSet(configSet).create();
    solrRule.getSolrClient().ping();
  }
}
