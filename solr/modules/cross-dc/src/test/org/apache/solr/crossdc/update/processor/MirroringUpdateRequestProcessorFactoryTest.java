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
package org.apache.solr.crossdc.update.processor;

import static org.apache.solr.crossdc.update.processor.MirroringUpdateRequestProcessorFactory.SERVER_SHOULD_MIRROR;
import static org.apache.solr.update.processor.DistributedUpdateProcessor.PARAM_WHITELIST_CTX_KEY;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.crossdc.common.KafkaCrossDcConf;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.junit.BeforeClass;
import org.junit.Test;

public class MirroringUpdateRequestProcessorFactoryTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  /**
   * getInstance() must whitelist SERVER_SHOULD_MIRROR on the request so that when the add/delete is
   * subsequently forwarded to other replicas/shards by DistributedUpdateProcessor, the param is
   * preserved instead of being stripped, which previously caused the receiving replica to re-decide
   * (and potentially re-mirror) independently. See SOLR-18409.
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testShouldMirrorParamWhitelists() {
    MirroringUpdateRequestProcessorFactory factory = new MirroringUpdateRequestProcessorFactory();
    factory.setMirroringHandler(mock(KafkaRequestMirroringHandler.class));
    factory.setKafkaCrossDcConf(new KafkaCrossDcConf(new HashMap<>()));

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(SERVER_SHOULD_MIRROR, "false");
    SolrQueryRequestBase req = new SolrQueryRequestBase(mock(SolrCore.class), params) {};
    SolrQueryResponse rsp = new SolrQueryResponse();
    UpdateRequestProcessor next = mock(UpdateRequestProcessor.class);

    // this should add the SERVER_SHOULD_MIRROR param to the request context whitelist
    factory.getInstance(req, rsp, next);

    Set<String> whitelist = (Set<String>) req.getContext().get(PARAM_WHITELIST_CTX_KEY);
    assertNotNull(
        "shouldMirror param must be added to the distributed request whitelist", whitelist);
    assertTrue(
        "shouldMirror param missing from distributed request whitelist",
        whitelist.contains(SERVER_SHOULD_MIRROR));
  }
}
