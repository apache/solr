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
package org.apache.solr.handler.component;

import java.io.IOException;

/**
 * This class is a test component to help test the forced distributed functionality of the
 * SearchComponent.
 */
public class ForcedDistributedComponent extends SearchComponent {

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    rb.addDebugInfo("prepare()", "ForceDistributedComponent.prepare()");
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    rb.rsp.addResponse("ForceDistributedComponent.process()");
  }

  @Override
  protected boolean isForceDistributed() {
    return true;
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    if (rb.getDebugInfo().get("process()") == null) {
      rb.addDebugInfo(
          "process()",
          ((HttpShardHandler.SimpleSolrResponse) sreq.responses.getFirst().getSolrResponse())
              .nl.get("response"));
    }
  }

  @Override
  public String getDescription() {
    return "A test search component to assert the forced distributed behaviour.";
  }
}
