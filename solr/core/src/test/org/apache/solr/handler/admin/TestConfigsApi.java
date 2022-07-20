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

package org.apache.solr.handler.admin;

import org.apache.solr.SolrTestCaseJ4;

public class TestConfigsApi extends SolrTestCaseJ4 {

    /*
  public void testCommands() throws Exception {

    try (ConfigSetsHandler handler =
        new ConfigSetsHandler(null) {

          @Override
          protected void checkErrors() {}

          @Override
          protected void sendToOverseer(
              SolrQueryResponse rsp, ConfigSetOperation operation, Map<String, Object> result) {
            result.put(QUEUE_OPERATION, operation.action.toLower());
            rsp.add(ZkNodeProps.class.getName(), new ZkNodeProps(result));
          }
        }) {
      ApiBag apiBag = new ApiBag(false);

      ClusterAPI o = new ClusterAPI(null, handler);
      apiBag.registerObject(o);

      apiBag.registerObject(o.configSetCommands);
      //      for (Api api : handler.getApis()) apiBag.register(api, emptyMap());
      compareOutput(
          apiBag, "/cluster/configs/sample", DELETE, null, "{name :sample, operation:delete}");
    }
    }
     */
}
